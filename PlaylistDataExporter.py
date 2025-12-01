import os
import time
import csv
from pathlib import Path
from typing import Dict, List, Any

from dotenv import load_dotenv
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

 
# Paths & basic setup
load_dotenv()

# Assume this file is in src/, so the project root is one level up
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT/ "FINALPROJECT" / "output"
LOGS_DIR = ROOT/ "FINALPROJECT" / "logs"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "PlaylistDataExportLog.log"

SLEEP_MS = int(os.getenv("SPOTIFY_SLEEP_MS", "200"))


def log(msg: str) -> None:
     #Print and append to a log file with a simple timestamp. #
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] {msg}"
    print(line)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


 
# Camelot mapping (Spotify key/mode -> Camelot code)
# key: 0–11 (C, C#/Db, D, ..., B), mode: 1=major, 0=minor
KEYMODE_TO_CAMELOT: Dict[tuple, str] = {
    # minor (mode = 0)
    (0, 0): "5A",   # C minor
    (1, 0): "12A",  # C#/Db minor
    (2, 0): "7A",   # D minor
    (3, 0): "2A",   # D#/Eb minor
    (4, 0): "9A",   # E minor
    (5, 0): "4A",   # F minor
    (6, 0): "11A",  # F#/Gb minor
    (7, 0): "6A",   # G minor
    (8, 0): "1A",   # G#/Ab minor
    (9, 0): "8A",   # A minor
    (10, 0): "3A",  # A#/Bb minor
    (11, 0): "10A", # B minor

    # major (mode = 1)
    (0, 1): "8B",   # C major
    (1, 1): "3B",   # C#/Db major
    (2, 1): "10B",  # D major
    (3, 1): "5B",   # D#/Eb major
    (4, 1): "12B",  # E major
    (5, 1): "7B",   # F major
    (6, 1): "2B",   # F#/Gb major
    (7, 1): "9B",   # G major
    (8, 1): "4B",   # G#/Ab major
    (9, 1): "11B",  # A major
    (10, 1): "6B",  # A#/Bb major
    (11, 1): "1B",  # B major
}

def keymode_to_camelot(key: Any, mode: Any) -> str:
     #Map Spotify key/mode to Camelot code, or '' if unknown. #
    if key is None or mode is None:
        return ""
    try:
        k = int(key)
        m = int(mode)
    except (TypeError, ValueError):
        return ""
    return KEYMODE_TO_CAMELOT.get((k, m), "")
 
# Spotify client
def get_spotify_client() -> Spotify:
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
    redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI", "http://localhost:8888/callback")

    if not client_id or not client_secret:
        raise SystemExit("SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET must be set in your .env")

    scope = "user-library-read"

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=scope,
    )
    sp = Spotify(auth_manager=auth_manager)
    return sp
 
# Fetch Liked Songs
def fetch_liked_tracks(sp: Spotify, sleep_ms: int = SLEEP_MS) -> List[Dict[str, Any]]:
    """
    Fetch the current user's saved tracks ("Liked Songs") with pagination.
    Returns the raw 'items' list from the /me/tracks endpoint.
    """

    items: List[Dict[str, Any]] = []

    limit = 50
    log("Fetching Liked Songs (saved tracks)...")
    res = sp.current_user_saved_tracks(limit=limit)
    items.extend(res.get("items", []))
    log(f"  Fetched first page: {len(items)} tracks")

    while res.get("next"):
        time.sleep(sleep_ms / 1000.0)
        res = sp.next(res)
        page_items = res.get("items", [])
        items.extend(page_items)
        log(f"  Fetched another page: +{len(page_items)} (total {len(items)})")

    log(f"Total liked tracks fetched: {len(items)}")
    return items

# Collect metadata from raw items
def collect_base_rows(items: List[Dict[str, Any]]) -> (List[Dict[str, Any]], List[str]):
    """
    Convert raw saved-track items into "base rows" and track_ids.
    Returns:
      - rows: list of dicts with basic track metadata
      - track_ids: list of unique track IDs for audio-feature lookup
    """
    rows: List[Dict[str, Any]] = []
    track_ids: List[str] = []
    seen_ids = set()

    for item in items:
        added_at = item.get("added_at", "")
        track = item.get("track") or {}
        if not track:
            continue

        track_id = track.get("id")
        if not track_id:
            # Local files or unsupported tracks may have no ID.
            continue

        uri = track.get("uri", "")
        track_name = track.get("name", "")

        album = track.get("album") or {}
        album_name = album.get("name", "")
        release_date = album.get("release_date", "")

        artists = track.get("artists") or []
        artist_names = [a.get("name", "") for a in artists]
        artists_str = "; ".join(artist_names)

        duration_ms = track.get("duration_ms", 0)
        popularity = track.get("popularity", 0)
        explicit = bool(track.get("explicit", False))

        row = {
            "track_id": track_id,
            "uri": uri,
            "track_name": track_name,
            "artists": artists_str,
            "album": album_name,
            "release_date": release_date,
            "duration_ms": duration_ms,
            "popularity": popularity,
            "explicit": explicit,
            "added_at": added_at,
        }
        rows.append(row)

        if track_id not in seen_ids:
            seen_ids.add(track_id)
            track_ids.append(track_id)

    log(f"Base rows collected: {len(rows)}")
    log(f"Unique track IDs for audio features: {len(track_ids)}")
    return rows, track_ids
 
# Audio features
def fetch_audio_features(sp: Spotify, track_ids: List[str], sleep_ms: int = SLEEP_MS) -> Dict[str, Dict[str, Any]]:
    #Fetch audio features for all track IDs, in batches.
    #Returns a dict: track_id -> features dict.
    features_by_id: Dict[str, Dict[str, Any]] = {}

    if not track_ids:
        return features_by_id

    batch_size = 100
    total = len(track_ids)
    log("Fetching audio features...")

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        chunk = track_ids[start:end]
        try:
            feats = sp.audio_features(chunk)
        except SpotifyException as e:
            log(f"  SpotifyException on batch {start}-{end}: {e}")
            feats = []

        if feats:
            for f in feats:
                if not f:
                    continue
                tid = f.get("id")
                if not tid:
                    continue
                features_by_id[tid] = f

        log(f"  Processed batch {start}-{end} (accumulated {len(features_by_id)})")
        time.sleep(sleep_ms / 1000.0)

    log(f"Total tracks with audio features: {len(features_by_id)}")
    return features_by_id

# Assemble final CSV rows
CSV_FIELDS = [
    "track_id",
    "uri",
    "track_name",
    "artists",
    "album",
    "release_date",
    "duration_ms",
    "popularity",
    "explicit",
    "added_at",
    # Audio feature raw values
    "tempo",
    "key",
    "mode",
    "camelot",
    "danceability",
    "energy",
    "valence",
    "acousticness",
    "instrumentalness",
    "liveness",
    "speechiness",
    "time_signature",
]

def assemble_rows(base_rows: List[Dict[str, Any]],
                  audio_features_by_id: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    #Merge base metadata with audio features into final CSV-ready dicts.
    final_rows: List[Dict[str, Any]] = []

    for row in base_rows:
        tid = row["track_id"]
        feats = audio_features_by_id.get(tid) or {}

        key = feats.get("key")
        mode = feats.get("mode")
        camelot = keymode_to_camelot(key, mode)

        combined = dict(row)  # copy base row
        combined.update({
            "tempo": feats.get("tempo"),
            "key": key,
            "mode": mode,
            "camelot": camelot,
            "danceability": feats.get("danceability"),
            "energy": feats.get("energy"),
            "valence": feats.get("valence"),
            "acousticness": feats.get("acousticness"),
            "instrumentalness": feats.get("instrumentalness"),
            "liveness": feats.get("liveness"),
            "speechiness": feats.get("speechiness"),
            "time_signature": feats.get("time_signature"),
        })

        final_rows.append(combined)

    log(f"Final rows assembled: {len(final_rows)}")
    return final_rows

# CSV writer
def write_liked_songs_csv(rows: List[Dict[str, Any]]) -> Path:
    #Write the final rows to output/Liked_Songs.csv.
    out_path = OUT_DIR / "Liked_Songs.csv"
    log(f"Writing CSV: {out_path}")

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            # ensure only known fields are written
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})

    log("CSV write complete.")
    return out_path

 
# Orchestration
def export_liked_songs(sp: Spotify) -> None:
    me = sp.me()
    display_name = me.get("display_name") or me.get("id")
    user_id = me.get("id")
    log(f"Authed as: {display_name} ({user_id})")

    items = fetch_liked_tracks(sp, sleep_ms=SLEEP_MS)
    if not items:
        log("No liked tracks found; nothing to export.")
        return

    base_rows, track_ids = collect_base_rows(items)
    audio_features_by_id = fetch_audio_features(sp, track_ids, sleep_ms=SLEEP_MS)
    final_rows = assemble_rows(base_rows, audio_features_by_id)
    out_path = write_liked_songs_csv(final_rows)
    log(f"Done. Exported {len(final_rows)} tracks to {out_path}")


def main() -> None:
    sp = get_spotify_client()
    export_liked_songs(sp)


if __name__ == "__main__":
    main()
