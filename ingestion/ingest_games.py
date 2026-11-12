"""Fetch FBS regular-season games for one season and save the raw JSON.

Output: data/raw/games/season=<year>/games.json
Run:    python ingestion/ingest_games.py --season 2024
"""

import argparse
import json
from pathlib import Path

from api_client import get

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw" / "games"


def ingest_games(season):
    out_dir = RAW_DIR / f"season={season}"
    out_path = out_dir / "games.json"

    if out_path.exists():
        print(f"Cached — {out_path} already exists, skipping API call.")
        return

    games = get(
        "/games",
        {"year": season, "seasonType": "regular", "classification": "fbs"},
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(games, indent=2))
    print(f"Saved {len(games)} FBS games to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FBS regular-season games for a season.")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g. 2024")
    args = parser.parse_args()
    ingest_games(args.season)
