"""Fetch FBS teams for one season and save the raw JSON.

Output: data/raw/teams/season=<year>/teams.json
Run:    python ingestion/ingest_teams.py --season 2024
"""

import argparse
import json
from pathlib import Path

from api_client import get

# Anchor output to the repo root so it works no matter where the script is run from.
REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw" / "teams"


def ingest_teams(season):
    out_dir = RAW_DIR / f"season={season}"
    out_path = out_dir / "teams.json"

    # Caching: if we already pulled this season, don't spend an API call on it.
    if out_path.exists():
        print(f"Cached — {out_path} already exists, skipping API call.")
        return

    teams = get("/teams/fbs", {"year": season})

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(teams, indent=2))
    print(f"Saved {len(teams)} FBS teams to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FBS teams for a season.")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g. 2024")
    args = parser.parse_args()
    ingest_teams(args.season)
