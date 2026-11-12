"""Fetch FBS team game stats for one season, one week at a time.

Stats come from /games/teams, which is keyed by week, so this script loops over
the weeks that actually have games (read from the already-saved games file).

Output: data/raw/game_stats/season=<year>/week=<n>/stats.json
Run:    python ingestion/ingest_stats.py --season 2024
Note:   run ingest_games.py for the season first — this reads that file.
"""

import argparse
import json
from pathlib import Path

from api_client import get

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw" / "game_stats"
GAMES_DIR = REPO_ROOT / "data" / "raw" / "games"


def weeks_for_season(season):
    """Read the saved games file to learn which weeks have games."""
    games_path = GAMES_DIR / f"season={season}" / "games.json"
    if not games_path.exists():
        raise FileNotFoundError(
            f"{games_path} not found — run ingest_games.py --season {season} first."
        )
    games = json.loads(games_path.read_text())
    return sorted(set(g["week"] for g in games))


def ingest_stats(season):
    weeks = weeks_for_season(season)
    print(f"Season {season}: {len(weeks)} weeks to fetch (weeks {weeks[0]}-{weeks[-1]})")

    for week in weeks:
        out_dir = RAW_DIR / f"season={season}" / f"week={week}"
        out_path = out_dir / "stats.json"

        # Per-week caching: skip any week we've already pulled.
        if out_path.exists():
            print(f"  week {week}: cached, skipping")
            continue

        stats = get(
            "/games/teams",
            # seasonType=regular must match ingest_games.py — without it the
            # endpoint pulls extra season-boundary games that aren't in dim_games.
            {"year": season, "week": week, "seasonType": "regular", "classification": "fbs"},
        )

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(stats, indent=2))
        print(f"  week {week}: saved {len(stats)} game records")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FBS team game stats for a season.")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g. 2024")
    args = parser.parse_args()
    ingest_stats(args.season)
