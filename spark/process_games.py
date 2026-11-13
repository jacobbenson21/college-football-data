"""Transform raw FBS games JSON into a clean, flat Parquet dimension (one row per game).

Reads  data/raw/games/season=<year>/games.json
Writes data/processed/games/season=<year>/*.parquet

Differences from process_teams.py:
  - games JSON already contains `season` and `week`, so we read each season file
    and union them (folder-name partition discovery would collide with the
    in-data `season` column).
  - filter to completed games only (drops future/unplayed and cancellations).
  - flatten the lineScores arrays into q1..q4 + ot columns.
  - drop the line-score arrays, Elo besides pregame, win-prob besides postgame, etc.

Run: python spark/process_games.py
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_GAMES = REPO_ROOT / "data" / "raw" / "games"
OUT_GAMES = REPO_ROOT / "data" / "processed" / "games"


def side_columns(prefix):
    """Build the home/away columns for one side ('home' or 'away')."""
    line = F.col(f"{prefix}LineScores")
    q1, q2, q3, q4 = line[0], line[1], line[2], line[3]
    points = F.col(f"{prefix}Points")
    return [
        F.col(f"{prefix}Id").alias(f"{prefix}_team_id"),
        F.col(f"{prefix}Team").alias(f"{prefix}_team"),
        F.col(f"{prefix}Conference").alias(f"{prefix}_conference"),
        F.col(f"{prefix}Classification").alias(f"{prefix}_classification"),
        points.alias(f"{prefix}_points"),
        q1.alias(f"{prefix}_q1"),
        q2.alias(f"{prefix}_q2"),
        q3.alias(f"{prefix}_q3"),
        q4.alias(f"{prefix}_q4"),
        # Final score already includes OT, so OT = final minus the four quarters.
        (points - (q1 + q2 + q3 + q4)).alias(f"{prefix}_ot"),
        F.col(f"{prefix}PregameElo").alias(f"{prefix}_pregame_elo"),
        F.col(f"{prefix}PostgameWinProbability").alias(f"{prefix}_postgame_win_prob"),
    ]


def main():
    spark = (
        SparkSession.builder.appName("process_games").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read each season's file separately and stack them. Reading one concrete file at
    # a time means Spark does not treat season=<year> as a partition column, so the
    # in-data `season` field is used without a name clash.
    frames = []
    for season_dir in sorted(RAW_GAMES.glob("season=*")):
        frames.append(
            spark.read.option("multiLine", "true").json(str(season_dir / "games.json"))
        )
    raw = frames[0]
    for f in frames[1:]:
        raw = raw.unionByName(f, allowMissingColumns=True)

    games = raw.filter(F.col("completed") == True).select(  # noqa: E712
        F.col("id").alias("game_id"),
        F.col("season"),
        F.col("week"),
        F.col("startDate").alias("start_date"),
        F.col("neutralSite").alias("neutral_site"),
        F.col("conferenceGame").alias("conference_game"),
        F.col("venue"),
        F.col("attendance"),
        F.col("excitementIndex").alias("excitement_index"),
        *side_columns("home"),
        *side_columns("away"),
    )

    games.write.mode("overwrite").partitionBy("season").parquet(str(OUT_GAMES))

    print(f"Wrote {games.count()} game rows to {OUT_GAMES}")
    games.select(
        "game_id", "week", "home_team", "home_points",
        "home_q1", "home_q2", "home_q3", "home_q4", "home_ot",
        "away_team", "away_points",
    ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
