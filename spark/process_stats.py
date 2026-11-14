"""Transform raw team game stats JSON into a clean, wide Parquet fact table.

Reads  data/raw/game_stats/season=<year>/week=<n>/stats.json
Writes data/processed/game_stats/season=<year>/*.parquet

Three steps:
  1. read + explode the teams array -> one row per game-team (the fact grain).
  2. pivot the long {category, stat} list -> one wide column per category.
  3. parse/cast values (ints, floats, "5-9" splits, "29:21" -> seconds).

Run: python spark/process_stats.py
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_STATS = REPO_ROOT / "data" / "raw" / "game_stats"
OUT_STATS = REPO_ROOT / "data" / "processed" / "game_stats"

# --- Stage 3 parsing maps: how each pivoted string column becomes clean column(s) ---

# Whole-number stats: cast the string to int, rename to snake_case.
INT_COLS = {
    "rushingYards": "rushing_yards", "rushingAttempts": "rushing_attempts",
    "rushingTDs": "rushing_tds", "netPassingYards": "net_passing_yards",
    "passingTDs": "passing_tds", "passesIntercepted": "passes_intercepted",
    "passesDeflected": "passes_deflected", "totalYards": "total_yards",
    "firstDowns": "first_downs", "interceptions": "interceptions",
    "interceptionYards": "interception_yards", "interceptionTDs": "interception_tds",
    "fumblesRecovered": "fumbles_recovered", "fumblesLost": "fumbles_lost",
    "totalFumbles": "total_fumbles", "turnovers": "turnovers", "tackles": "tackles",
    "qbHurries": "qb_hurries", "defensiveTDs": "defensive_tds",
    "kickReturns": "kick_returns", "kickReturnYards": "kick_return_yards",
    "kickReturnTDs": "kick_return_tds", "puntReturns": "punt_returns",
    "puntReturnYards": "punt_return_yards", "puntReturnTDs": "punt_return_tds",
    "kickingPoints": "kicking_points",
}

# Decimal stats (can be fractional, e.g. half-sacks, per-play averages): cast to double.
FLOAT_COLS = {
    "sacks": "sacks", "tacklesForLoss": "tackles_for_loss",
    "yardsPerPass": "yards_per_pass", "yardsPerRushAttempt": "yards_per_rush_attempt",
}

# "X-Y" compound stats: split on "-" into two int columns (made/attempts style).
SPLIT_COLS = {
    "completionAttempts": ("completions", "pass_attempts"),
    "thirdDownEff": ("third_down_conversions", "third_down_attempts"),
    "fourthDownEff": ("fourth_down_conversions", "fourth_down_attempts"),
    "totalPenaltiesYards": ("penalties", "penalty_yards"),
}


def main():
    spark = (
        SparkSession.builder.appName("process_stats").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # season and week come from the folder names (the stats JSON has neither inside
    # it), so base-folder partition discovery gives us both columns, no clash.
    raw = spark.read.option("multiLine", "true").json(str(RAW_STATS))

    # explode: turn each element of the `teams` array into its own row, so one game
    # row (2 teams) becomes two rows. This is where the game-team grain is born.
    exploded = raw.select(
        "season",
        "week",
        F.col("id").alias("game_id"),
        F.explode("teams").alias("t"),
    ).select(
        "season",
        "week",
        "game_id",
        F.col("t.teamId").alias("team_id"),
        F.col("t.team").alias("team"),
        F.col("t.homeAway").alias("home_away"),
        F.col("t.points").alias("points"),
        F.col("t.stats").alias("stats"),  # still a long array; pivoted in stage 2
    )

    # STAGE 2: pivot the long stats list into wide columns.
    # First explode the stats array -> one row per (game-team, category).
    id_cols = ["season", "week", "game_id", "team_id", "team", "home_away", "points"]
    long = exploded.select(
        *id_cols,
        F.explode("stats").alias("s"),
    ).select(
        *id_cols,
        F.col("s.category").alias("category"),
        F.col("s.stat").alias("stat"),
    )

    # Then pivot: one column per distinct category, value = the stat string.
    wide = long.groupBy(*id_cols).pivot("category").agg(F.first("stat"))

    # STAGE 3: parse/cast the raw string columns into clean typed columns.
    exprs = [F.col(c) for c in id_cols]  # carry the id columns through unchanged

    for src, tgt in INT_COLS.items():
        exprs.append(F.col(src).cast("int").alias(tgt))

    for src, tgt in FLOAT_COLS.items():
        exprs.append(F.col(src).cast("double").alias(tgt))

    for src, (made, att) in SPLIT_COLS.items():
        parts = F.split(F.col(src), "-")  # "5-9" -> ["5", "9"]
        exprs.append(parts.getItem(0).cast("int").alias(made))
        exprs.append(parts.getItem(1).cast("int").alias(att))

    # possessionTime "29:21" -> total seconds (29*60 + 21).
    mmss = F.split(F.col("possessionTime"), ":")
    exprs.append(
        (mmss.getItem(0).cast("int") * 60 + mmss.getItem(1).cast("int")).alias(
            "possession_seconds"
        )
    )

    fact = wide.select(*exprs)

    fact.write.mode("overwrite").partitionBy("season").parquet(str(OUT_STATS))

    print(f"Wrote {fact.count()} game-team rows to {OUT_STATS}")
    print("\nParsed-value check (compare to the raw strings from stage 2):")
    fact.select(
        "game_id", "team", "rushing_yards", "total_yards",
        "third_down_conversions", "third_down_attempts",
        "completions", "pass_attempts", "possession_seconds",
    ).show(6, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
