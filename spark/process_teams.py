"""Transform raw FBS teams JSON into a clean, flat Parquet dimension.

Reads  data/raw/teams/season=<year>/teams.json  (season comes from the folder name)
Writes data/processed/teams/season=<year>/*.parquet

Run: python spark/process_teams.py
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_TEAMS = REPO_ROOT / "data" / "raw" / "teams"
OUT_TEAMS = REPO_ROOT / "data" / "processed" / "teams"


def main():
    spark = (
        SparkSession.builder.appName("process_teams").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # multiLine=True: each file is a pretty-printed JSON array, not one object per line.
    # Pointing at the base folder lets Spark read each "season=<year>" folder name as a
    # `season` column automatically (Hive-style partition discovery).
    raw = spark.read.option("multiLine", "true").json(str(RAW_TEAMS))

    # Flatten + select: pull the useful location sub-fields up to top-level columns,
    # rename for clarity, and drop the array fields (logos, alternateNames) entirely.
    teams = raw.select(
        col("season"),
        col("id").alias("team_id"),
        col("school"),
        col("mascot"),
        col("abbreviation"),
        col("conference"),
        col("color"),
        col("alternateColor").alias("alternate_color"),
        col("location.name").alias("venue_name"),
        col("location.city").alias("venue_city"),
        col("location.state").alias("venue_state"),
        col("location.capacity").alias("venue_capacity"),
        col("location.dome").alias("venue_dome"),
        col("location.grass").alias("venue_grass"),
    )

    # overwrite = idempotent reruns; partition the output by season to mirror the raw layout.
    teams.write.mode("overwrite").partitionBy("season").parquet(str(OUT_TEAMS))

    print(f"Wrote {teams.count()} team rows to {OUT_TEAMS}")
    teams.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
