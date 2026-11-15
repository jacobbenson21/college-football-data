"""Load processed Parquet from S3 into Snowflake RAW tables.

Each RAW table lands the full Parquet record as a VARIANT plus lineage columns:
  - season       recovered from the partition folder (season=<year>/) in the S3 path
  - source_file  the S3 key the row came from
  - loaded_at    load timestamp
dbt staging models downstream extract and type the individual fields. Keeping RAW
as the faithful landing zone (semi-structured + lineage) is the standard ELT split:
RAW = land it, STAGING = type/clean it, ANALYTICS = model it.

Idempotent: truncates and fully reloads each run (data is small).
Assumes the storage integration from the one-time AWS/Snowflake setup already exists;
the file format and stage are (re)created here so the script is self-contained.

Run: python snowflake/load_raw.py
"""

import os
import warnings

warnings.filterwarnings("ignore")

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

STAGE = "CFB.RAW.processed_stage"
BUCKET_URL = "s3://cfb-data-pipeline-jacobbenson21/processed/"

# dataset -> expected row count (sanity check against the processed Parquet)
EXPECTED = {"teams": 534, "games": 3483, "game_stats": 6966}


def connect():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        login_timeout=15,
    )


def ensure_stage(cur):
    """Create the Parquet file format and external stage if they don't exist.
    (The storage integration itself requires the one-time manual AWS/IAM setup.)"""
    cur.execute("CREATE FILE FORMAT IF NOT EXISTS CFB.RAW.parquet_fmt TYPE = PARQUET")
    cur.execute(
        f"""
        CREATE STAGE IF NOT EXISTS {STAGE}
          STORAGE_INTEGRATION = cfb_s3_int
          URL = '{BUCKET_URL}'
          FILE_FORMAT = CFB.RAW.parquet_fmt
        """
    )


def load_dataset(cur, dataset):
    table = f"CFB.RAW.{dataset}"

    # 1. Landing table: the whole Parquet record as VARIANT + lineage columns.
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            data        VARIANT,
            season      INTEGER,
            source_file STRING,
            loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )

    # 2. Full reload for idempotency (data is small): clear the table first.
    cur.execute(f"TRUNCATE TABLE {table}")

    # 3. COPY: load each record, recovering `season` from the season=<year>/ folder in
    #    the file path. FORCE = TRUE reloads files even though COPY's load history
    #    remembers them (needed because we just truncated).
    cur.execute(
        f"""
        COPY INTO {table} (data, season, source_file)
        FROM (
            SELECT
                $1,
                REGEXP_SUBSTR(METADATA$FILENAME, 'season=([0-9]+)', 1, 1, 'e', 1)::INT,
                METADATA$FILENAME
            FROM @{STAGE}/{dataset}/
        )
        FILE_FORMAT = (FORMAT_NAME = CFB.RAW.parquet_fmt)
        PATTERN = '.*\\.parquet'
        FORCE = TRUE
        """
    )

    # 4. Validate the loaded row count against the processed Parquet.
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    actual = cur.fetchone()[0]
    expected = EXPECTED.get(dataset)
    flag = "OK" if actual == expected else "!! MISMATCH"
    print(f"{table}: {actual} rows (expected {expected}) [{flag}]")


def main():
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("USE SCHEMA CFB.RAW")
        ensure_stage(cur)
        for dataset in EXPECTED:
            load_dataset(cur, dataset)
    finally:
        conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
