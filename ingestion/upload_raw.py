"""Upload the local data/raw/ tree to S3, preserving the folder layout.

Local  data/raw/games/season=2024/games.json
  -> S3  s3://<bucket>/raw/games/season=2024/games.json

Skips any file already present in the bucket, so reruns are cheap and idempotent.
Run:   python ingestion/upload_raw.py
"""

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
S3_PREFIX = "raw"  # leaves room for a "processed/" prefix later


def existing_keys(s3, bucket):
    """Return the set of object keys already under raw/ in the bucket."""
    keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=S3_PREFIX + "/"):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


def upload_raw():
    bucket = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_REGION")
    if not bucket:
        raise RuntimeError("S3_BUCKET not set — add it to your .env file.")

    s3 = boto3.client("s3", region_name=region)
    already = existing_keys(s3, bucket)

    uploaded = skipped = 0
    for path in sorted(RAW_DIR.rglob("*.json")):
        rel = path.relative_to(RAW_DIR)          # e.g. games/season=2024/games.json
        key = f"{S3_PREFIX}/{rel.as_posix()}"    # e.g. raw/games/season=2024/games.json

        if key in already:
            skipped += 1
            continue

        s3.upload_file(str(path), bucket, key)
        uploaded += 1
        print(f"uploaded {key}")

    print(f"\nDone — {uploaded} uploaded, {skipped} already present.")


if __name__ == "__main__":
    upload_raw()
