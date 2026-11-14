"""Upload local data layers to S3, preserving the folder layout.

  data/raw/...        -> s3://<bucket>/raw/...
  data/processed/...  -> s3://<bucket>/processed/...

Skips any object already present, so reruns are cheap and idempotent.
Run: python ingestion/upload_to_s3.py
"""

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

# local subfolder under data/  ->  S3 key prefix
LAYERS = {"raw": "raw", "processed": "processed"}


def existing_keys(s3, bucket, prefix):
    """Return the set of object keys already under a prefix in the bucket."""
    keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


def upload_tree(s3, bucket, local_dir, prefix):
    """Mirror one local folder into s3://<bucket>/<prefix>/, skipping existing keys."""
    if not local_dir.exists():
        print(f"{prefix}/: skipped — {local_dir} not found")
        return

    already = existing_keys(s3, bucket, prefix)
    uploaded = skipped = 0
    for path in sorted(local_dir.rglob("*")):
        # skip directories and Spark's hidden .crc checksum files
        if not path.is_file() or path.name.startswith("."):
            continue
        rel = path.relative_to(local_dir)
        key = f"{prefix}/{rel.as_posix()}"
        if key in already:
            skipped += 1
            continue
        s3.upload_file(str(path), bucket, key)
        uploaded += 1

    print(f"{prefix}/: {uploaded} uploaded, {skipped} already present")


def main():
    bucket = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_REGION")
    if not bucket:
        raise RuntimeError("S3_BUCKET not set — add it to your .env file.")

    s3 = boto3.client("s3", region_name=region)
    for subdir, prefix in LAYERS.items():
        upload_tree(s3, bucket, DATA_DIR / subdir, prefix)


if __name__ == "__main__":
    main()
