"""Upload local data layers to S3, preserving the folder layout.

  data/raw/...        -> s3://<bucket>/raw/...        (additive: raw is immutable)
  data/processed/...  -> s3://<bucket>/processed/...  (mirror: Spark regenerates it)

The processed layer is MIRRORED: Spark rewrites Parquet with new filenames every
run, so stale files in S3 are deleted to keep it a 1-to-1 copy of local. Without
this, COPY INTO would read old + new generations and multiply rows. The raw layer
is additive (immutable source files, stable names).

Run: python ingestion/upload_to_s3.py
"""

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

# (local subfolder under data/, S3 key prefix, mirror?)
LAYERS = [
    ("raw", "raw", False),
    ("processed", "processed", True),
]


def existing_keys(s3, bucket, prefix):
    """Return the set of object keys already under a prefix in the bucket."""
    keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


def local_keys_for(local_dir, prefix):
    """Map every local data file to the S3 key it should have."""
    keys = {}
    for path in sorted(local_dir.rglob("*")):
        # skip directories and Spark's hidden .crc checksum files
        if not path.is_file() or path.name.startswith("."):
            continue
        rel = path.relative_to(local_dir)
        keys[f"{prefix}/{rel.as_posix()}"] = path
    return keys


def upload_tree(s3, bucket, local_dir, prefix, mirror):
    """Upload a local folder to s3://<bucket>/<prefix>/, skipping existing keys.
    If mirror=True, also delete S3 objects that no longer exist locally."""
    if not local_dir.exists():
        print(f"{prefix}/: skipped — {local_dir} not found")
        return

    remote = existing_keys(s3, bucket, prefix)
    local = local_keys_for(local_dir, prefix)

    deleted = 0
    if mirror:
        for stale_key in remote - set(local):
            s3.delete_object(Bucket=bucket, Key=stale_key)
            deleted += 1

    uploaded = skipped = 0
    for key, path in local.items():
        if key in remote:
            skipped += 1
            continue
        s3.upload_file(str(path), bucket, key)
        uploaded += 1

    tail = f", {deleted} stale deleted" if mirror else ""
    print(f"{prefix}/: {uploaded} uploaded, {skipped} already present{tail}")


def main():
    bucket = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_REGION")
    if not bucket:
        raise RuntimeError("S3_BUCKET not set — add it to your .env file.")

    s3 = boto3.client("s3", region_name=region)
    for subdir, prefix, mirror in LAYERS:
        upload_tree(s3, bucket, DATA_DIR / subdir, prefix, mirror)


if __name__ == "__main__":
    main()
