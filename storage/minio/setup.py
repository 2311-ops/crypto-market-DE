"""
Create required MinIO buckets for the crypto pipeline.

Usage (host machine with Python + boto3 available):
  python storage/minio/setup.py

Buckets created (idempotent):
  - raw-data
  - processed-data
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
BUCKETS = ["raw-data", "processed-data"]


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"✓ Bucket exists: {bucket}")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"+ Created bucket: {bucket}")


def main() -> int:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        for b in BUCKETS:
            ensure_bucket(s3, b)
        return 0
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
