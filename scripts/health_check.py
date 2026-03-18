"""
Service health check script (Phase 7).

Usage:
  python scripts/health_check.py
"""

import os
import sys
import socket
import requests
import psycopg
from botocore.client import Config
import boto3

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


def check_port(host: str, port: int, timeout: float = 3.0) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def check_postgres():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    try:
        conn = psycopg.connect(
            host=host,
            port=port,
            dbname=os.getenv("POSTGRES_DB", "crypto_db"),
            user=os.getenv("POSTGRES_USER", "crypto_user"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
            connect_timeout=3,
        )
        conn.close()
        print(f"[OK] Postgres reachable at {host}:{port}")
    except Exception as exc:
        print(f"[FAIL] Postgres unreachable: {exc}")


def check_kafka():
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    all_ok = True
    for broker in brokers:
        host, port_str = broker.strip().split(":")
        port = int(port_str)
        if not check_port(host, port):
            print(f"[FAIL] Kafka unreachable at {broker}")
            all_ok = False
    if all_ok:
        print(f"[OK] Kafka reachable at {brokers}")


def check_minio():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access = os.getenv("MINIO_ROOT_USER", "minio_admin")
    secret = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=Config(s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )
        buckets = s3.list_buckets().get("Buckets", [])
        print(f"[OK] MinIO reachable ({len(buckets)} buckets)")
    except Exception as exc:
        print(f"[FAIL] MinIO unreachable: {exc}")


def check_airflow():
    host = os.getenv("AIRFLOW_HOST", "localhost")
    port = int(os.getenv("AIRFLOW_PORT", "8088"))
    if check_port(host, port):
        try:
            resp = requests.get(f"http://{host}:{port}/health", timeout=3)
            if resp.status_code == 200:
                print(f"[OK] Airflow UI healthy at {host}:{port}")
            else:
                print(f"[FAIL] Airflow UI responded {resp.status_code}")
        except Exception as exc:
            print(f"[FAIL] Airflow health endpoint error: {exc}")
    else:
        print(f"[FAIL] Airflow port {port} not reachable on {host}")


def main():
    check_postgres()
    check_kafka()
    check_minio()
    check_airflow()


if __name__ == "__main__":
    sys.exit(main())