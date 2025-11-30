"""Demo DAG that creates a tiny Iceberg table in the bronze layer and exposes it read-only via ClickHouse."""
import os
import uuid
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

MINIO_BUCKET = "practice-bucket"
MINIO_ENDPOINT = os.getenv("PYICEBERG_CATALOG__REST__S3__ENDPOINT", "http://minio:9000").rstrip("/")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

CATALOG_NAME = "rest"
ICEBERG_NAMESPACE = "bronze"
ICEBERG_TABLE = "rides_iceberg"
ICEBERG_IDENTIFIER = (ICEBERG_NAMESPACE, ICEBERG_TABLE)
ICEBERG_PATH_PREFIX = f"{ICEBERG_NAMESPACE}/{ICEBERG_TABLE}/"


def _imports():
    """Import heavy deps lazily so the DAG still parses even if the image wasn't rebuilt yet."""
    try:
        import boto3
        import pyarrow as pa
        from pyiceberg.catalog import load_catalog
        from pyiceberg.schema import Schema
        from pyiceberg.table import Table, WriteTask
        from pyiceberg.types import LongType, NestedField, StringType, TimestampType
    except ModuleNotFoundError as exc:
        raise ImportError(
            "Missing pyiceberg/pyarrow/boto3. Rebuild the Airflow image: "
            "`docker compose up -d --build airflow-webserver airflow-scheduler airflow-init`."
        ) from exc
    return {
        "boto3": boto3,
        "pa": pa,
        "load_catalog": load_catalog,
        "Schema": Schema,
        "Table": Table,
        "WriteTask": WriteTask,
        "LongType": LongType,
        "NestedField": NestedField,
        "StringType": StringType,
        "TimestampType": TimestampType,
    }


def ensure_minio_bucket() -> None:
    """Create the MinIO bucket if it does not yet exist."""
    libs = _imports()
    boto3 = libs["boto3"]

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
        return
    except Exception:
        # Fall back to creating the bucket; MinIO accepts the simple call.
        s3_client.create_bucket(Bucket=MINIO_BUCKET)


def _reset_prefix_if_needed() -> None:
    """Clear the Iceberg table prefix in MinIO so we can recreate with a clean format-version."""
    libs = _imports()
    boto3 = libs["boto3"]
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=ICEBERG_PATH_PREFIX):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
            if len(to_delete) == 1000:
                s3.delete_objects(Bucket=MINIO_BUCKET, Delete={"Objects": to_delete})
                to_delete = []
    if to_delete:
        s3.delete_objects(Bucket=MINIO_BUCKET, Delete={"Objects": to_delete})


def _build_schema():
    libs = _imports()
    return libs["Schema"](
        libs["NestedField"](1, "trip_id", libs["LongType"](), required=True),
        libs["NestedField"](2, "started_at", libs["TimestampType"](), required=True),
        libs["NestedField"](3, "start_station", libs["StringType"](), required=True),
        libs["NestedField"](4, "rider_type", libs["StringType"](), required=True),
    )


def upsert_iceberg_table(**context) -> None:
    """Create the Iceberg table (if missing) and append a small batch of demo rows."""
    libs = _imports()
    pa = libs["pa"]

    catalog = libs["load_catalog"](CATALOG_NAME)
    schema = _build_schema()

    try:
        table = catalog.load_table(ICEBERG_IDENTIFIER)
        # Recreate as format-version 2 to satisfy ClickHouse Iceberg reader.
        _reset_prefix_if_needed()
        catalog.drop_table(ICEBERG_IDENTIFIER)
        _reset_prefix_if_needed()
        table = catalog.create_table(
            ICEBERG_IDENTIFIER,
            schema,
            properties={"format-version": "2"},
        )
    except Exception:
        _reset_prefix_if_needed()
        table = catalog.create_table(
            ICEBERG_IDENTIFIER,
            schema,
            properties={"format-version": "2"},
        )

    # Build data that matches the Iceberg schema exactly (timestamp in microseconds, required fields).
    arrow_schema = pa.schema(
        [
            pa.field("trip_id", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "1"}),
            pa.field("started_at", pa.timestamp("us"), nullable=False, metadata={"PARQUET:field_id": "2"}),
            pa.field("start_station", pa.string(), nullable=False, metadata={"PARQUET:field_id": "3"}),
            pa.field("rider_type", pa.string(), nullable=False, metadata={"PARQUET:field_id": "4"}),
        ]
    )
    sample_data = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(
                [
                    datetime(2025, 1, 1, 8, 0, 0),
                    datetime(2025, 1, 1, 9, 30, 0),
                    datetime(2025, 1, 1, 18, 15, 0),
                ],
                type=pa.timestamp("us"),
            ),
            pa.array(["Central Park", "Brooklyn Bridge", "Times Square"], type=pa.string()),
            pa.array(["member", "casual", "member"], type=pa.string()),
        ],
        schema=arrow_schema,
    )

    # Simplest path on PyIceberg 0.6.x: append the Arrow table directly.
    table.append(sample_data)

    context["ti"].xcom_push(key="iceberg_location", value=table.metadata.location)


def expose_in_clickhouse(**context) -> None:
    """Register the Iceberg table as read-only in ClickHouse using the Iceberg engine."""
    location = context["ti"].xcom_pull(key="iceberg_location", task_ids="write_bronze_iceberg")
    if not location:
        raise ValueError("No Iceberg table location found in XCom; cannot publish to ClickHouse.")

    # Convert s3:// bucket path to an HTTP URL MinIO understands.
    http_path = location.replace("s3://", f"{MINIO_ENDPOINT}/")
    if not http_path.endswith("/"):
        http_path += "/"

    # ClickHouse 25.10 Iceberg engine expects manifest entries with a `sequence_number`
    # field name, while PyIceberg writes `data_sequence_number`. To avoid the spec
    # mismatch, register a read-only view over the Iceberg data files via the S3
    # table function.
    ddl = f"""
    CREATE DATABASE IF NOT EXISTS {ICEBERG_NAMESPACE};
    CREATE OR REPLACE VIEW {ICEBERG_NAMESPACE}.{ICEBERG_TABLE}_s3 AS
    SELECT *
    FROM s3('{http_path}data/*', '{AWS_ACCESS_KEY}', '{AWS_SECRET_KEY}', 'Parquet');
    """

    for stmt in ddl.strip().split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        response = requests.post(
            "http://clickhouse-server:8123/",
            params={"query": stmt},
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        )
        if not response.ok:
            raise RuntimeError(f"ClickHouse DDL failed ({response.status_code}): {response.text}")


with DAG(
    dag_id="iceberg_bronze_demo",
    description="Create a demo Iceberg table in MinIO and expose it to ClickHouse (read-only).",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    create_bucket = PythonOperator(
        task_id="ensure_minio_bucket",
        python_callable=ensure_minio_bucket,
    )

    write_table_task = PythonOperator(
        task_id="write_bronze_iceberg",
        python_callable=upsert_iceberg_table,
        provide_context=True,
    )

    publish_clickhouse = PythonOperator(
        task_id="publish_clickhouse_iceberg_view",
        python_callable=expose_in_clickhouse,
        provide_context=True,
    )

    create_bucket >> write_table_task >> publish_clickhouse
