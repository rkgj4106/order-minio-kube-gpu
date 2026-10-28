"""Student Lab Step 1: Check MySQL order changes every 10 seconds and save them to MinIO as Parquet.

Required packages: python -m pip install 'PyMySQL[rsa]' boto3 pyarrow
Runtime environment: Kubernetes Pod (connect using MySQL/MinIO Service DNS).
Run command: python -u etl_orders_minio.py

Required environment variables:
  MYSQL_PASSWORD, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
Optional environment variables:
  MYSQL_HOST=mysql, MYSQL_PORT=3306, MYSQL_USER=root,
  MYSQL_DATABASE=orderdb, MYSQL_TABLE=orders,
  MINIO_BUCKET=warehouse, MINIO_PREFIX=raw/orders_step1/,
  CREATE_BUCKET=true

Prerequisite: An orders table with a unique order_id and the order columns
defined in SCHEMA below. The simulation_phase column may be absent or NULL.
All other order columns are required.
The first run also saves existing orders. Subsequent runs save the current
rows of new or modified orders.
Multiple orders are grouped into a single file. No file is created if there
are no changes.
Deletion events and intermediate changes that occur and disappear between
polls are not captured.
For this lab, the entire orders table is read on every poll. Large-scale
production systems require an approach such as CDC.
Use Pod replicas=1. Processing history is persisted in a MinIO checkpoint.
On failure, processing history is not advanced, and the operation is retried.
If the Pod restarts between uploading data and writing the checkpoint, the
same order version may remain in multiple files (at-least-once).
For subsequent analysis, use the latest row by etl_extracted_at for each order_id.
Deleting the checkpoint file causes all orders to be collected again.
order_time preserves the original database time; etl_extracted_at is stored in UTC.
"""

import hashlib
import json
import logging
import os
import re
import signal
import threading
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pymysql
from botocore.config import Config
from botocore.exceptions import ClientError

LOG = logging.getLogger("orders-etl")
POLL_SECONDS = 10
SCHEMA = pa.schema([
    ("order_id", pa.string()), ("customer_id", pa.string()),
    ("product_id", pa.string()), ("quantity", pa.int64()),
    ("unit_price", pa.decimal128(20, 2)),
    ("total_amount", pa.decimal128(20, 2)),
    ("simulation_phase", pa.string()), ("order_time", pa.timestamp("us")),
    ("etl_change_type", pa.string()),
    ("etl_extracted_at", pa.timestamp("us", tz="UTC")),
])


def json_value(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported database value type: {type(value).__name__}")


def fingerprint(row):
    # Identical row contents produce the same hash.
    # New orders are detected even when their IDs or timestamps are lower.
    canonical = json.dumps(row, default=json_value, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def make_parquet(changes, extracted_at):
    records = []
    for row, change_type in changes:
        record = dict(row)
        for field in ("order_id", "customer_id", "product_id"):
            if record.get(field) is None or not str(record[field]).strip():
                raise ValueError(f"Order is missing {field}.")
            record[field] = str(record[field])
        quantity = Decimal(str(record["quantity"]))
        if not quantity.is_finite() or quantity <= 0 or quantity != quantity.to_integral_value():
            raise ValueError("quantity must be a positive integer.")
        record["quantity"] = int(quantity)
        for field in ("unit_price", "total_amount"):
            money = Decimal(str(record[field]))
            if not money.is_finite() or money < 0:
                raise ValueError(f"{field} must be a non-negative amount.")
            record[field] = money
        if not isinstance(record.get("order_time"), datetime):
            raise ValueError("order_time must be a MySQL DATETIME/TIMESTAMP value.")
        phase = record.get("simulation_phase")
        record["simulation_phase"] = None if phase is None else str(phase)
        record.update(etl_change_type=change_type, etl_extracted_at=extracted_at)
        records.append(record)
    # Preserve Decimal amounts without converting them to float.
    # Use a fixed schema for every batch.
    buffer = BytesIO()
    pq.write_table(pa.Table.from_pylist(records, schema=SCHEMA), buffer, compression="snappy")
    return buffer.getvalue()


class OrderETL:
    def __init__(self):
        self.table = os.getenv("MYSQL_TABLE", "orders")
        if not re.fullmatch(r"[A-Za-z0-9_]+", self.table):
            raise ValueError("MYSQL_TABLE may contain only ASCII letters, digits, and underscores.")
        self.db = dict(
            host=os.getenv("MYSQL_HOST", "mysql"), port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER", "root"), password=os.environ["MYSQL_PASSWORD"],
            database=os.getenv("MYSQL_DATABASE", "orderdb"), charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor, autocommit=True,
            connect_timeout=5, read_timeout=30, write_timeout=30,
        )
        self.bucket = os.getenv("MINIO_BUCKET", "warehouse")
        self.prefix = os.getenv("MINIO_PREFIX", "raw/orders_step1/").strip("/") + "/"
        if self.prefix == "/":
            raise ValueError("Specify MINIO_PREFIX.")
        self.checkpoint_key = self.prefix + "_checkpoint/state.json"
        self.source = f"{self.db['host']}:{self.db['port']}/{self.db['database']}/{self.table}"
        self.s3 = boto3.client(
            "s3", endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"], region_name="us-east-1",
            verify=False,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                          connect_timeout=5, read_timeout=30,
                          retries={"mode": "standard", "total_max_attempts": 2},
                          request_checksum_calculation="when_required",
                          response_checksum_validation="when_required"),
        )
        self.previous = None  # None means the checkpoint has not been loaded yet.
        self.pending = None   # Retry the same file and contents on the next poll if uploading fails.

    def initialize(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            code = str(exc.response["Error"]["Code"])
            if code not in {"404", "NoSuchBucket", "NotFound"} or os.getenv("CREATE_BUCKET", "true").lower() != "true":
                raise
            self.s3.create_bucket(Bucket=self.bucket)
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.checkpoint_key)
            try:
                state = json.loads(response["Body"].read())
            finally:
                response["Body"].close()
            if state["source"] != self.source:
                raise ValueError("The existing checkpoint refers to a different database source. Use a new MINIO_PREFIX.")
            hashes = state["hashes"]
            if not isinstance(hashes, dict):
                raise ValueError("Invalid checkpoint format.")
            self.previous = hashes
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
                raise  # Do not treat access errors as missing processing history.
            self.previous = {}

    def read_orders(self):
        connection = pymysql.connect(**self.db)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM `{self.table}` ORDER BY order_id")
                return cursor.fetchall()
        finally:
            connection.close()

    def prepare_batch(self, rows):
        changes, seen = [], set()
        new_hashes = dict(self.previous)
        for row in rows:
            if row.get("order_id") is None:
                raise ValueError("Order is missing order_id.")
            order_id = str(row["order_id"])
            if order_id in seen:
                raise ValueError("order_id must be unique for each order.")
            seen.add(order_id)
            digest = fingerprint(row)
            if self.previous.get(order_id) != digest:
                operation = "insert" if order_id not in self.previous else "update"
                changes.append((row, operation))
                new_hashes[order_id] = digest
        if not changes:
            return None
        now = datetime.now(timezone.utc)
        key = f"{self.prefix}date={now:%Y-%m-%d}/orders_{now:%H%M%S}_{uuid.uuid4().hex}.parquet"
        return dict(key=key, payload=make_parquet(changes, now), hashes=new_hashes,
                    inserts=sum(operation == "insert" for _, operation in changes),
                    updates=sum(operation == "update" for _, operation in changes))

    def run_once(self):
        if self.previous is None:
            self.initialize()
        if self.pending is None:
            self.pending = self.prepare_batch(self.read_orders())
        if self.pending is None:
            LOG.info("No order changes")
            return 0
        batch = self.pending
        # Always upload data before saving processing history.
        # Keep pending on failure.
        self.s3.put_object(Bucket=self.bucket, Key=batch["key"], Body=batch["payload"],
                           ContentType="application/vnd.apache.parquet")
        state = json.dumps({"source": self.source, "hashes": batch["hashes"]},
                           ensure_ascii=False).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=self.checkpoint_key, Body=state,
                           ContentType="application/json")
        self.previous = batch["hashes"]
        self.pending = None
        count = batch["inserts"] + batch["updates"]
        LOG.info("Saved successfully: inserts=%s updates=%s → s3://%s/%s",
                 batch["inserts"], batch["updates"], self.bucket, batch["key"])
        return count


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    etl = OrderETL()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        try:
            etl.run_once()
        except Exception:
            LOG.exception("ETL failed: processing history is preserved. Retrying in 10 seconds.")
        # Wait 10 seconds after success or failure.
        # The actual interval between starts also includes processing time.
        stop.wait(POLL_SECONDS)


if __name__ == "__main__":
    main()