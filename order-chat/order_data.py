"""대시보드 2.4에서 재사용한 Parquet 조회·검증·최신 주문 메모리 캐시."""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError, SSLError

LOG = logging.getLogger("order-dashboard")
UTC = timezone.utc
# 원본 주문 시각과 화면 표시를 한국 시간으로 고정합니다.
# Kubernetes의 DB_TIMEZONE / DISPLAY_TIMEZONE 환경변수보다 이 설정을 우선합니다.
DB_TIMEZONE = "Asia/Seoul"
DISPLAY_TIMEZONE = "Asia/Seoul"
REFRESH_SECONDS = 5
COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "total_amount",
           "order_time", "etl_extracted_at"]
STATIC = Path(__file__).parent / "static"


@dataclass(frozen=True)
class Settings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "warehouse"
    prefix: str = "raw/orders_step1/"
    db_timezone: str = DB_TIMEZONE
    display_timezone: str = DISPLAY_TIMEZONE
    verify: bool | str = True

    @classmethod
    def from_env(cls):
        # HTTPS 자체는 유지. 실습 YAML에서만 인증서 검증을 끄도록 명시.
        verify = os.getenv("MINIO_VERIFY_SSL", "true").lower() != "false"
        ca = os.getenv("MINIO_CA_BUNDLE")
        if verify and ca:
            verify = ca
        prefix = os.getenv("MINIO_PREFIX", "raw/orders_step1/").strip("/")
        if not prefix:
            raise ValueError("MINIO_PREFIX를 지정하세요.")
        return cls(os.environ["MINIO_ENDPOINT"], os.environ["MINIO_ACCESS_KEY"],
                   os.environ["MINIO_SECRET_KEY"], os.getenv("MINIO_BUCKET", "warehouse"),
                   prefix + "/", DB_TIMEZONE, DISPLAY_TIMEZONE, verify)


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    amount: Decimal
    ordered_at: datetime
    extracted_at: datetime


def parse_time(value, naive_zone: ZoneInfo | timezone):
    if not isinstance(value, datetime):
        raise ValueError("Parquet 시간 칼럼의 형식이 datetime이 아닙니다.")
    if value.tzinfo is None:
        value = value.replace(tzinfo=naive_zone)
    return value.astimezone(UTC)


def read_parquet(payload: bytes, db_zone: ZoneInfo) -> tuple[Order, ...]:
    # ParquetFile로 단일 객체를 읽어 date=... 경로의 가상 칼럼을 추가하지 않음.
    parquet = pq.ParquetFile(BytesIO(payload))
    missing = set(COLUMNS) - set(parquet.schema_arrow.names)
    if missing:
        raise ValueError("필수 Parquet 칼럼 누락: " + ", ".join(sorted(missing)))
    orders = []
    for row in parquet.read(columns=COLUMNS).to_pylist():
        for key in ("order_id", "customer_id", "product_id"):
            if row[key] is None or not str(row[key]).strip():
                raise ValueError(f"{key} 값이 없습니다.")
        quantity = Decimal(str(row["quantity"]))
        amount = Decimal(str(row["total_amount"]))
        if not quantity.is_finite() or quantity <= 0 or quantity != quantity.to_integral_value():
            raise ValueError("quantity는 양의 정수여야 합니다.")
        if not amount.is_finite() or amount < 0:
            raise ValueError("total_amount는 0 이상의 금액이어야 합니다.")
        orders.append(Order(str(row["order_id"]), str(row["customer_id"]),
                            str(row["product_id"]), int(quantity), amount,
                            parse_time(row["order_time"], db_zone),
                            parse_time(row["etl_extracted_at"], UTC)))
    return tuple(orders)


def create_s3(settings: Settings):
    if settings.verify is False:
        LOG.warning("실습 설정: MinIO HTTPS 인증서 검증이 비활성화되어 있습니다.")
    return boto3.client("s3", endpoint_url=settings.endpoint,
                        aws_access_key_id=settings.access_key,
                        aws_secret_access_key=settings.secret_key,
                        region_name="us-east-1", verify=settings.verify,
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      connect_timeout=5, read_timeout=30,
                                      retries={"mode": "standard", "total_max_attempts": 2},
                                      request_checksum_calculation="when_required",
                                      response_checksum_validation="when_required"))


class ParquetStore:
    """파일 목록은 매번 확인하고, 새 파일/ETag가 바뀐 파일만 다운로드.

    모든 파일이 정상적으로 읽힌 경우에만 새 스냅샷을 공개한다.
    하나라도 실패하면 마지막 정상 스냅샷을 유지하고 다음 주기에 재시도.
    """
    def __init__(self, settings: Settings, s3=None):
        self.settings = settings
        self.s3 = s3 if s3 is not None else create_s3(settings)
        self.db_zone = ZoneInfo(settings.db_timezone)
        ZoneInfo(settings.display_timezone)  # 잘못된 시간대 설정은 즉시 확인
        self._files = {}
        self._orders = ()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._status = dict(last_success=None, last_change=None, error=None,
                            file_count=0, raw_rows=0, order_count=0, generation=0)

    def refresh(self):
        listing = {}
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.settings.bucket, Prefix=self.settings.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".parquet"):
                    listing[key] = (obj.get("ETag", ""), obj["Size"], obj["LastModified"])
        staged = {}
        changed = set(listing) != set(self._files)
        for key in sorted(listing):
            stamp = listing[key]
            cached = self._files.get(key)
            if cached is not None and cached[0] == stamp:
                staged[key] = cached
            else:
                response = self.s3.get_object(Bucket=self.settings.bucket, Key=key)
                try:
                    rows = read_parquet(response["Body"].read(), self.db_zone)
                finally:
                    response["Body"].close()
                staged[key] = (stamp, rows)
                changed = True
        latest = {}
        if changed:
            for key in sorted(staged):
                for index, row in enumerate(staged[key][1]):
                    # 같은 주문의 수정본/재시도 중복을 제거. 시각이 같으면 안정된 정렬 사용.
                    rank = (row.extracted_at, key, index)
                    previous = latest.get(row.order_id)
                    if previous is None or rank > previous[0]:
                        latest[row.order_id] = (rank, row)
            orders = tuple(sorted((value[1] for value in latest.values()),
                                  key=lambda row: (row.ordered_at, row.order_id)))
        now = datetime.now(UTC).isoformat()
        with self._lock:
            if changed:
                self._orders = orders
                self._files = staged
                self._status["last_change"] = now
                self._status["generation"] += 1
            self._status.update(last_success=now, error=None, file_count=len(staged),
                                raw_rows=sum(len(value[1]) for value in staged.values()),
                                order_count=len(self._orders))

    def poll(self):
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self.refresh()
            except Exception as exc:
                LOG.exception("MinIO Parquet 갱신 실패; 마지막 정상 데이터를 유지합니다.")
                if isinstance(exc, ClientError):
                    code = exc.response["Error"].get("Code", "S3Error")
                    message = f"MinIO 접근 오류 ({code}). 접속 주소·버킷·권한을 확인하세요."
                elif isinstance(exc, SSLError):
                    message = "MinIO 인증서 검증 오류. TLS/CA 설정을 확인하세요."
                elif isinstance(exc, ValueError):
                    message = "Parquet 스키마 또는 값 오류. Pod 로그를 확인하세요."
                else:
                    message = "MinIO 데이터 갱신 실패. 네트워크 설정과 Pod 로그를 확인하세요."
                with self._lock:
                    self._status["error"] = message
            self._stop.wait(max(0.1, REFRESH_SECONDS - (time.monotonic() - started)))

    def start(self):
        self._thread = threading.Thread(target=self.poll, daemon=True, name="parquet-poller")
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def snapshot(self):
        with self._lock:
            return self._orders, dict(self._status)

