from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import BytesIO
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from zoneinfo import ZoneInfo

from app import Settings, ParquetStore, analytics, create_app, read_parquet

UTC = timezone.utc
NOW = datetime(2026, 10, 9, 3, 0, 20, tzinfo=UTC)
SETTINGS = Settings("http://test-minio:9000", "test-access", "test-secret")


def row(order, customer="C1", product="P1", quantity=2, seconds=-15, amount="20.00", extracted=0):
    # 기존 ETL처럼 order_time은 naive 한국 시각, 추출 시각은 UTC aware.
    local = (NOW + timedelta(seconds=seconds)).astimezone(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
    return dict(order_id=order, customer_id=customer, product_id=product, quantity=quantity,
                total_amount=Decimal(amount), order_time=local,
                etl_extracted_at=NOW + timedelta(seconds=extracted))


def parquet(rows):
    buffer = BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    return buffer.getvalue()


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.reads = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        assert Bucket == "warehouse"
        # 각 객체를 다른 페이지에 배치하여 전체 페이지 순회 검증.
        for key, payload in sorted(self.objects.items()):
            if key.startswith(Prefix):
                yield {"Contents": [{"Key": key, "ETag": hashlib.md5(payload).hexdigest(),
                                      "Size": len(payload), "LastModified": NOW}]}

    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        return {"Body": BytesIO(self.objects[Key])}


def fixture_store():
    s3 = FakeS3()
    s3.objects["raw/orders_step1/date=2026-10-09/a.parquet"] = parquet([
        row("O1"), row("O2", product="P2", quantity=3, seconds=-10, amount="30.00"),
        row("O3", quantity=1, seconds=-10, amount="10.00"),
        row("O4", customer="C2", quantity=4, seconds=-5, amount="40.00")])
    s3.objects["raw/orders_step1/date=2026-10-09/b.parquet"] = parquet([
        row("O1", quantity=5, amount="50.00", extracted=1),
        row("O4", customer="C2", quantity=4, seconds=-5, amount="40.00")])
    s3.objects["raw/orders_step1/_checkpoint/state.json"] = b"{}"
    store = ParquetStore(SETTINGS, s3)
    store.refresh()
    return store, s3


def test_newest_version_and_duplicate_removed():
    store, s3 = fixture_store()
    rows, status = store.snapshot()
    assert len(rows) == 4
    assert sum(r.quantity for r in rows) == 13
    assert next(r for r in rows if r.order_id == "O1").quantity == 5
    assert status["raw_rows"] == 6 and status["file_count"] == 2
    assert all(key.endswith(".parquet") for key in s3.reads)


def test_incremental_cache_and_changed_object():
    store, s3 = fixture_store()
    store.refresh()
    assert len(s3.reads) == 2
    s3.objects["raw/orders_step1/date=2026-10-09/c.parquet"] = parquet([row("O5")])
    store.refresh()
    assert len(s3.reads) == 3 and len(store.snapshot()[0]) == 5
    s3.objects["raw/orders_step1/date=2026-10-09/c.parquet"] = parquet([row("O5", quantity=6, extracted=2)])
    store.refresh()
    assert len(s3.reads) == 4
    assert next(r for r in store.snapshot()[0] if r.order_id == "O5").quantity == 6


def test_removed_objects_rebuild_latest():
    store, s3 = fixture_store()
    del s3.objects["raw/orders_step1/date=2026-10-09/b.parquet"]
    store.refresh()
    assert next(r for r in store.snapshot()[0] if r.order_id == "O1").quantity == 2
    s3.objects = {}
    store.refresh()
    assert store.snapshot()[0] == ()


def test_failed_batch_does_not_publish_partial_data_and_can_retry():
    store, s3 = fixture_store()
    before = store.snapshot()
    s3.objects["raw/orders_step1/c.parquet"] = parquet([row("O5")])
    s3.objects["raw/orders_step1/d.parquet"] = b"bad parquet"
    with pytest.raises(Exception):
        store.refresh()
    assert store.snapshot() == before
    s3.objects["raw/orders_step1/d.parquet"] = parquet([row("O6")])
    store.refresh()
    assert len(store.snapshot()[0]) == 6


def test_poll_error_retains_last_good_data():
    store, _ = fixture_store()
    before = store.snapshot()[0]
    def fail():
        store._stop.set()
        raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")
    store.refresh = fail
    store.poll()
    rows, status = store.snapshot()
    assert rows == before and "AccessDenied" in status["error"]


def test_five_second_bins_unique_products_and_exact_amount():
    store, _ = fixture_store()
    result = analytics(store.snapshot()[0], NOW, live_minutes=1, interval=5)
    c1 = next(s for s in result["live"]["series"] if s["name"] == "C1")
    index = result["live"]["times"].index(int((NOW - timedelta(seconds=10)).timestamp()) * 1000)
    assert c1["items"][index] == 2
    assert c1["products"][index] == ["P1", "P2"]
    assert c1["quantity"][index] == 4 and c1["amount"][index] == 40
    assert result["summary"]["quantity"] == 13
    assert sum(p["quantity"] for p in result["products"]) == 13


def test_trend_delta_cumulative_and_customer_filter():
    store, _ = fixture_store()
    result = analytics(store.snapshot()[0], NOW, customer="C1", period="all", interval=5)
    s = result["trend"]["series"][0]
    assert s["quantity"] == [5, 4, 0, 0]
    assert s["delta"] == [None, -1, -4, 0]
    assert s["delta_pct"] == [None, -20.0, -100.0, None]
    assert s["cumulative"] == [5, 9, 9, 9]
    assert result["summary"]["orders"] == 3
    assert result["summary"]["quantity"] == 9


def test_timezones_hour_boundary_and_decimal_sums():
    source = [row("A", seconds=-21, amount="0.10"), row("B", seconds=-20, amount="0.20")]
    rows = read_parquet(parquet(source), ZoneInfo("Asia/Seoul"))
    assert rows[0].ordered_at == NOW - timedelta(seconds=21)
    result = analytics(rows, NOW, period="all", interval=5)
    assert len(result["hourly"]["times"]) == 2
    assert result["summary"]["amount"] == 0.3


def test_future_orders_are_excluded_without_invalid_grids():
    rows = read_parquet(parquet([row("future", seconds=3600)]), ZoneInfo("Asia/Seoul"))
    result = analytics(rows, NOW, period="all")
    assert result["summary"]["orders"] == 0
    assert len(result["trend"]["times"]) == 1


def test_initial_empty_bucket_is_success():
    store = ParquetStore(SETTINGS, FakeS3())
    store.refresh()
    assert store.snapshot()[1]["last_success"] is not None
    result = analytics((), NOW)
    assert result["summary"]["orders"] == 0 and result["live"]["series"] == []


def test_missing_schema_and_bad_quantity_rejected():
    with pytest.raises(ValueError, match="누락"):
        read_parquet(parquet([{"order_id": "X"}]), ZoneInfo("Asia/Seoul"))
    with pytest.raises(ValueError, match="quantity"):
        read_parquet(parquet([row("bad", quantity=-1)]), ZoneInfo("Asia/Seoul"))


def test_api_and_local_chart_assets():
    store, _ = fixture_store()
    with TestClient(create_app(store, start_poller=False)) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        html = client.get("/")
        assert "ORDER OBSERVATORY" in html.text
        assert html.headers["cache-control"] == "no-store"
        assert '<style>' in html.text and 'function ensureCharts' in html.text
        assert 'src="/static/dashboard.js"' not in html.text
        assert 'href="/static/style.css"' not in html.text
        assert '<!--DASHBOARD_' not in html.text
        assert client.get("/static/echarts.min.js").status_code == 200
        assert "hourlyOption" in client.get("/static/dashboard.js").text
        assert "live-items" not in client.get("/").text
        assert "현재부터 직전 5분" in client.get("/").text
        data = client.get("/api/dashboard?period=all").json()
        assert data["app_version"] == "2.1"
        assert data["status"]["order_count"] == 4
        assert data["timezone"] == "Asia/Seoul"
        assert client.get("/api/dashboard?period=invalid").status_code == 422
        assert client.get("/api/dashboard?live_minutes=61").status_code == 422


def test_live_window_is_exact_five_minutes_and_right_endpoint_is_now():
    now = NOW + timedelta(microseconds=123000)
    source = [row("old", seconds=-301, quantity=20),
              row("edge", seconds=-300, quantity=30),
              row("history", seconds=-290, quantity=3, amount="30.00"),
              row("future", seconds=1, quantity=90)]
    current = row("current", quantity=4, amount="40.00")
    current["order_time"] = (NOW + timedelta(microseconds=50000)).astimezone(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
    source.append(current)
    rows = read_parquet(parquet(source), ZoneInfo("Asia/Seoul"))
    result = analytics(rows, now)
    live = result["live"]
    assert live["window_end"] - live["window_start"] == 300000
    assert live["times"][-1] == live["window_end"] == int(now.timestamp()*1000)
    assert len(live["times"]) in (60, 61)
    assert all(a < b for a, b in zip(live["times"], live["times"][1:]))
    series = live["series"][0]
    assert sum(series["quantity"]) == 7  # 오래된 주문·왼쪽 경계·미래 주문 제외
    assert series["quantity"][-1] == 4 and series["amount"][-1] == 40


def test_order_exactly_at_now_belongs_to_rightmost_interval():
    rows = read_parquet(parquet([row("now", seconds=0, quantity=7)]), ZoneInfo("Asia/Seoul"))
    result = analytics(rows, NOW)
    assert result["live"]["series"][0]["quantity"][-1] == 7
    assert result["live"]["times"][-1] == int(NOW.timestamp()*1000)


def test_hourly_fills_empty_hours_without_interpolating_orders():
    rows = read_parquet(parquet([row("early", seconds=-7200, quantity=2),
                                row("late", seconds=-1, quantity=5)]), ZoneInfo("Asia/Seoul"))
    result = analytics(rows, NOW, period="all")
    assert result["hourly"]["series"][0]["quantity"] == [2, 0, 5]
    assert result["hourly"]["sparse"] is False


def test_certificate_settings(monkeypatch):
    for key, value in {"MINIO_ENDPOINT":"https://minio:9000", "MINIO_ACCESS_KEY":"a", "MINIO_SECRET_KEY":"b"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("MINIO_VERIFY_SSL", "false")
    assert Settings.from_env().verify is False
    monkeypatch.setenv("MINIO_VERIFY_SSL", "true")
    monkeypatch.setenv("MINIO_CA_BUNDLE", "/etc/minio-ca/ca.crt")
    assert Settings.from_env().verify == "/etc/minio-ca/ca.crt"
