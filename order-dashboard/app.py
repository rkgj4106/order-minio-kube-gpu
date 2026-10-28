"""MinIO Parquet 주문 분석: Python 3.12, 단일 Uvicorn worker, 5초 갱신."""
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
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

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


def group_metrics(rows, customers, slots, seconds, right_closed=False):
    groups = {}
    for row in rows:
        timestamp = row.ordered_at.timestamp()
        slot = ((math.ceil(timestamp / seconds) - 1) * seconds if right_closed
                else int(timestamp) // seconds * seconds)
        key = (row.customer_id, slot)
        cell = groups.setdefault(key, {"quantity": 0, "amount": Decimal("0"),
                                       "orders": 0, "products": set()})
        cell["quantity"] += row.quantity
        cell["amount"] += row.amount
        cell["orders"] += 1
        cell["products"].add(row.product_id)
    series = []
    for customer in customers:
        item = dict(name=customer, quantity=[], amount=[], orders=[], items=[], products=[])
        for slot in slots:
            cell = groups.get((customer, slot))
            item["quantity"].append(cell["quantity"] if cell else 0)
            item["amount"].append(float(cell["amount"]) if cell else 0)
            item["orders"].append(cell["orders"] if cell else 0)
            products = sorted(cell["products"]) if cell else []
            item["items"].append(len(products))
            item["products"].append(products)
        series.append(item)
    return {"times": [slot * 1000 for slot in slots], "series": series, "seconds": seconds}


def analytics(orders: tuple[Order, ...], now: datetime, customer: str = "*",
              live_minutes: int = 5, period: str = "24h", interval: int = 60,
              trend_minutes: int = 0):
    customers = sorted({row.customer_id for row in orders})
    selected = customers if customer == "*" else ([customer] if customer in customers else [])
    selected_set = set(selected)
    filtered = [row for row in orders if row.customer_id in selected_set]
    end = int(now.timestamp())
    # 직전 5분의 주문을 5초 구간으로 집계. 각 점은 구간 종료 시각.
    # 마지막 미완료 구간은 실제 현재 시각에 표시하며 시간을 임의로 이동하지 않음.
    now_seconds = now.timestamp()
    live_start = now_seconds - live_minutes * 60
    first_slot = math.floor(live_start / 5) * 5
    last_slot = (math.ceil(now_seconds / 5) - 1) * 5
    live_slots = list(range(first_slot, last_slot + 1, 5))
    live_rows = [row for row in filtered if live_start < row.ordered_at.timestamp() <= now_seconds]
    live = group_metrics(live_rows, selected, live_slots, 5, right_closed=True)
    live["times"] = [min((slot + 5) * 1000, int(now_seconds * 1000)) for slot in live_slots]
    live.update(window_start=int(live_start * 1000), window_end=int(now_seconds * 1000),
                window_minutes=live_minutes,
                current_interval_start=last_slot * 1000,
                orders=sum(item["orders"][-1] for item in live["series"]))
    durations = {"1h": 3600, "24h": 86400, "7d": 604800, "all": None}
    if period not in durations:
        raise ValueError("지원하지 않는 분석 기간입니다.")
    start = min(end, min((int(row.ordered_at.timestamp()) for row in filtered), default=end))
    if durations[period] is not None:
        start = end - durations[period]
    analysis_rows = [row for row in filtered if start <= row.ordered_at.timestamp() <= now_seconds]
    # 장기간 분석의 응답 크기를 제한하면서 전체 선택 기간을 유지.
    effective = max(interval, math.ceil(max(1, end - start) / 1500 / interval) * interval)
    slots = list(range(start // effective * effective, end // effective * effective + 1, effective))
    trend = group_metrics(analysis_rows, selected, slots, effective)
    for item in trend["series"]:
        quantity = item["quantity"]
        # 첫 구간의 이전 데이터가 없으므로 증감/증감률은 null로 표시.
        item["delta"] = [None] + [quantity[i] - quantity[i - 1] for i in range(1, len(quantity))]
        item["delta_pct"] = [None] + [round((quantity[i] - quantity[i - 1]) / quantity[i - 1] * 100, 2)
                                      if quantity[i - 1] else None for i in range(1, len(quantity))]
        total = 0
        item["cumulative"] = []
        for value in quantity:
            total += value
            item["cumulative"].append(total)
    # 누적 그래프는 전체 선택 기간의 시작부터 합산. 증감 확대와 독립적으로 유지.
    cumulative = trend
    latest_analysis = max((row.ordered_at.timestamp() for row in analysis_rows), default=None)
    if trend_minutes and latest_analysis is not None:
        # 현재까지 긴 공백이 있어도 최근 주문 구간을 확대하되 실제 시각은 그대로 유지.
        focus_end = min(now_seconds, (math.floor(latest_analysis / interval) + 2) * interval)
        focus_start = max(start, focus_end - trend_minutes * 60)
        focus_first = math.floor(focus_start / interval) * interval
        focus_last = math.floor(focus_end / interval) * interval
        focus_slots = list(range(focus_first, focus_last + 1, interval))
        focused_rows = [row for row in analysis_rows if focus_first <= row.ordered_at.timestamp() <= focus_end]
        trend = group_metrics(focused_rows, selected, focus_slots, interval)
        previous = group_metrics([row for row in analysis_rows
                                  if focus_first - interval <= row.ordered_at.timestamp() < focus_first],
                                 selected, [focus_first - interval], interval)
        for item, prior in zip(trend["series"], previous["series"]):
            quantity = item["quantity"]
            prev = prior["quantity"][0] if focus_first > start else None
            item["delta"] = [(quantity[0] - prev) if prev is not None else None]
            item["delta_pct"] = [round((quantity[0] - prev) / prev * 100, 2) if prev else None]
            for i in range(1, len(quantity)):
                item["delta"].append(quantity[i] - quantity[i - 1])
                item["delta_pct"].append(round((quantity[i] - quantity[i - 1]) / quantity[i - 1] * 100, 2)
                                         if quantity[i - 1] else None)
        trend.update(window_start=int(focus_start * 1000), window_end=int(focus_end * 1000))
    else:
        trend.update(window_start=start * 1000, window_end=int(now_seconds * 1000))
    trend["focus_minutes"] = trend_minutes
    cumulative.update(window_start=start * 1000, window_end=int(now_seconds * 1000))
    # 한국 정시 = UTC 정시 경계. 시간당 합계는 범주형 막대로 표시.
    # 주문이 없는 시간도 0으로 채워 불연속적인 시간대의 선 연결을 방지.
    first_hour, last_hour = start // 3600 * 3600, end // 3600 * 3600
    sparse_hours = (last_hour - first_hour) // 3600 > 2000
    hour_slots = (sorted({int(row.ordered_at.timestamp()) // 3600 * 3600 for row in analysis_rows})
                  if sparse_hours else list(range(first_hour, last_hour + 1, 3600)))
    hourly = group_metrics(analysis_rows, selected, hour_slots, 3600)
    hourly["sparse"] = sparse_hours
    product_groups = {}
    for row in analysis_rows:
        product_groups[row.product_id] = product_groups.get(row.product_id, 0) + row.quantity
    products = [{"name": key, "quantity": value}
                for key, value in sorted(product_groups.items(), key=lambda pair: (-pair[1], pair[0]))]
    summary = dict(orders=len(analysis_rows), quantity=sum(row.quantity for row in analysis_rows),
                   amount=float(sum((row.amount for row in analysis_rows), Decimal("0"))),
                   customers=len({row.customer_id for row in analysis_rows}),
                   products=len(product_groups))
    return dict(customers=customers, selected=selected, live=live, hourly=hourly,
                trend=trend, cumulative=cumulative, products=products, summary=summary,
                period_start=start * 1000, period_end=end * 1000,
                server_now=int(now_seconds * 1000),
                latest_order=max((row.ordered_at.isoformat() for row in filtered), default=None))


def clock_diagnostic(orders, now, db_timezone):
    latest = max((row.ordered_at for row in orders), default=None)
    extracted = max((row.extracted_at for row in orders), default=None)
    lag = (now - latest).total_seconds() if latest else None
    extraction_lag = (now - extracted).total_seconds() if extracted else None
    # 추정만 안내. 오래된 실제 주문을 현재로 옮기거나 원본 시간을 자동 변경하지 않음.
    suspected = bool(db_timezone == "Asia/Seoul" and lag is not None
                     and 8.5 * 3600 <= lag <= 9.5 * 3600
                     and extraction_lag is not None and -60 <= extraction_lag <= 120)
    return dict(order_lag_seconds=lag, extraction_lag_seconds=extraction_lag,
                possible_utc_source=suspected)


def create_app(store=None, settings=None, start_poller=True):
    @asynccontextmanager
    async def lifespan(app):
        actual = store if store is not None else ParquetStore(settings or Settings.from_env())
        app.state.store = actual
        if start_poller:
            actual.start()
        yield
        if start_poller:
            actual.stop()

    application = FastAPI(title="MinIO 주문 분석", lifespan=lifespan)
    @application.get("/")
    def index():
        # HTML과 함께 전달하므로 이전 static 파일/브라우저 캐시와 섞이지 않습니다.
        html = DASHBOARD_HTML.replace("<!--DASHBOARD_CSS-->", "<style>" + DASHBOARD_CSS + "</style>")
        html = html.replace("<!--DASHBOARD_JS-->",
                            "<script>window.addEventListener('DOMContentLoaded', function () {\n"
                            + DASHBOARD_JS + "\n});</script>")
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})

    # app.py 하나만 교체해도 기존 static 화면보다 이 내장 화면을 우선 사용.
    @application.get("/static/dashboard.js")
    def dashboard_js():
        return Response(DASHBOARD_JS, media_type="application/javascript",
                        headers={"Cache-Control": "no-store"})

    @application.get("/static/style.css")
    def dashboard_css():
        return Response(DASHBOARD_CSS, media_type="text/css",
                        headers={"Cache-Control": "no-store"})

    application.mount("/static", StaticFiles(directory=STATIC), name="static")

    @application.get("/healthz")
    def health():
        # MinIO 오류는 화면에서 안내. UI 자체의 생존 여부만 검사.
        return {"status": "ok"}

    @application.get("/api/dashboard")
    def dashboard(customer: str = Query("*", max_length=128),
                  live_minutes: int = Query(5, ge=1, le=60),
                  period: str = Query("24h", pattern="^(1h|24h|7d|all)$"),
                  interval: int = Query(60, ge=5, le=3600),
                  trend_minutes: int = Query(30, ge=0, le=60)):
        actual = application.state.store
        rows, status = actual.snapshot()
        try:
            now = datetime.now(UTC)
            data = analytics(rows, now, customer, live_minutes, period, interval, trend_minutes)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        data["status"] = status
        data["timezone"] = actual.settings.display_timezone
        data["db_timezone"] = actual.settings.db_timezone
        chosen = rows if customer == "*" else tuple(row for row in rows if row.customer_id == customer)
        data["clock_diagnostic"] = clock_diagnostic(chosen, now, actual.settings.db_timezone)
        data["app_version"] = "2.4"
        data["refresh_seconds"] = REFRESH_SECONDS
        return data

    return application



# 아래 웹 자산을 내장해 기존 프로젝트에서 app.py만 교체해도 화면이 갱신됩니다.
DASHBOARD_HTML = r"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>주문 관측소 · MinIO Analytics</title>
  <link rel="stylesheet" href="/static/fonts/noto-sans-kr.css">
  <!--DASHBOARD_CSS-->
  <script>
    window.dashboardFailure = function (title, detail) {
      const show = function () {
        const badge = document.getElementById('connection'), message = document.getElementById('message');
        if (badge) { badge.className = 'pill error'; badge.textContent = title; }
        if (message) { message.hidden = false; message.textContent = detail; }
      };
      if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', show, {once:true});
      else show();
    };
    window.addEventListener('error', function (event) {
      if (event.message) window.dashboardFailure('화면 실행 오류', '화면 코드 실행에 실패했습니다: ' + event.message);
    });
    window.addEventListener('unhandledrejection', function (event) {
      window.dashboardFailure('화면 실행 오류', '화면 코드 실행에 실패했습니다: ' + String(event.reason));
    });
    setTimeout(function () {
      const badge = document.getElementById('connection');
      if (badge && badge.textContent.includes('연결 확인 중'))
        window.dashboardFailure('초기화 지연', '20초 동안 화면 초기화가 완료되지 않았습니다. /api/dashboard 응답과 Pod 로그를 확인하세요.');
    }, 20000);
  </script>
  <script src="/static/echarts.min.js?v=2.4" defer></script>
</head>
<body>
  <header class="topbar"><div class="brand"><span class="brand-icon">↗</span><span>ORDER OBSERVATORY</span></div><span class="source">MinIO · Parquet</span></header>
  <main>
    <section class="heading">
      <div><div class="eyebrow">CUSTOMER ORDER ANALYTICS</div><h1>주문 흐름을 한눈에.</h1><p>고객의 주문이 들어오는 순간부터, 누적되는 흐름까지 확인하세요.</p></div>
      <div class="connection"><span id="connection" class="pill waiting"><i></i>연결 확인 중</span><small id="updated">5초마다 자동 갱신</small></div>
    </section>
    <div id="message" class="message" role="status" hidden></div>
    <section class="controls" aria-label="분석 조건">
      <label>고객<select id="customer"><option value="*">전체 고객</option></select></label>
      <div class="fixed-window"><span>실시간 표시 구간</span><strong>현재부터 직전 5분</strong><small>5초 단위 · 자동 이동</small></div>
      <label>하단 분석 기간<select id="period"><option value="1h">최근 1시간</option><option value="24h" selected>최근 24시간</option><option value="7d">최근 7일</option><option value="all">전체 기간</option></select></label>
      <div class="control-right"><span id="files">Parquet 읽는 중</span><button id="refresh" class="refresh" type="button">↻ 새로고침</button></div>
    </section>
    <section class="kpis" aria-label="선택한 분석 기간 합계">
      <article><span>주문 건수</span><strong id="kpi-orders">—</strong><small>중복 제거한 주문</small></article>
      <article><span>총 주문량</span><strong id="kpi-quantity">—</strong><small>수량 합계</small></article>
      <article><span>총 주문액</span><strong id="kpi-amount">—</strong><small>주문액 합계 · 원</small></article>
      <article><span>주문 고객 / 제품</span><strong id="kpi-customers">—</strong><small>분석 기간 내 고유 개수</small></article>
    </section>
    <section class="live-section">
      <div class="section-heading"><div><h2><span class="live-dot"></span>실시간 주문 흐름</h2><p>직전 5분의 주문 · 고객별 꺾은선 · 오른쪽 끝은 현재 시각</p></div><span id="live-range" class="small-tag">최근 5분 · 5초 갱신</span></div>
      <div class="live-grid">
        <article class="chart-card"><div class="card-title"><h3>주문량</h3><span>5초 구간 수량 합계</span></div><div id="live-quantity" class="chart small" role="img" aria-label="고객별 직전 5분 주문량 꺾은선 그래프"></div><div id="current-quantity" class="current-values" aria-label="현재 구간 고객별 주문량"></div></article>
        <article class="chart-card"><div class="card-title"><h3>주문액</h3><span>5초 구간 금액 합계 · 원</span></div><div id="live-amount" class="chart small" role="img" aria-label="고객별 직전 5분 주문액 꺾은선 그래프"></div><div id="current-amount" class="current-values" aria-label="현재 구간 고객별 주문액"></div></article>
      </div>
      <p class="footnote">각 점은 해당 5초 구간의 주문 합계입니다. 오른쪽 점과 고객별 값은 현재 구간의 합계이며, ETL 저장 뒤 반영됩니다. 주문이 없는 구간은 0입니다.</p>
      <p id="latest-order-note" class="footnote"></p>
    </section>
    <section class="analysis-section">
      <div class="section-heading"><div><h2>주문 분석</h2><p>아래 버튼을 눌러 같은 데이터를 다른 관점으로 살펴보세요.</p></div><span id="range-label" class="range-label"></span></div>
      <div class="tabs" role="tablist" aria-label="분석 종류">
        <button id="tab-hourly" data-view="hourly" role="tab" aria-selected="true" class="active">고객별 시간당 집계</button>
        <button id="tab-trend" data-view="trend" role="tab" aria-selected="false">고객별 주문량 증감추이</button>
        <button id="tab-cumulative" data-view="cumulative" role="tab" aria-selected="false">고객별 누적주문량</button>
        <button id="tab-products" data-view="products" role="tab" aria-selected="false">제품별 주문 총량</button>
      </div>
      <article class="chart-card analysis-card" role="tabpanel" aria-labelledby="tab-hourly" id="analysis-panel">
        <div class="analysis-head"><div><h3 id="analysis-title">고객별 시간당 주문량</h3><p id="analysis-description">주문이 발생한 시간대를 1시간 단위로 묶어 비교합니다.</p></div>
          <div class="analysis-selects"><label id="trend-window-label">증감 표시 구간<select id="trend-window"><option value="5">최근 주문 5분</option><option value="15">최근 주문 15분</option><option value="30" selected>최근 주문 30분</option><option value="60">최근 주문 1시간</option><option value="0">선택 기간 전체</option></select></label><label id="interval-label">집계 간격<select id="interval"><option value="5">5초</option><option value="10">10초</option><option value="30">30초</option><option value="60" selected>1분</option><option value="300">5분</option><option value="3600">1시간</option></select></label><label id="metric-label">표시 지표<select id="metric"><option value="quantity">주문량</option><option value="amount">주문액</option><option value="items">주문품목 수</option></select></label></div>
        </div>
        <div id="analysis-chart" class="chart large" role="img" aria-label="주문 분석 그래프"></div>
        <p id="analysis-note" class="footnote"></p>
      </article>
    </section>
    <footer><span>주문 시각 · 한국 시간 기본 / 최신 주문 버전 기준</span><span>v2.4 · Charts by Apache ECharts</span></footer>
  </main>
  <!--DASHBOARD_JS-->
</body>
</html>
"""

DASHBOARD_CSS = r""":root{--ink:#172234;--muted:#718096;--line:#e5eaf0;--bg:#f6f8fb;--blue:#4165e8;--green:#139887}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,"Pretendard","Noto Sans KR",system-ui,-apple-system,"Segoe UI",sans-serif;font-size:14px}.topbar{height:66px;background:#142037;color:#fff;padding:0 max(28px,calc((100vw - 1440px)/2));display:flex;align-items:center;justify-content:space-between}.brand{display:flex;gap:12px;align-items:center;font-weight:700;letter-spacing:1.8px;font-size:13px}.brand-icon{width:32px;height:32px;background:#4165e8;border-radius:9px;display:grid;place-items:center;font-size:24px}.source{color:#adbed7;font-size:12px;letter-spacing:1px}main{max-width:1440px;margin:auto;padding:34px 28px 20px}.heading{display:flex;justify-content:space-between;align-items:center;gap:20px;margin-bottom:27px}.eyebrow{font-size:11px;letter-spacing:2px;color:var(--blue);font-weight:700}h1{font-size:30px;letter-spacing:-1.2px;margin:8px 0 10px}p{color:var(--muted);margin:0;line-height:1.6}.connection{display:flex;flex-direction:column;align-items:flex-end;gap:9px}.pill{display:flex;align-items:center;gap:8px;border:1px solid #d0eee4;background:#edf9f3;color:#138861;border-radius:30px;padding:8px 12px;font-size:12px;font-weight:600}.pill i,.live-dot{width:7px;height:7px;border-radius:50%;background:currentColor;display:inline-block}.pill.waiting{background:#fff5e7;border-color:#f5dfb7;color:#ab7416}.pill.error{background:#fff0f0;border-color:#f4cdcd;color:#c04e4e}.connection small{font-size:11px;color:var(--muted)}.controls{display:flex;gap:22px;align-items:center;background:#fff;border:1px solid var(--line);border-radius:12px;padding:16px 20px;margin-bottom:20px}label{font-size:11px;color:var(--muted);display:flex;gap:6px;flex-direction:column}select{border:1px solid #dfe5ed;border-radius:7px;background:white;padding:8px 28px 8px 10px;font:inherit;font-size:13px;color:var(--ink);min-width:140px}select:focus,button:focus-visible{outline:2px solid #91a7ff;outline-offset:2px}.control-right{margin-left:auto;display:flex;align-items:center;gap:14px;color:var(--muted);font-size:11px}.refresh{background:white;border:1px solid var(--line);border-radius:7px;padding:9px 12px;color:#3b4b63;cursor:pointer}.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:32px}.kpis article{border:1px solid var(--line);background:white;border-radius:12px;padding:20px 22px;display:flex;flex-direction:column;gap:9px}.kpis article>span{font-size:12px;color:var(--muted)}.kpis strong{font-size:27px;font-weight:650;letter-spacing:-.7px}.kpis small{color:#9aa5b5;font-size:11px}.section-heading{display:flex;justify-content:space-between;align-items:center;margin-bottom:17px;gap:15px}h2{font-size:18px;letter-spacing:-.5px;margin:0 0 6px}h3{font-size:15px;margin:0}.section-heading p{font-size:12px}.live-dot{color:var(--green);margin-right:9px;margin-bottom:2px}.small-tag{font-size:10px;letter-spacing:1px;background:#eaf0ff;color:#526bbb;border-radius:5px;padding:7px 9px;white-space:nowrap}.live-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}.chart-card{background:#fff;border:1px solid var(--line);border-radius:12px;overflow:hidden}.card-title{display:flex;justify-content:space-between;align-items:center;padding:19px 20px 0}.card-title span{font-size:11px;color:var(--muted)}.chart{width:100%}.chart.small{height:370px}.chart.large{height:385px}.footnote{font-size:11px;line-height:1.7;color:#8c99aa;margin-top:11px}.analysis-section{margin-top:32px}.range-label{font-size:11px;color:var(--muted)}.tabs{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:15px}.tabs button{border:1px solid var(--line);border-radius:9px;padding:14px 8px;background:white;color:#66768b;font-size:13px;cursor:pointer;transition:background .2s,color .2s}.tabs button:hover{border-color:#becbfa;background:#f2f5ff}.tabs button.active{background:var(--blue);border-color:var(--blue);color:white;font-weight:600;box-shadow:0 3px 8px #4165e820}.analysis-head{padding:22px 24px 0;display:flex;flex-wrap:wrap;justify-content:space-between;gap:14px;align-items:center}.analysis-head p{font-size:12px;margin-top:7px}.analysis-selects{display:flex;gap:12px;flex-wrap:wrap}.analysis-selects select{min-width:100px}.analysis-card>.footnote{margin:0;padding:0 24px 18px}footer{display:flex;justify-content:space-between;border-top:1px solid var(--line);padding:20px 0 4px;margin-top:28px;font-size:10px;color:#9aa5b5}.message{padding:12px 16px;border-radius:9px;border:1px solid #f0dab3;background:#fff8eb;color:#8c6220;line-height:1.7;font-size:12px;margin-bottom:16px}[hidden]{display:none!important}
button{font-family:inherit}
@media(min-width:1600px){.chart.small{height:390px}}@media(max-width:1000px){.controls{flex-wrap:wrap;gap:15px}.control-right{margin-left:0}.live-grid{grid-template-columns:1fr}.chart.small{height:330px}.kpis strong{font-size:22px}.tabs{grid-template-columns:repeat(2,1fr)}}@media(max-width:600px){main{padding:24px 16px}.topbar{padding:0 16px}.heading{align-items:flex-start;flex-direction:column}.connection{align-items:flex-start}h1{font-size:26px}.kpis{grid-template-columns:repeat(2,1fr);gap:10px}.kpis article{padding:16px}.kpis strong{font-size:22px}.controls{padding:14px}.controls label{flex:1}.controls select{width:100%;min-width:120px}.control-right{width:100%;justify-content:space-between}.section-heading{align-items:flex-start}.small-tag{display:none}.analysis-head{padding:20px 16px 0;flex-direction:column;align-items:flex-start}.analysis-selects{width:100%}.chart.large{height:350px}.tabs button{font-size:12px}.range-label{display:none}footer{gap:10px;flex-direction:column}.source{font-size:10px}.brand{font-size:11px}}

.fixed-window{display:flex;flex-direction:column;gap:5px;color:var(--muted);font-size:11px}.fixed-window strong{font-size:13px;color:var(--ink);font-weight:600}.fixed-window small{font-size:10px}.current-values{padding:4px 20px 17px;display:flex;gap:8px;flex-wrap:wrap}.current-values span{font-size:11px;color:#596b82;background:#f6f8fb;border:1px solid var(--line);padding:6px 9px;border-radius:6px}.current-values i{display:inline-block;width:6px;height:6px;border-radius:50%;margin-right:5px}.current-values b{color:var(--ink);font-weight:600;margin-left:6px}
"""

DASHBOARD_JS = r"""'use strict';
const $ = id => document.getElementById(id);
const colors = ['#4165e8','#16a394','#ef9a40','#af6bda','#e76d85','#57a6cf','#73864b','#ac7c59'];
const charts = {};
let chartError = null;
const number = new Intl.NumberFormat('ko-KR', {maximumFractionDigits:2});
const escapeHTML = v => String(v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let data = null, view = 'hourly', busy = false, pending = false;
let chartZoom = {}; // 자동 갱신 중 사용자가 선택한 확대 범위 유지
let chartLegend = {};
function ensureCharts() {
  try {
    if (!window.echarts) throw new Error('/static/echarts.min.js를 읽지 못했습니다. Docker 이미지에 static/echarts.min.js가 포함되어 있는지 확인하세요.');
    for (const [key, id] of Object.entries({quantity:'live-quantity',amount:'live-amount',analysis:'analysis-chart'})) {
      if (charts[key]) continue;
      const element=$(id);
      if (!element) throw new Error('그래프 영역을 찾을 수 없습니다: '+id);
      const chart=echarts.init(element);
      chart.on('datazoom', event => {const zoom = event.batch ? event.batch[0] : event; if (zoom.start !== undefined) chartZoom[key] = {start:zoom.start,end:zoom.end};});
      chart.on('legendselectchanged', event => {chartLegend[key] = event.selected;});
      charts[key]=chart;
    }
    chartError=null;
    return true;
  } catch(error) {
    chartError=error;
    return false;
  }
}
const metricLabels = {quantity:'주문량', amount:'주문액', items:'주문품목 수', delta:'주문량 증감', delta_pct:'주문량 증감률', cumulative:'누적 주문량'};
function timeLabel(value, live=false, includeSeconds=false) {
  const opts = {timeZone:data?.timezone || 'Asia/Seoul', hour:'2-digit',minute:'2-digit',hour12:false};
  if (live || includeSeconds) opts.second='2-digit';
  if (!live) {opts.month='2-digit';opts.day='2-digit';}
  return new Intl.DateTimeFormat('ko-KR',opts).format(new Date(value));
}
function colorFor(name) {return colors[Math.max(0,data.customers.indexOf(name)) % colors.length];}
function lineOption(group, metric, key, live=false) {
  const zoom = chartZoom[key] || {start:0,end:100};
  return {
    backgroundColor:'transparent', animation:false, color:group.series.map(s=>colorFor(s.name)),
    textStyle:{fontFamily:'Noto Sans KR, system-ui, sans-serif'},
    legend:{type:'scroll',selected:chartLegend[key]||{},top:14,left:20,right:15,icon:'roundRect',itemWidth:12,itemHeight:3,textStyle:{fontSize:10,color:'#718096'}},
    grid:{left:live?58:64,right:live?125:35,top:55,bottom:live?58:70},
    tooltip:{trigger:'axis',backgroundColor:'#fff',borderColor:'#e5eaf0',textStyle:{color:'#26364b',fontSize:12},
      formatter:params=>{
        if(!params.length) return '';
        let out=`<b>${escapeHTML(timeLabel(params[0].axisValue,live,group.seconds<60))}</b>`;
        for(const p of params){
          const val=p.value[1], series=group.series[p.seriesIndex];
          out+=`<br>${p.marker}${escapeHTML(p.seriesName)}: <b>${val===null?'—':number.format(val)}</b>${metric==='amount'?' 원':metric==='delta_pct'?'%':''}`;
          if(metric==='items' && series.products[p.dataIndex]?.length) out+=`<br><span style="color:#8793a3;font-size:10px">${escapeHTML(series.products[p.dataIndex].join(', '))}</span>`;
          if(metric==='delta' && series.delta_pct?.[p.dataIndex]!=null) out+=` <span style="color:#8793a3">(${number.format(series.delta_pct[p.dataIndex])}%)</span>`;
        }
        return out;
      }},
    xAxis:{type:'time',splitNumber:live?5:6,minInterval:live?60000:group.seconds*1000,maxInterval:live?60000:undefined,axisLine:{lineStyle:{color:'#e3e8ef'}},axisTick:{show:false},axisLabel:{fontSize:10,color:'#9aa5b5',hideOverlap:true,formatter:value=>timeLabel(value,true,group.seconds<60)},splitLine:{show:false},min:group.window_start,max:group.window_end>group.window_start?group.window_end:group.window_start+group.seconds*1000},
    yAxis:{type:'value',minInterval:metric==='amount'||metric==='delta_pct'?0:1,axisLabel:{fontSize:10,color:'#9aa5b5',formatter:v=>Math.abs(v)>=10000?number.format(v/10000)+'만':number.format(v)},splitLine:{lineStyle:{color:'#eef1f5',type:'dashed'}}},
    dataZoom:live?[]:[{type:'inside',...zoom},{type:'slider',height:12,bottom:20,borderColor:'transparent',backgroundColor:'#f1f4f9',fillerColor:'#4165e815',handleSize:0,showDetail:false,...zoom}],
    series:group.series.map(s=>({name:s.name,type:'line',showSymbol:live||group.times.length<=80,symbolSize:live?0:4,lineStyle:{width:live?2.5:2},emphasis:{focus:'series'},connectNulls:false,labelLayout:{moveOverlap:'shiftY',hideOverlap:true},step:metric==='cumulative'?'end':false,data:group.times.map((t,i)=>live&&i===group.times.length-1?{value:[t,s[metric][i]],symbolSize:7,label:{show:true,position:'right',distance:8,fontSize:10,color:colorFor(s.name),formatter:()=>`${s.name} · ${number.format(s[metric][i])}`}}:[t,s[metric][i]])})),
    graphic:live?[{type:'text',left:58,bottom:15,style:{text:'5분 전 '+timeLabel(group.window_start,true),fill:'#8b99ac',fontSize:10,fontFamily:'Noto Sans KR'}},{type:'text',right:125,bottom:15,style:{text:'현재 '+timeLabel(group.window_end,true),fill:'#4165e8',fontSize:10,fontFamily:'Noto Sans KR'}}].concat(group.series.some(s=>s.orders.some(n=>n>0))?[]:[{type:'text',left:'center',top:'middle',style:{text:'직전 5분에 주문이 없습니다',fill:'#94a0b1',fontSize:13}}]):group.series.some(s=>s.orders.some(n=>n>0))?[]:[{type:'text',left:'center',top:'middle',style:{text:'표시할 주문 데이터가 없습니다',fill:'#94a0b1',fontSize:13}}]
  };
}
function hourlyOption(metric) {
  const group=data.hourly, zoom=chartZoom.analysis||{start:0,end:100};
  return {color:group.series.map(s=>colorFor(s.name)),textStyle:{fontFamily:'Noto Sans KR, system-ui, sans-serif'},
    legend:{type:'scroll',selected:chartLegend.analysis||{},top:14,left:24,right:20,itemWidth:10,itemHeight:10,textStyle:{fontSize:11,color:'#718096'}},
    grid:{left:68,right:30,top:58,bottom:75},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},backgroundColor:'#fff',borderColor:'#e5eaf0',textStyle:{color:'#26364b',fontSize:12},formatter:params=>{
      if(!params.length)return '';
      const index=params[0].dataIndex, hour=group.times[index];
      let out=`<b>${escapeHTML(timeLabel(hour))} – ${escapeHTML(timeLabel(hour+3600000))}</b>`;
      for(const p of params){out+=`<br>${p.marker}${escapeHTML(p.seriesName)}: <b>${number.format(p.value)}</b>${metric==='amount'?' 원':''}`;
        if(metric==='items'&&group.series[p.seriesIndex].products[index]?.length)out+=`<br><small>${escapeHTML(group.series[p.seriesIndex].products[index].join(', '))}</small>`;}
      return out;
    }},
    xAxis:{type:'category',data:group.times.map(t=>timeLabel(t)),axisLabel:{color:'#8b99ac',fontSize:10,interval:0,hideOverlap:false,rotate:30,formatter:(_value,index)=>new Intl.DateTimeFormat('en-GB',{timeZone:data.timezone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(new Date(group.times[index]))},axisTick:{interval:0,alignWithLabel:true},axisLine:{lineStyle:{color:'#e3e8ef'}}},
    yAxis:{type:'value',minInterval:metric==='amount'?0:1,axisLabel:{fontSize:10,color:'#9aa5b5',formatter:v=>Math.abs(v)>=10000?number.format(v/10000)+'만':number.format(v)},splitLine:{lineStyle:{color:'#eef1f5',type:'dashed'}}},
    dataZoom:[{type:'inside',...zoom},{type:'slider',height:12,bottom:20,showDetail:false,borderColor:'transparent',...zoom}],
    series:group.series.map(s=>({name:s.name,type:'bar',data:s[metric],barMaxWidth:28,barGap:'12%',barCategoryGap:'25%',itemStyle:{borderRadius:[3,3,0,0]},emphasis:{focus:'series'}})),
    graphic:group.series.some(s=>s.orders.some(v=>v>0))?[]:[{type:'text',left:'center',top:'middle',style:{text:'선택 기간에 주문이 없습니다',fill:'#94a0b1',fontSize:13}}]};
}
function productOption() {
  const zoom=chartZoom.analysis||{start:0,end:100};
  return {textStyle:{fontFamily:'Noto Sans KR, system-ui, sans-serif'},grid:{left:64,right:35,top:35,bottom:70},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>params.length?`${escapeHTML(params[0].name)}<br>총 주문량: <b>${number.format(params[0].value)}</b>`:''},
    xAxis:{type:'category',data:data.products.map(p=>p.name),axisLabel:{color:'#718096',fontSize:11,hideOverlap:true},axisTick:{show:false},axisLine:{lineStyle:{color:'#e3e8ef'}}},
    yAxis:{type:'value',minInterval:1,axisLabel:{color:'#9aa5b5',fontSize:10},splitLine:{lineStyle:{color:'#eef1f5',type:'dashed'}}},
    dataZoom:[{type:'inside',...zoom},{type:'slider',height:12,bottom:20,showDetail:false,borderColor:'transparent',...zoom}],
    series:[{type:'bar',name:'총 주문량',data:data.products.map(p=>p.quantity),barMaxWidth:46,itemStyle:{color:'#4165e8',borderRadius:[5,5,0,0]},label:{show:true,position:'top',color:'#66768b',fontSize:11,formatter:p=>number.format(p.value)}}],
    graphic:data.products.length?[]:[{type:'text',left:'center',top:'middle',style:{text:'선택 기간에 주문이 없습니다',fill:'#94a0b1',fontSize:13}}]};
}
function setMetricOptions() {
  const values=view==='trend'?['delta','quantity','delta_pct']:['quantity','amount','items'];
  $('metric').replaceChildren(...values.map(value=>{const o=document.createElement('option');o.value=value;o.textContent=metricLabels[value];return o;}));
}
function fitTrendInterval() {
  const minutes=Number($('trend-window').value);
  for(const option of $('interval').options) option.disabled=view==='trend'&&minutes>0&&Number(option.value)>minutes*60/2;
  if($('interval').selectedOptions[0]?.disabled) {
    $('interval').value='60';
    delete chartZoom.analysis;
    return true;
  }
  return false;
}
function renderAnalysis() {
  if (!data || !charts.analysis) return;
  $('metric-label').hidden=view==='products'||view==='cumulative';
  $('interval-label').hidden=view==='hourly'||view==='products';
  $('trend-window-label').hidden=view!=='trend';
  const metric=view==='cumulative'?'cumulative':$('metric').value;
  const group=view==='cumulative'?data.cumulative:data.trend;
  const seconds=group.seconds;
  const intervalText=seconds>=3600?`${number.format(seconds/3600)}시간`:seconds>=60?`${number.format(seconds/60)}분`:`${seconds}초`;
  const titles={hourly:`고객별 시간당 ${metricLabels[metric]}`,trend:`고객별 ${metricLabels[metric]} 추이`,cumulative:'고객별 누적 주문량',products:'제품별 총 주문량'};
  const focus=data.trend.focus_minutes?`최근 주문이 발생한 ${data.trend.focus_minutes}분 구간`:'선택 기간 전체';
  const descriptions={hourly:'시간대마다 고객별 막대를 나란히 배치해 1시간 합계를 비교합니다.',trend:`${focus} · ${intervalText} 집계 · ${timeLabel(data.trend.window_start)} – ${timeLabel(data.trend.window_end)}`,cumulative:'선택한 분석 기간의 시작부터 주문 수량을 차례로 더합니다.',products:'선택한 고객·분석 기간의 제품별 수량을 많은 순서대로 표시합니다.'};
  $('analysis-title').textContent=titles[view];$('analysis-description').textContent=descriptions[view];
  $('analysis-panel').setAttribute('aria-labelledby','tab-'+view);
  $('analysis-chart').setAttribute('aria-label',titles[view]);
  $('analysis-note').textContent=view==='trend'?'증감 = 현재 구간 수량 − 직전 구간 수량. 직전 값이 0이면 증감률은 표시하지 않습니다. 첫 구간과 현재 진행 중인 구간의 해석에 유의하세요.':view==='cumulative'?`선택 기간 내 누적값입니다. ${intervalText} 구간 끝까지 발생한 주문을 합산하며, 수정 주문은 최신 버전으로 반영됩니다.`:view==='hourly'?`각 막대는 정시부터 다음 정시 직전까지의 합계입니다. ${data.hourly.sparse?'장기간 분석에서는 주문이 있는 시간대만 표시합니다.':'주문이 없는 시간대는 0입니다.'} 현재 시간대는 진행 중인 합계입니다.`:'제품별 수량 합계입니다. 같은 주문의 수정본과 재수집 중복은 합산하지 않습니다.';
  const option=view==='products'?productOption():view==='hourly'?hourlyOption(metric):lineOption(group,metric,'analysis');
  charts.analysis.setOption(option,{notMerge:true});
}
function render() {
  ensureCharts();
  const selected=$('customer').value;
  const options=['*',...data.customers];
  if(JSON.stringify([...$('customer').options].map(o=>o.value))!==JSON.stringify(options)){
    $('customer').replaceChildren(...options.map(value=>{const o=document.createElement('option');o.value=value;o.textContent=value==='*'?'전체 고객':value;return o;}));
    if(options.includes(selected)) $('customer').value=selected;
  }
  $('kpi-orders').textContent=number.format(data.summary.orders);
  $('kpi-quantity').textContent=number.format(data.summary.quantity);
  $('kpi-amount').textContent=number.format(data.summary.amount);
  $('kpi-customers').textContent=`${data.summary.customers} / ${data.summary.products}`;
  $('files').textContent=`Parquet ${number.format(data.status.file_count)}개 · 고유 주문 ${number.format(data.status.order_count)}건`;
  $('range-label').textContent=timeLabel(data.period_start)+' – '+timeLabel(data.period_end);
  const stale=data.status.last_success && Date.now()-Date.parse(data.status.last_success)>20000;
  let connection=data.status.error?'갱신 오류':!data.status.last_success?'데이터 읽는 중':stale?'갱신 지연':'실시간 연결됨';
  $('connection').className='pill '+(data.status.error?'error':!data.status.last_success||stale?'waiting':'');
  $('connection').replaceChildren(Object.assign(document.createElement('i'),{}),document.createTextNode(connection));
  $('updated').textContent=data.status.last_success?`마지막 확인 ${timeLabel(data.status.last_success,true)} · 5초 갱신`:'5초마다 자동 갱신';
  let message=data.status.error;
  if(message && data.status.last_success) message+=' 마지막 정상 데이터를 표시하고 있습니다.';
  if(!message && data.status.last_success && !data.status.order_count) message='아직 Parquet 주문 데이터가 없습니다. ETL의 저장 경로와 실행 상태를 확인하세요.';
  if(!message && data.latest_order && Date.parse(data.latest_order)>Date.now()+60000) message='주문 시각이 현재보다 미래입니다. 원천 주문 시각의 시간대와 DB_TIMEZONE 설정을 확인하세요.';
  if(!message && data.status.order_count && !data.live.series.some(s=>s.orders.some(n=>n>0))) message='직전 5분에 신규 주문이 없습니다. 주문 시뮬레이터의 실행 상태와 원본 주문 시각의 시간대를 확인하세요. 과거 데이터는 아래 분석 화면에서 볼 수 있습니다.';
  if(!data.status.error && data.clock_diagnostic?.possible_utc_source) message='MinIO에는 최근 추출 데이터가 있지만 주문 시각은 약 9시간 전입니다. UTC 주문 시각을 한국 시각으로 해석했을 가능성이 있습니다. 원본이 UTC라면 DB_TIMEZONE=UTC, DISPLAY_TIMEZONE=Asia/Seoul로 설정하세요.';
  $('message').hidden=!message;$('message').textContent=message||'';
  $('live-range').textContent=`${timeLabel(data.live.window_start,true)} → 현재 ${timeLabel(data.live.window_end,true)}`;
  const recentOrders=data.live.series.reduce((sum,s)=>sum+s.orders.reduce((a,b)=>a+b,0),0);
  $('latest-order-note').textContent=`현재 시각: ${timeLabel(data.server_now,true)} · 직전 5분 주문 ${number.format(recentOrders)}건 · `+(data.latest_order?`최근 주문 시각: ${timeLabel(data.latest_order,true)} · 원본 시간대: ${data.db_timezone} · 화면 시간대: ${data.timezone}`:'최근 주문 시각: 데이터 없음');
  for(const metric of ['quantity','amount']) {
    if (charts[metric]) charts[metric].setOption(lineOption(data.live,metric,metric,true),{notMerge:true});
    const label=document.createElement('span');label.textContent='현재 구간';
    const chips=data.live.series.map(s=>{const chip=document.createElement('span'),dot=document.createElement('i'),value=document.createElement('b');dot.style.background=colorFor(s.name);value.textContent=number.format(s[metric].at(-1))+(metric==='amount'?' 원':'');chip.append(dot,document.createTextNode(s.name),value);return chip;});
    $('current-'+metric).replaceChildren(label,...chips);
  }
  renderAnalysis();
  if(chartError) window.dashboardFailure('그래프 초기화 오류', chartError.message+' 주문 데이터 조회는 계속 진행합니다.');
}
async function refresh() {
  if(busy){pending=true;return;}
  busy=true;const query=new URLSearchParams({customer:$('customer').value,live_minutes:5,period:$('period').value,interval:$('interval').value,trend_minutes:$('trend-window').value});
  const controller=new AbortController(), timeout=setTimeout(()=>controller.abort(),20000);
  let stage='API 조회';
  try {
    const response=await fetch('/api/dashboard?'+query,{cache:'no-store',signal:controller.signal});
    if(!response.ok) throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0,200)}`);
    data=await response.json();
    stage='화면 표시';
    if(data.app_version!=='2.4') throw new Error(`화면은 v2.4인데 API는 v${data.app_version||'알 수 없음'}입니다. 실행 중인 이미지와 Service 연결을 확인하세요.`);
    render();
  } catch(error) {
    const detail=error.name==='AbortError'?'20초 동안 응답이 없습니다.':error.message;
    window.dashboardFailure(stage==='API 조회'?'서버 조회 오류':'화면 표시 오류', `${stage} 실패: ${detail} 5초마다 재시도합니다.`);
  } finally {
    clearTimeout(timeout);
    busy=false;
    if(pending){pending=false;refresh();}
  }
}
for(const id of ['customer','period','interval','trend-window']) $(id).addEventListener('change',()=>{chartZoom={};fitTrendInterval();refresh();});
$('refresh').addEventListener('click',refresh);
$('metric').addEventListener('change',()=>{delete chartZoom.analysis;renderAnalysis();});
document.querySelectorAll('[data-view]').forEach(button=>button.addEventListener('click',()=>{
  view=button.dataset.view;delete chartZoom.analysis;
  document.querySelectorAll('[data-view]').forEach(b=>{b.classList.toggle('active',b===button);b.setAttribute('aria-selected',String(b===button));});
  setMetricOptions();
  if(fitTrendInterval()) refresh(); else renderAnalysis();
}));
const resizeCharts=()=>Object.values(charts).forEach(chart=>chart.resize());
if(window.ResizeObserver) new ResizeObserver(resizeCharts).observe(document.querySelector('main'));
else window.addEventListener('resize',resizeCharts);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
ensureCharts();
refresh(); // 웹폰트 로딩을 기다리지 않고 API를 즉시 조회합니다.
if(document.fonts) document.fonts.ready.then(resizeCharts);
setInterval(()=>{if(!document.hidden)refresh();},5000);
"""

app = create_app()
