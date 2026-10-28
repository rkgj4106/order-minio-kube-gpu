# MinIO Parquet 주문 분석 대시보드

실습 2단계: 기존 ETL이 MinIO에 저장한 주문 Parquet을 읽어 그래프로 분석합니다.
대상 환경은 **220.149.119.234 서버의 Kubernetes**, 서비스는 **NodePort 30111**입니다.
GPU, Spark, Iceberg, MySQL 직접 연결은 필요하지 않습니다.

## 화면 구성

- 상단: 고객별 5초 구간의 **주문량 / 주문액** 꺾은선 그래프 2개.
- 화면과 MinIO 조회는 5초마다 갱신. 고객별로 동일한 색상을 사용합니다.
- 고객 선택, 현재 시각부터 직전 5분으로 고정한 상단 그래프, 하단 분석 기간(1시간/24시간/7일/전체).
- 하단 버튼 4개:
  1. 고객별 시간당 집계: 시간대별 고객 막대를 나란히 배치. 주문량·주문액·주문품목 수 지표 선택.
  2. 고객별 주문량 증감추이: 주문량 증감·주문량·증감률 지표 선택.
  3. 고객별 누적주문량: 선택 기간 내 누적 수량.
  4. 제품별 주문 총량: 제품별 수량 합계를 내림차순 막대그래프로 표시.
- 하단 그래프의 확대/축소 슬라이더, 고객 범례 표시/숨기기, 시간당 품목 수의 제품 ID 툴팁.
- 오픈소스 Apache ECharts 6.0.0을 프로젝트에 포함. 브라우저에서 외부 CDN을 호출하지 않습니다.
- 한국어 웹폰트 Noto Sans KR 및 OFL 라이선스도 포함합니다.

## 집계 기준

| 항목 | 기준 |
|---|---|
| 주문 시각 | `order_time` 기준. `etl_extracted_at`은 중복 제거에만 사용 |
| 주문품목 | 각 고객·시간 구간 내 서로 다른 `product_id` 수. 툴팁에 제품 ID 표시 |
| 주문량 | `quantity` 합계 |
| 주문액 | `total_amount` 합계. Decimal로 집계한 뒤 그래프 응답에서 숫자로 변환 |
| 주문 버전 | `order_id`별 가장 최신 `etl_extracted_at` 행 한 건만 반영 |
| 주문량 증감 | 현재 구간 수량 − 직전 구간 수량. 주문이 없는 구간은 0 |
| 증감률 | `(현재 − 직전) / 직전 × 100`. 직전 값 0 또는 첫 구간이면 계산하지 않음 |
| 누적 주문량 | 선택한 분석 기간의 시작부터 집계. 전체 누적은 ‘전체 기간’ 선택 |
| 제품별 총량 | 선택한 고객과 분석 기간에 포함된 주문의 제품별 수량 합계 |

주문품목 수는 하단 시간당 집계의 지표이며 **고유 제품 개수**를 사용합니다. 상단에는 품목 그래프를 표시하지 않습니다.
제품별 상세 수량은 하단 ‘제품별 주문 총량’에서 확인합니다.
상단 그래프는 현재 시각 기준이며, 종료된 시뮬레이션의 과거 데이터는 하단 분석에서 확인합니다.
시간당 집계는 주문이 없는 시간도 0으로 채우며 선으로 보간하지 않습니다. 전체 기간이 2,000시간을 넘으면 주문이 있는 시간대만 표시합니다. 증감·누적 그래프도 빈 구간을 0으로 채웁니다.
장기간의 증감·누적 그래프는 약 1,500개 구간을 넘으면 집계 간격을 자동으로 확대하고 화면에 실제 간격을 표시합니다.
현재 진행 중인 구간은 아직 미완료이므로 값과 증감률이 계속 변할 수 있습니다.

**중요:** 화면 갱신 5초와 ETL 수집 10초는 다릅니다. 새 주문은 ETL의 Parquet 저장 뒤 화면에 나타납니다.
원래 ETL이 삭제 이벤트를 저장하지 않으므로 원천 DB 주문 삭제는 이 대시보드에서도 감지하지 못합니다.
수정 주문은 과거 주문 시각의 집계도 최신 값으로 바뀝니다. 이는 수정 이벤트 건수를 집계하는 화면이 아닙니다.

## 1. GPU 서버에 접속하여 압축 해제

아래 명령은 **Ubuntu GPU 서버의 터미널**에서 실행합니다.
Windows에서는 다운로드한 ZIP을 SCP 등으로 서버에 복사한 뒤 SSH로 접속하세요.

```bash
unzip order-dashboard.zip
cd order-dashboard
```

## 2. 기존 ETL 연결 설정 확인

배포 YAML은 이미 동작하는 ETL의 MinIO 설정과 인증정보를 재사용합니다.
기존 ConfigMap/Secret을 다시 만들거나 덮어쓰지 않습니다.

```bash
kubectl -n default get configmap order-etl-config
kubectl -n default get secret order-etl-secret
kubectl -n default get configmap order-etl-config -o yaml
```

필요 키:

- ConfigMap `order-etl-config`: `MINIO_ENDPOINT`, `MINIO_BUCKET`, `MINIO_PREFIX`
- Secret `order-etl-secret`: `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`

기본 데이터 경로는 `s3://warehouse/raw/orders_step1/`이며 `.parquet` 객체만 읽습니다.
`_checkpoint/state.json`은 분석 대상에서 제외합니다.
실제 이름이 다르면 `order-dashboard.yaml`의 참조 이름을 변경하세요.
ETL이 다른 namespace에서 실행 중이면 YAML의 **세 군데 metadata.namespace**를 같은 namespace로 바꾸고, 아래 명령의 `-n default`도 함께 변경하세요.
ConfigMap과 Secret은 다른 namespace에서 참조할 수 없습니다.

**MinIO 주소는 Console 주소가 아닌 S3 API 주소**여야 합니다.
기존에 ETL이 HTTPS 주소로 연결 중이면 그 주소를 그대로 사용합니다.

## 3. Docker 이미지 빌드 및 push

```bash
read -r -p "Docker Hub 사용자명: " DOCKER_USER
docker login
docker build -t "docker.io/${DOCKER_USER}/order-dashboard:2.1" .
docker push "docker.io/${DOCKER_USER}/order-dashboard:2.1"
```

Kubernetes 노드에서 내려받을 수 있는 이미지여야 합니다.
사설 저장소라면 기존 `imagePullSecrets`를 Deployment에 지정하세요.

## 4. Kubernetes 배포

앞 단계와 **같은 터미널**에서 실행합니다.
파일의 이미지 예시를 실제 빌드한 이미지 이름으로 바꾸어 적용합니다.

```bash
sed "s|docker.io/your-dockerhub-id/order-dashboard:2.1|docker.io/${DOCKER_USER}/order-dashboard:2.1|" order-dashboard.yaml | kubectl apply -f -
kubectl -n default rollout status deployment/order-dashboard --timeout=180s
kubectl -n default get pods -l app=order-dashboard
kubectl -n default get svc order-dashboard
kubectl -n default logs -f deployment/order-dashboard
```

브라우저 접속: **http://220.149.119.234:30111**

서버 방화벽/네트워크에서 TCP 30111 접속이 허용되어 있어야 합니다.
30111이 이미 다른 서비스에서 사용 중이면 새 서비스 생성이 실패하므로 기존 사용 여부를 확인하세요.

```bash
kubectl get svc -A -o wide
```

MinIO 데이터 갱신에 실패해도 웹 화면은 열리며 원인을 안내합니다.
한 번이라도 정상적으로 읽었다면 마지막 정상 그래프를 유지하고 재시도합니다.
최초에는 모든 파일을 읽으므로 데이터량에 따라 초기 로딩이 오래 걸릴 수 있습니다.

## TLS 및 시간대 설정

이번 실습 YAML은 이전 인증서 오류를 고려하여 `MINIO_VERIFY_SSL: "false"`를 명시했습니다.
HTTPS 연결의 암호화는 유지하면서 인증서 검증만 생략합니다.
Python 코드 자체의 기본값은 인증서 검증 활성화입니다.
신뢰할 수 있는 인증서 환경이면 `MINIO_VERIFY_SSL`을 `"true"`로 변경하세요.
자체 CA를 사용할 때는 CA 파일을 Pod에 마운트하고 `MINIO_CA_BUNDLE`에 경로를 지정합니다.

기존 ETL의 `order_time`에는 시간대 정보가 없으므로 `DB_TIMEZONE`으로 해석합니다.
기본값 `Asia/Seoul`은 MySQL 주문 시각이 한국 시간이라는 가정입니다.
MySQL에 실제 UTC 시각이 저장되어 있으면 `DB_TIMEZONE: "UTC"`로 변경하세요.
`DISPLAY_TIMEZONE`은 화면의 시각 표시를 지정하며 기본 한국 시간입니다.
현재 시간당 집계 경계는 UTC 정시이며 한국 정시와 일치합니다.

설정 변경 뒤 적용 및 재시작:

```bash
sed "s|docker.io/your-dockerhub-id/order-dashboard:2.1|docker.io/${DOCKER_USER}/order-dashboard:2.1|" order-dashboard.yaml | kubectl apply -f -
kubectl -n default rollout restart deployment/order-dashboard
```

## 파일 구성 및 검증

```text
order-dashboard/
  app.py                  MinIO 읽기·집계 API·내장 웹 화면
  static/index.html       대시보드 화면
  static/dashboard.js     ECharts 그래프·5초 화면 갱신
  static/style.css        반응형 화면 스타일
  static/echarts.min.js    Apache ECharts 6.0.0
  static/ECHARTS-*         오픈소스 라이선스·NOTICE
  requirements.txt        실행 의존성
  requirements-dev.txt    검증 의존성
  Dockerfile              Python 3.12 이미지
  order-dashboard.yaml    ConfigMap·Deployment·NodePort Service
  tests/test_dashboard.py 실제 Parquet 바이트를 이용한 테스트
```

검증 코드 실행(선택):

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

Parquet 실제 읽기, 중복/수정 주문, 증분 캐시, 파일 삭제, 실패 시 정상 스냅샷 유지,
5초·시간당 집계, 품목 고유 개수, Decimal 금액, 증감률, 누적값, 시간대, API를 검증합니다.
제작 시 집계/API 테스트 16개 통과, Chromium에서 네 버튼·고객/지표 선택·자동 갱신·모바일 화면과 이전 화면 자산, 차트 자산 누락, API 실패/복구, 버전 불일치를 검증했습니다.
이 패키지를 작성한 환경에서 테스트하며 실제 220.149.119.234 서버/MinIO에는 접속하거나 배포하지 않았습니다.

## 실습 범위

한 Pod·한 Uvicorn worker에서 파일 및 주문을 메모리에 보관합니다. 재시작하면 다시 읽습니다.
매 5초마다 전체 파일 목록을 조회하되 새 파일/변경 파일만 다운로드합니다.
전체 이력이 계속 늘어나면 메모리와 파일 목록 조회 비용도 증가합니다.
장기 운영용 대용량 분석으로 확장할 때는 파일 병합, 기간별 읽기, 분석 엔진 도입 등을 별도 단계로 진행하세요.

## 2.0 변경 내용 및 기존 배포 업데이트

- 상단 주문품목 그래프 제거. 주문량/주문액을 큰 두 그래프로 표시합니다.
- 가로축은 현재 시각부터 직전 5분입니다. 각 점은 5초 구간의 합계이며 시간축을 자동 이동합니다.
- 마지막 점은 현재 시각에 배치합니다. 현재 구간의 고객별 값은 선 끝과 그래프 아래에도 표시합니다.
- 과거 주문을 임의로 현재 시각으로 이동하거나 데이터를 생성하지 않습니다. 최근 5분에 주문이 없으면 0이며 최신 주문 시각과 원본 시간대를 안내합니다.
- 시간당 집계는 범주형 그룹 막대그래프입니다. 서로 멀리 떨어진 시간대를 직선으로 연결하지 않습니다.
- `app.py`에 화면 HTML/CSS/JavaScript를 내장했습니다. **기존 Dockerfile 폴더에서 app.py만 교체하여 다시 빌드해도 새 화면이 적용됩니다.** 기존 static/echarts.min.js와 폰트는 계속 사용합니다.
- 소스 원본은 static/index.html, static/dashboard.js, static/style.css에도 보관되어 있지만 실행 화면은 app.py 내장 자산을 우선 사용합니다.
- 실제 서버 시간대 설정은 바꾸지 않습니다. 이전에 `DB_TIMEZONE=UTC`를 적용했다면 아래 `set image` 방식으로 이미지만 바꾸어 설정을 유지하세요.

GPU 서버의 기존 Dockerfile 폴더에서 app.py를 교체한 뒤:

```bash
docker build -t yklee2002/order-dashboard:2.1 .
docker push yklee2002/order-dashboard:2.1
kubectl -n default set image deployment/order-dashboard dashboard=yklee2002/order-dashboard:2.1
kubectl -n default rollout status deployment/order-dashboard --timeout=180s
```

접속: http://220.149.119.234:30111 에서 강력 새로고침(Ctrl+F5).
Docker 이미지 이름은 `order-dashboard`입니다. 이전 빌드의 `order-dashbord` 철자와 혼동하지 마세요.
API의 `app_version`이 `2.1`이면 수정된 이미지가 실행 중입니다.

```powershell
$d = Invoke-RestMethod "http://220.149.119.234:30111/api/dashboard"
$d.app_version
$d.db_timezone
$d.latest_order
```


## 2.1: 연결 확인 중에서 멈추는 문제 수정

2.0 HTML에 이전 dashboard.js가 섞이면 삭제된 live-items 영역을 찾다 오류가 나서
API 조회가 시작되지 않습니다. 이전 CSS는 두 그래프를 세 열 공간에 배치합니다.
2.1은 CSS와 JavaScript를 HTML 안에 직접 제공하므로 이전 두 파일의 캐시를 사용하지 않습니다.
ECharts 파일에는 버전 쿼리를 붙이며 폰트 로딩을 기다리지 않고 API를 즉시 조회합니다.

초기화 오류, 차트 자산 누락, API 오류, 화면/API 버전 불일치를 화면에 표시합니다.
차트 파일이 누락되어도 API 조회와 KPI 표시는 계속합니다.
MinIO 접속 오류가 있으면 API status.error 내용을 화면에서 확인할 수 있습니다.
하단 v2.1과 API app_version=2.1을 확인하세요.

기존 프로젝트에서 app.py만 교체할 때도 기존 static/echarts.min.js는 있어야 합니다.
없는 경우 전체 ZIP의 static 폴더를 함께 사용하세요. ZIP의 Dockerfile은 static을 포함합니다.

GPU 서버에서, 새 app.py가 있는 기존 Dockerfile 폴더에서 실행합니다:

```bash
docker build -t yklee2002/order-dashboard:2.1 .
docker push yklee2002/order-dashboard:2.1
kubectl -n default set image deployment/order-dashboard dashboard=yklee2002/order-dashboard:2.1
kubectl -n default rollout status deployment/order-dashboard --timeout=180s
```

배포 후 브라우저를 새로고침합니다. Windows PowerShell에서 실행 버전을 확인합니다:

```powershell
$d = Invoke-RestMethod "http://220.149.119.234:30111/api/dashboard"
$d.app_version
$d.status.error
```

버전이 2.1인데 오류가 계속되면 화면의 오류 문구와 위 status.error를 확인하세요.
Pod 로그는 다음 명령으로 확인합니다:

```bash
kubectl -n default logs deployment/order-dashboard --tail=80
```
