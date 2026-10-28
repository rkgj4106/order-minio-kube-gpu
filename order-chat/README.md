# MinIO 주문 Parquet + 설치된 Gemma로 주문 데이터에 질문하기

대상: **220.149.119.234 GPU 서버의 Kubernetes**.
기존 주문 대시보드 30111과 함께 사용하는 별도 상담 화면: **NodePort 30112**.

## 처리 방식

1. Python이 MinIO 파일 목록을 5초마다 조회하고 새 파일·변경 파일을 읽습니다.
2. order_id별 가장 최신 etl_extracted_at 한 건을 메모리에 보관합니다.
3. 질문을 현재 설치된 Ollama/Gemma에 보내 JSON 조회 계획을 받습니다.
4. 계획의 필드·기간·허용 집계 종류를 검증하고 Python이 실제 주문을 집계합니다.
5. Gemma는 집계 결과를 근거로 한국어 답변을 작성합니다.
6. 화면에 답변, 실제 계산표 [R1…], 조회 기간·조건·기준 시각을 함께 표시합니다.

Gemma가 SQL이나 Python 코드를 생성해 실행하지 않습니다. 모든 집계는 허용된 Python 함수입니다.
전체 주문을 프롬프트에 넣지 않고 최대 20행의 집계 근거와 전체 합계를 전달합니다.
금액은 Decimal로 합산합니다. 답변 설명의 정확성은 모델에 따라 달라질 수 있으므로 화면의
조회 조건과 실제 집계표가 수치 확인의 기준입니다. 실제 설치된 Gemma에 연결한 뒤 예시 질문의
조건 해석과 수치를 확인하세요. 제작 환경에서는 Ollama 요청/응답을 모의하여 검증했습니다.

## 질문 예시

- 오늘 C001의 총 주문량과 주문액은?
- 오늘 주문액이 가장 큰 고객 5명은?
- 오늘 제품별 총 주문량을 알려줘.
- 최근 1시간 C004 주문량을 5분 단위로 보여줘.
- 최근 1시간 주문량을 직전 1시간과 비교해줘.
- 어제 고객별 주문액 순위를 알려줘.
- 오늘 최근 주문 10건은?
- 2026-10-09 15:00부터 16:00 전까지 C001 주문액은?

집계 종류: 합계 / 고객·제품 순위 / 시간별 변화 / 직전 동일 길이 기간 비교 / 최근 주문 내역.
지표: 수량 / 주문액 / 주문건수. 고객·제품 필터를 함께 지정할 수 있습니다.
기간: 오늘 / 어제 / 최근 5분·1시간·24시간 / 전체 / 사용자 지정.
기간을 지정하지 않으면 오늘의 주문입니다. 모든 시각은 한국 시각이며 사용자 지정 종료 경계는 제외합니다.
진행 중인 오늘과 최근 기간은 현재 시각까지 포함합니다.
오늘을 비교할 때는 오늘 현재까지의 길이와 바로 앞 동일 길이 기간을 비교합니다.
‘어제 전체 대 오늘’처럼 별도의 길이가 다른 두 기간 비교는 이 버전에서 지원하지 않습니다.
질문의 원인·고객 주소·이익·재고·미래 예측 등 데이터로 확인할 수 없는 내용은 답할 수 없다고 안내합니다.
표시한 행이 일부이면 화면에 표시합니다. 시간별 분석은 마지막 구간의 행을 표시하며
장기간에는 최대 약 500개 시간 구간으로 집계 간격을 자동 확대합니다.
첫 시간 구간의 증감은 null, 직전 값이 0이면 증감률은 null입니다.

제품 이름 P001 Laptop / P002 Monitor / P003 Keyboard / P004 Mouse / P005 Server는
사용자가 제시한 시뮬레이터 제품 목록입니다. 제품 목록을 바꾸면 app.py의 PRODUCTS도 수정하세요.
고객 이름 대신 실제 customer_id를 사용합니다. 최근 대화 6개 메시지를 질문 해석에 활용하며
대화 기록은 현재 브라우저에만 보관합니다. 서버의 공용 대화 메모리는 사용하지 않습니다.

## 1. 설치된 Ollama Service와 모델 확인

아래 명령은 **GPU 서버의 Ubuntu 터미널**에서 실행합니다.

```bash
kubectl get svc -A -o wide
kubectl -n default exec deployment/ollama -- ollama list
```

두 번째 명령은 기존 Deployment가 default/ollama인 경우입니다.
실제 namespace 또는 Deployment 이름이 다르면 해당 이름으로 바꾸세요.
아래 주소와 모델 태그는 예시입니다. 현재 클러스터의 실제 값은 확인되지 않았습니다.

order-chat.yaml에서 확인할 값:

```yaml
OLLAMA_URL: "http://ollama.default.svc.cluster.local:11434"
OLLAMA_MODEL: "gemma3:12b"
```

다른 namespace라면 `http://<Service>.<namespace>.svc.cluster.local:11434`로 바꾸세요.
OLLAMA_MODEL은 `ollama list`에 표시되는 **정확한 태그**와 일치해야 합니다.
코드는 설치된 모델을 재사용하며 새 모델을 다운로드하거나 새 GPU Pod를 만들지 않습니다.
GPU는 기존 Ollama Pod가 사용합니다. order-chat Pod에는 GPU 리소스 요청이 없습니다.

## 2. 기존 ETL 설정 재사용

```bash
kubectl -n default get configmap order-etl-config
kubectl -n default get secret order-etl-secret
```

ConfigMap 키: MINIO_ENDPOINT / MINIO_BUCKET / MINIO_PREFIX.
Secret 키: MINIO_ACCESS_KEY / MINIO_SECRET_KEY.
기존에 정상 연결 중인 S3 API 주소·버킷·prefix·인증정보를 사용합니다.
기본 prefix는 raw/orders_step1/입니다. checkpoint JSON은 읽지 않습니다.
ETL이 다른 namespace에 있다면 order-chat.yaml의 모든 namespace와 명령을 그곳으로 바꾸세요.

주문 시각 해석과 표시 시각은 order_data.py에서 Asia/Seoul로 고정했습니다.
앞서 수정한 시뮬레이터가 한국 시각을 저장하는 것을 전제로 합니다.
과거 UTC 데이터는 사용자의 요청에 따라 변환하지 않으며 한국 시각으로 해석됩니다.
기존 실습의 자체서명 MinIO 인증서 환경을 고려해 YAML은 MINIO_VERIFY_SSL=false입니다.
신뢰 가능한 인증서 또는 CA 환경이면 true 및 MINIO_CA_BUNDLE을 적용하세요.

## 3. 파일 준비와 Docker 빌드

다운로드한 order-chat.zip을 GPU 서버로 복사하고 압축을 풉니다.

```bash
unzip order-chat.zip
cd order-chat
docker build -t yklee2002/order-chat:1.0 .
docker push yklee2002/order-chat:1.0
```

Docker Hub 계정 yklee2002는 배포 예시입니다.
실제 계정에 맞게 빌드/push 명령과 YAML의 image를 동일하게 바꾸세요.
푸시 전에 기존 로그인 상태를 사용하거나 docker login을 실행합니다.

## 4. Kubernetes 배포

order-chat.yaml의 Ollama 주소·모델 태그가 실제 설치와 일치하는지 확인한 뒤:

```bash
kubectl apply -f order-chat.yaml
kubectl -n default rollout status deployment/order-chat --timeout=180s
kubectl -n default get pods -l app=order-chat
kubectl -n default logs deployment/order-chat --tail=80
```

접속: **http://220.149.119.234:30112**.
30112가 기존 Service에 사용 중이면 YAML의 nodePort를 사용 가능한 포트로 바꾸세요.
화면이 열리면 Parquet 연결 및 Gemma 연결 상태와 실제 모델 목록을 확인합니다.
웹 앱 자체는 MinIO/Ollama 오류가 있어도 열려 연결 문제를 표시합니다.

ConfigMap 수정 후:

```bash
kubectl -n default rollout restart deployment/order-chat
kubectl -n default rollout status deployment/order-chat
```

Windows PowerShell에서 확인:

```powershell
$s = Invoke-RestMethod "http://220.149.119.234:30112/api/status"
$s.model
$s.models
$s.ollama_error
$s.data
```

질문 API:

```powershell
$body = @{question="오늘 제품별 총 주문량을 알려줘"} | ConvertTo-Json
$r = Invoke-RestMethod -Method Post -Uri "http://220.149.119.234:30112/api/chat" -ContentType "application/json; charset=utf-8" -Body ([System.Text.Encoding]::UTF8.GetBytes($body))
$r.answer
$r.evidence.rows | Format-Table
```

## 오류 처리 및 실습 범위

- Parquet 최초 읽기 실패: 503과 원인을 안내합니다.
- 이후 MinIO 오류: 마지막 정상 데이터를 사용하고 화면에 갱신 지연을 표시합니다.
- Ollama 주소 오류·없는 모델·시간 초과: 오류를 표시하며 주문 답변으로 가장하지 않습니다.
- 질문 계획 검증 실패: 한 번 교정 요청 후, 실패하면 구체적인 질문을 안내합니다.
- 해당 주문 없음: 임의로 답변을 만들지 않고 빈 조회 결과를 안내합니다.
- 한 번에 질문 하나를 처리합니다. 동시에 들어오는 다음 질문은 429로 재시도를 안내합니다.

실습용 단일 Pod/worker가 전체 최신 주문을 메모리에 보관합니다.
데이터가 커지면 기간별 읽기, 파일 병합, 별도 분석 엔진 등이 다음 확장 단계입니다.
원래 ETL이 삭제 이벤트를 저장하지 않으므로 원천 DB 삭제는 이 앱도 감지하지 못합니다.
애플리케이션은 MinIO/DB에 주문을 쓰거나 수정하지 않습니다.
이 화면은 기존 실습처럼 별도 사용자 인증이 없는 NodePort 앱입니다.
본 패키지는 실제 서버에 접속하거나 배포한 상태가 아닙니다.

## 파일

- app.py: Ollama 연동, 조회 계획 검증, 허용된 주문 집계, API.
- order_data.py: 기존 대시보드 2.4에서 재사용한 MinIO Parquet 캐시.
- index.html: 질문·답변·근거표 웹 화면. 외부 CDN 호출 없음.
- static/fonts: Noto Sans KR 웹 폰트 및 OFL 라이선스.
- requirements.txt / requirements-dev.txt: 실행·검증 의존성.
- Dockerfile / order-chat.yaml: Docker 및 Kubernetes 배포.
- tests/test_chat.py: 집계·JSON 계획·API·오류·동시 요청 검증.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

제작 시 집계·API 테스트 24개와 데스크톱·모바일 브라우저 검증을 통과했습니다.
브라우저 검증은 샘플 Parquet과 모의 Ollama 응답으로 진행했습니다.
실제 GPU 서버와 설치된 Gemma를 통한 질의는 배포 후 확인해야 합니다.

Ollama API 공식 문서:
https://docs.ollama.com/api/chat
https://docs.ollama.com/capabilities/structured-outputs
https://docs.ollama.com/api/tags
