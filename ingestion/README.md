# Bronze 적재 - 서울시 공공자전거 대여이력 (OA-15182)

메인 데이터셋의 Bronze 계층 적재 파이프라인. **Backfill(1회)** 과 **일 배치(증분)**
두 개의 잡으로 구성되며, 로컬(LocalStack)에서 개발한 뒤 AWS로 그대로 옮길 수 있도록
설계했다.

## 핵심 설계 결정

| 항목 | 결정 | 이유 |
|---|---|---|
| 로컬↔AWS 전환 | 환경변수만 교체, 코드 수정 없음 | `.env.example` 참고 |
| Iceberg 카탈로그 | local=Hadoop / aws=Glue | Glue는 LocalStack 무료 버전에서 에뮬레이션 안 됨 |
| 멱등성 | `overwritePartitions()` | 재실행 시 같은 날짜 파티션만 덮어씀 (중복/누락 방지) |
| 스키마 대응 | 컬럼 "이름" 기준 select+cast | 2025년 17컬럼 vs 2026년 16컬럼 통보없는 변경 대응 |
| Bronze 원칙 | 전부 STRING, 캐스팅 안 함 | "원본 그대로" 보존. 타입 캐스팅은 Silver 책임 |
| 인코딩 | EUC-KR→UTF-8 사전변환, 손상바이트는 폐기 후 계속 진행 | Spark 4.x charset 제약 + 실제 손상 바이트 존재 확인됨 |
| 워터마크 | 날짜 단위로 하루씩 전진, 성공한 날만 커밋 | 부분 실패 시 데이터 누락 방지 |

## 로컬 개발 환경 준비

### 1. LocalStack 실행

```bash
docker compose -f docker-compose.localstack.yml up -d
```

### 2. 의존성 설치

```bash
pip install -r requirements.txt --break-system-packages   # 또는 venv 사용 권장
```

### 3. 환경변수 설정

```bash
cp .env.example .env
# .env 값 확인 (기본값이 LocalStack 기준으로 이미 맞춰져 있음)
export $(grep -v '^#' .env | xargs)
```

### 4. Backfill 실행

열린데이터광장에서 반기/월별로 다운로드한 원본 CSV(.zip 포함)를 한 디렉토리에 모은다.

```bash
# data/ 에 다운로드한 파일들 배치
INPUT_DIR=../data python -m jobs.backfill_rental_history
```

재실행해도 안전하다 — 같은 날짜 파티션은 덮어써지고 다른 파티션은 그대로 유지된다.

### 5. 일 배치 실행

```bash
python -m jobs.daily_batch_rental_history
```

워터마크가 없으면 `BACKFILL_START_DATE`부터, 있으면 그 다음날부터 어제까지 순차 처리한다.
Airflow에 태울 때는 이 스크립트를 하나의 Task로 등록하면 된다 (내부에서 날짜별로 반복 처리).

## 테스트

Spark/Iceberg 세션이 필요 없는 순수 로직(인코딩 변환, 스키마 검증, 워터마크, API
페이징/재시도)은 실제로 테스트가 통과하는 상태다.

```bash
pytest tests/ -v
```

컬럼 매핑(`build_select_exprs`)이나 날짜 파티션 정규화처럼 Spark DataFrame이
필요한 로직은 로컬 `local[*]` 모드로 직접 검증했다(별도 pytest에는 포함 안 함,
Iceberg 확장 없이 순수 Spark로도 검증 가능하기 때문).

## AWS 배포 시 바꿔야 하는 것

`.env`에서 아래 값만 교체하면 코드는 그대로 동작한다.

```bash
APP_ENV=aws
ICEBERG_CATALOG_TYPE=glue
ICEBERG_WAREHOUSE_PATH=s3://ttareungyi-warehouse-prod/warehouse
# AWS_ACCESS_KEY_ID / SECRET은 EC2 IAM Role 사용 시 생략
```

그 외에 실제로 확인이 필요한 것들:

- **EMR Step으로 실행 시** `spark.jars.packages`가 Maven Central에 접근해야 하므로,
  EMR 클러스터가 인터넷 아웃바운드(또는 VPC endpoint/NAT)를 가지고 있는지 확인
- **CloudWatch 커스텀 메트릭** 연동은 로컬에서 테스트 불가 — AWS에서 별도 확인
- **PyDeequ 대용량 성능**은 로컬 리소스로는 의미 있는 측정이 안 됨 — EMR에서 실측

## 반드시 확인해야 할 TODO

`common/api_client.py`의 API 호출 URL 패턴과 응답 JSON의 root key 이름은
**서울 열린데이터광장 데이터셋 상세 페이지의 "Open API" 탭에서 실제 값으로 재확인**
해야 한다. 지금 코드는 일반적인 URL 패턴(`/{인증키}/json/{서비스명}/{시작}/{종료}/{조건}`)과
알려진 응답 코드(`INFO-000`, `ERROR-300`, `ERROR-336`)만 신뢰하고, root key는
`RESULT`/`row`를 가진 노드를 방어적으로 탐색하도록 만들어 놨다 — 실제 응답을
한 번 받아본 뒤 `.env`의 `SEOUL_OPENDATA_SERVICE_NAME` 등을 맞추면 된다.

## 다음 단계 (Silver 계층에서 할 일)

Bronze는 "원본 그대로"만 책임진다. 아래는 이 파이프라인이 넘기지 않는 것들이며
Silver 계층(PyDeequ 품질 검증 + 타입 캐스팅 + 조인)에서 처리해야 한다:

- `use_min`, `use_distance_m` 등을 실제 숫자형으로 캐스팅
- `sex_cd` 대소문자 정규화(M/m), 24% 결측 처리
- `birth_year` 더미값(1901, 2098 등) 필터링/플래그
- 대여소 정보(OA-13252)와의 조인
