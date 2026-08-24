# Bronze 적재 파이프라인

서울시 공공자전거 원천 데이터를 Bronze 계층으로 적재하는 ingestion 프로젝트다.
파일 기반 **Backfill(1회성)** 과 API 기반 **일 배치**를 모두 지원한다.

대상 데이터셋:

| 데이터셋 | 원천 | Bronze 테이블 | 적재 방식 |
|---|---|---|---|
| 대여이력 | OA-15182 / `tbCycleRentData` | `bronze.rental_history` | 파일 백필 + API 증분 |
| 고장신고 | OA-15644 / `tbCycleFailureReport` | `bronze.failure_report` | 파일 백필 + API 증분 |
| 대여소정보 | OA-13252 / `tbCycleStationInfo` | `bronze.station_master` | API 스냅샷 (파일 백필 없음) |
| 실시간 대여정보 | OA-15493 / `bikeList` | `bronze.station_active` | API 스냅샷 (파일 백필 없음) |

## 핵심 설계 결정

| 항목 | 결정 | 이유 |
|---|---|---|
| 로컬 저장소 | LocalStack S3 + Iceberg Hadoop catalog | AWS 없이 S3/Iceberg 적재 흐름 검증 |
| AWS 전환 | 환경변수 교체 중심 | 코드 수정 없이 Glue/S3로 이전 가능 |
| Bronze 원칙 | 원본 필드는 전부 STRING | 타입 캐스팅/품질 규칙은 Silver 책임 |
| 멱등성 | `overwritePartitions()` | 재실행 시 같은 날짜/스냅샷 파티션만 덮어씀 |
| 워터마크 | 대여이력/고장신고만 사용 | 대여소정보 API는 날짜 파라미터 없이 전체 스냅샷만 반환 |
| 대여소정보 파티션 | `snapshot_date` | 이벤트 발생일이 아니라 스냅샷 기준일 |
| 실시간 대여정보 파티션 | `snapshot_date` | 대여소정보와 동일한 이유 - 워터마크 대상 아님 |

## 로컬 Airflow 실행

Spark/PySpark ingestion 잡을 Airflow 컨테이너 안에서 실행하려면 루트의 로컬 전용 compose 파일을 사용한다.

기본 `docker-compose.yml`과 `airflow/Dockerfile`은 건드리지 않는다. Spark 실행에 필요한 Java와
ingestion 의존성은 아래 파일에만 들어있다.

- `../docker-compose.local.yml`
- `../airflow/Dockerfile.local`

### 1. 환경변수 확인

루트 `.env` 하나가 유일한 소스다 (Airflow/Postgres/LocalStack 컨테이너 설정 +
ingestion/staging/pipeline 잡이 읽는 값 전부 포함).

Airflow DAG의 bash command는 컨테이너 안에서 `/opt/airflow/ingestion/.env`를
`source` 하는데, `ingestion/.env`는 실제 파일이 아니라 **루트 `.env`를 가리키는
심볼릭 링크**다. 두 파일에 값을 따로 넣었다가 서로 어긋나서 실 AWS 전환 때 한참
디버깅한 적이 있어서(#83) 이렇게 통합했다 - `ingestion/.env`에 절대 값을 직접
쓰지 말 것.

처음 클론했다면 심볼릭 링크가 없으니 한 번 만들어준다:

```bash
cd ingestion && ln -s ../.env .env && cd ..
```

`docker-compose.local.yml`/`docker-compose.yml`이 루트 `.env`를
`/opt/airflow/.env`로 바인드 마운트하므로, 컨테이너 안에서 심볼릭 링크가
정상적으로 풀린다.

필수 확인값 (루트 `.env`):

```bash
# .env
APP_ENV=local
S3_ENDPOINT=http://localstack:4566
RAW_BUCKET=ttareungyi-raw
WAREHOUSE_BUCKET=ttareungyi-warehouse
ICEBERG_CATALOG_TYPE=hadoop
ICEBERG_CATALOG_NAME=bike_catalog
ICEBERG_WAREHOUSE_PATH=s3a://ttareungyi-warehouse/warehouse
SEOUL_API_KEY=<서울 열린데이터광장 API 키>
SLACK_WEBHOOK_URL=<Slack 인커밍 웹훅 URL, 선택 - 비워두면 Airflow 태스크 실패 알림을 skip>
```

`SLACK_WEBHOOK_URL`은 Airflow 워커/스케줄러 프로세스 자체의 OS 환경변수라 `docker-compose*.yml`의
`x-airflow-common.environment`를 통해 컨테이너로 전달된다 (`airflow/dags/dag_common.py`의
`notify_slack_on_failure` 참고) - `infra/terraform`의 `notify_slack` Lambda가 읽는 동명의
Lambda 환경변수와는 완전히 별개의 값이다.

### 2. 컨테이너 실행

프로젝트 루트에서 실행한다.

```bash
docker compose -f docker-compose.local.yml up --build
```

백그라운드 실행:

```bash
docker compose -f docker-compose.local.yml up --build -d
```

Airflow UI:

```text
http://localhost:8080
```

로그인 계정은 루트 `.env`의 `_AIRFLOW_WWW_USER_USERNAME`,
`_AIRFLOW_WWW_USER_PASSWORD` 값을 사용한다.

### 3. 컨테이너 상태 확인

```bash
docker compose -f docker-compose.local.yml ps
docker compose -f docker-compose.local.yml logs -f airflow-scheduler
```

compose 파일 문법만 확인하고 싶으면:

```bash
docker compose -f docker-compose.local.yml config --quiet
```

## Airflow DAG 실행 방법

Airflow UI에서 DAG를 unpause한 뒤 실행한다.

### 0. Iceberg 테이블 Bootstrap (신규 환경에서 최초 1회 필수)

DAG ID:

```text
bootstrap_iceberg_tables
```

실행 시점: **인프라 배포 직후, Bronze 초기 적재/일 배치를 실행하기 전**. 신규
LocalStack/AWS 환경은 Iceberg warehouse와 JDBC 카탈로그가 완전히 비어 있어, 이 DAG를
먼저 실행하지 않으면 아래 Bronze 초기 적재/일 배치가 `load_table()` 단계에서
"테이블 없음"으로 실패한다. 특히 `bronze.station_active`는 별도 Initial Load
경로가 없어(Daily Batch만 있음) 이 DAG가 유일한 테이블 생성 경로다.

파라미터 입력 없이 `Trigger DAG`만 실행하면 된다. 태스크는 멱등하게 동작해 반복
실행해도 중복 테이블/데이터가 생기지 않고, 기존 데이터를 훼손하지 않는다:

| 태스크 | 잡 | 역할 |
|---|---|---|
| `create_bronze_tables` | `jobs/bootstrap_iceberg_tables.py` | 필수 Bronze 테이블(`rental_history`, `failure_report`, `station_master`, `station_active`, `bikeman_event`, `bikeman_event_quarantine`)이 없으면 새로 생성 |

새로운 환경에서는 `create_bronze_tables`가 필요한 테이블을 전부 새로 만든다.

### Bronze 초기 적재

DAG ID:

```text
bronze_initial_load_all_sources
```

실행 용도:

- 다운로드해 둔 파일 데이터를 Bronze로 최초 적재
- 대여이력과 고장신고를 병렬로 적재 (대여소정보는 파일 백필 대상이 아니라 이 DAG에 없음)

기본 입력 경로:

| 파라미터 | 기본값 |
|---|---|
| `rental_history_dir` | `/opt/airflow/ingestion/data/rental_history` |
| `rental_history_pattern` | `*` |
| `rental_history_watermark_date` | `2026-06-30` |
| `failure_report_dir` | `/opt/airflow/ingestion/data/failure_report` |
| `failure_report_pattern` | `*` |
| `failure_report_watermark_date` | `2026-06-30` |

실행 전 파일 배치 예시:

```text
ingestion/data/rental_history/
ingestion/data/failure_report/
```

Airflow UI에서 `Trigger DAG w/ config`를 사용할 경우 예시:

```json
{
  "rental_history_pattern": "*2601*",
  "rental_history_watermark_date": "2026-01-31",
  "failure_report_pattern": "*"
}
```

### Bronze 공백 자동 복구

DAG ID:

```text
bronze_historical_reconciliation
```

매일 00:30에 `rental_history`와 `failure_report`의 Bronze 워터마크 다음 날짜부터
D-2까지 공백을 확인하고 날짜별 Dynamic Task Mapping으로 복구한다.

```text
check_*_gap
  → catchup_failure_report_date (날짜별 mapped task)
  → prepare_rental_history_date (날짜별 mapped task: 수집+선택)
      → promote_rental_history_date (날짜별 mapped task: 승격+completion marker)
  → completion 확인
  → 원천별 Bronze 워터마크 전진
```

대여이력은 `SEOUL_API_KEY1~3`, 고장신고는 `SEOUL_API_KEY4`를 사용하며 `seoul_api`
Pool은 전체 4개 Task까지만 허용한다. 대여이력은 prepare/promote 두 단계로 나뉜다 -
prepare(API 수집 + Raw snapshot 선택)는 `seoul_api` Pool에서 날짜별로 최대 3개까지
병렬 실행되지만, promote(Bronze 승격 + completion marker)는 같은 `bronze.rental_history`
Iceberg 테이블에 동시에 commit하면 충돌하므로 전용 `bronze_rental_history_commit`
Pool(slot=1)에서 날짜 순서 상관없이 1개씩만 실행된다. promote는 prepare 성공 여부와
무관하게 항상 실행되어(all_done) 실패한 날짜도 completion marker에 FAILED로 남기고,
날짜 Task는 워터마크를 변경하지 않고 결과만 completion marker에 기록한다.

```text
_meta/completion/bronze_rental_history/target_date=YYYY-MM-DD/completion.json
_meta/completion/bronze_failure_report/target_date=YYYY-MM-DD/completion.json
```

`COMPLETE_EMPTY` 또는 실패 결과도 marker로 남기며, 연속 구간이 끊기면 해당 원천의
워터마크는 전진하지 않는다. 대여이력의 0행은 기존 수동 확인 규칙을 따른다.

### 워터마크 수동 설정

DAG ID:

```text
set_watermark
```

실행 용도:

- 파일 백필 완료 후 API 일 배치가 백필 다음 날짜부터 이어서 돌도록 워터마크를 찍는다.
- `station_master`는 워터마크 대상이 아니다. 매일 전체 스냅샷을 적재한다.

`Trigger DAG w/ config` 예시:

```json
{
  "dataset": "rental_history",
  "watermark_date": "2026-06-30"
}
```

고장신고도 필요하면 dataset만 바꿔 한 번 더 실행한다.

```json
{
  "dataset": "failure_report",
  "watermark_date": "2026-06-30"
}
```

Silver/Gold(대여이력 전용, `silver_gold_daily_batch_rental_history` DAG)도 같은 방식으로 찍는다.
Bronze 워터마크보다 뒤 날짜를 찍으면 그 사이 날짜는 영영 처리되지 않으니 주의한다.

```json
{
  "dataset": "silver_rental_history",
  "watermark_date": "2026-06-30"
}
```

```json
{
  "dataset": "gold_dim_bike",
  "watermark_date": "2026-06-30"
}
```

### Bronze 일 배치

DAG ID:

```text
bronze_daily_batch_all_sources
```

스케줄:

```text
매일 06:00 KST
```

실행 용도:

- 대여소정보: 실행일 기준 전체 스냅샷 적재
- 대여이력: 워터마크 다음날부터 어제까지 API 증분 적재
- 고장신고: 워터마크 다음날부터 어제까지 API 증분 적재

로컬 검증에서 한 번에 처리할 날짜 수를 제한하고 싶으면 `max_days_per_run`을 지정한다.

```json
{
  "max_days_per_run": "1"
}
```

빈 문자열이면 잡 내부 워터마크 로직이 처리 가능한 날짜를 순차 처리한다.

## 로컬에서 잡 직접 실행

Airflow 없이 ingestion 잡만 직접 실행할 수도 있다.

```bash
cd ingestion
export $(grep -v '^#' .env | xargs)
export PYTHONPATH=..:$PYTHONPATH  # ingestion/staging/pipeline이 공유하는 최상위 config/ 패키지를 찾기 위함
```

Bootstrap (신규 환경에서 아래 초기 적재/일 배치보다 먼저 1회 실행):

```bash
python -m jobs.bootstrap_iceberg_tables
```

초기 적재 (각 디렉터리에 파일이 없으면 열린데이터광장에서 자동으로 받는다. 대여소정보는
파일 백필이 없으므로 아래 "일 배치"의 `daily_batch_station_master`로 적재한다):

```bash
INPUT_DIR=./data/rental_history python -m jobs.initial_load_rental_history
INPUT_DIR=./data/failure_report python -m jobs.initial_load_failure_report
```

일 배치:

```bash
python -m jobs.daily_batch_rental_history
python -m jobs.daily_batch_failure_report
python -m jobs.daily_batch_station_master
python -m jobs.daily_batch_station_active
```

워터마크 수동 설정:

```bash
WATERMARK_DATE=2026-06-30 DATASET=rental_history python -m jobs.set_watermark
WATERMARK_DATE=2026-06-30 DATASET=failure_report python -m jobs.set_watermark
WATERMARK_DATE=2026-06-30 DATASET=silver_rental_history python -m jobs.set_watermark
WATERMARK_DATE=2026-06-30 DATASET=gold_dim_bike python -m jobs.set_watermark
```

대여소정보는 과거 스냅샷을 소급 조회할 수 없다. 재처리가 필요하면 오늘 API 응답에
과거 날짜를 찍는다는 점을 인지하고 `SNAPSHOT_DATE`를 직접 지정한다.

```bash
SNAPSHOT_DATE=2026-06-30 python -m jobs.daily_batch_station_master
```

## 테스트

```bash
cd ingestion
export PYTHONPATH=..:$PYTHONPATH  # 최상위 config/ 패키지를 찾기 위함
pytest tests/ -v
```

특정 테스트만 실행:

```bash
pytest tests/test_station_master_schema.py -v
```

## AWS 배포 시 바꿔야 하는 것

루트 `.env`에서 아래 값들을 AWS 기준으로 교체한다 (`ingestion/.env`는 심볼릭
링크라 따로 건드릴 필요 없음).

```bash
APP_ENV=aws
ICEBERG_CATALOG_TYPE=glue
ICEBERG_WAREHOUSE_PATH=s3://ttareungyi-warehouse-prod/warehouse
```

EC2/EMR에서 IAM Role을 사용하면 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`는 생략할 수 있다.

## Silver 계층에서 할 일

Bronze는 원본 보존과 적재 안정성만 책임진다. 아래 작업은 Silver에서 처리한다.

- 숫자/날짜 타입 캐스팅
- 성별/연령/결측/이상값 정규화
- 대여소정보와 대여이력/고장신고 조인
- 대여소정보 스냅샷 기반 SCD Type 2 이력화
