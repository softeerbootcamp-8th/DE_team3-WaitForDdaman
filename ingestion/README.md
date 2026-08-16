# Bronze 적재 파이프라인

서울시 공공자전거 원천 데이터를 Bronze 계층으로 적재하는 ingestion 프로젝트다.
파일 기반 **Backfill(1회성)** 과 API 기반 **일 배치**를 모두 지원한다.

대상 데이터셋:

| 데이터셋 | 원천 | Bronze 테이블 | 적재 방식 |
|---|---|---|---|
| 대여이력 | OA-15182 / `tbCycleRentData` | `bronze.rental_history` | 파일 백필 + API 증분 |
| 고장신고 | OA-15644 / `tbCycleFailureReport` | `bronze.failure_report` | 파일 백필 + API 증분 |
| 대여소정보 | OA-13252 / `tbCycleStationInfo` | `bronze.station_master` | 파일 백필 + API 스냅샷 |
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

루트 `.env`는 Airflow/Postgres/LocalStack 컨테이너 설정에 사용된다.

`ingestion/.env`는 DAG가 실행하는 ingestion 잡에서 읽는다. Airflow DAG의 bash command가
컨테이너 안에서 `/opt/airflow/ingestion/.env`를 `source` 하므로, 로컬에서는 기본값이
LocalStack 기준인지 확인한다.

필수 확인값:

```bash
# ingestion/.env
APP_ENV=local
S3_ENDPOINT=http://localstack:4566
RAW_BUCKET=ttareungyi-raw
WAREHOUSE_BUCKET=ttareungyi-warehouse
ICEBERG_CATALOG_TYPE=hadoop
ICEBERG_CATALOG_NAME=bike_catalog
ICEBERG_WAREHOUSE_PATH=s3a://ttareungyi-warehouse/warehouse
SEOUL_API_KEY=<서울 열린데이터광장 API 키>
```

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

### Bronze 백필

DAG ID:

```text
bronze_backfill_all_sources
```

실행 용도:

- 다운로드해 둔 파일 데이터를 Bronze로 최초 적재
- 대여소정보를 먼저 적재한 뒤, 대여이력과 고장신고를 병렬로 적재

기본 입력 경로:

| 파라미터 | 기본값 |
|---|---|
| `station_master_dir` | `/opt/airflow/ingestion/data/station_master` |
| `station_master_pattern` | `*` |
| `rental_history_dir` | `/opt/airflow/ingestion/data/rental_history` |
| `rental_history_pattern` | `*` |
| `failure_report_dir` | `/opt/airflow/ingestion/data/failure_report` |
| `failure_report_pattern` | `*` |

실행 전 파일 배치 예시:

```text
ingestion/data/station_master/
ingestion/data/rental_history/
ingestion/data/failure_report/
```

Airflow UI에서 `Trigger DAG w/ config`를 사용할 경우 예시:

```json
{
  "station_master_pattern": "*.xlsx",
  "rental_history_pattern": "*2601*",
  "failure_report_pattern": "*"
}
```

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

백필:

```bash
INPUT_DIR=./data/rental_history python -m jobs.backfill_rental_history
INPUT_DIR=./data/failure_report python -m jobs.backfill_failure_report
INPUT_DIR=./data/station_master python -m jobs.backfill_station_master
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

대여소정보 파일명이 기준일을 포함하지 않으면 `SNAPSHOT_DATE`를 직접 지정한다.

```bash
INPUT_DIR=./data/station_master SNAPSHOT_DATE=2026-06-30 python -m jobs.backfill_station_master
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

`ingestion/.env`에서 아래 값들을 AWS 기준으로 교체한다.

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
