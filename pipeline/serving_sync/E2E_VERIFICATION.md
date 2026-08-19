# serving_sync E2E 검증 (2026-07-01)

`pipeline/serving_sync`가 Gold Iceberg 마트를 서빙 Postgres(`serving` 스키마)로 정상
동기화하는지 실제 인프라(Airflow 컨테이너 + Iceberg/S3(LocalStack) + Postgres)에서
검증한 기록.

## 배경

`SNAPSHOT_DATE=2026-07-01`로 검증하기로 했으나, 이 파이프라인이 읽는 upstream Gold
테이블(`fact_bike_risk` / `fact_bike_decision` / `station_active` /
`fact_station_inventory` 등)에는 2026-06-24 파티션만 있고 2026-07-01 파티션이 없었다.
그래서 검증 전에 upstream Gold 파티션부터 2026-07-01 기준으로 먼저 채웠다.

## 테스트 방법

### 0) 사전 확인

```bash
docker exec airflow-scheduler bash -c '
cd /opt/airflow/pipeline/serving_sync && set -a && source /opt/airflow/ingestion/.env && set +a
PYTHONPATH=/opt/airflow/ingestion:/opt/airflow/pipeline/serving_sync/jobs:$PYTHONPATH python -c "
from common.spark_session import build_spark_session
spark = build_spark_session(\"check\")
for t in [\"gold.fact_bike_risk\",\"gold.fact_bike_decision\",\"gold.station_active\",\"gold.fact_station_inventory\"]:
    print(t, spark.table(f\"bike_catalog.{t}\").select(\"snapshot_date\").distinct().collect())
"'
```

### 1) upstream Gold 파티션 채우기 (2026-07-01)

`dag_gold_dim_fact`가 만드는 것 중 이 파이프라인이 실제로 쓰는 3개만 필요
(`dim_bike`는 mart 입력에 없어서 제외):

```bash
cd pipeline/collection_priority
export PYTHONPATH=../../ingestion:$PYTHONPATH
set -a && source ../../ingestion/.env && set +a
SNAPSHOT_DATE=2026-07-01 python -m jobs.build_station_active
SNAPSHOT_DATE=2026-07-01 python -m jobs.build_bike_location
SNAPSHOT_DATE=2026-07-01 python -m jobs.build_fact_station_inventory
```

`dag_risk_decision`이 만드는 것:

```bash
cd pipeline/risk_model
export PYTHONPATH=../..:../../ingestion:$PYTHONPATH
set -a && source ../../ingestion/.env && set +a
SNAPSHOT_DATE=2026-07-01 python -m jobs.build_bike_features_daily
SNAPSHOT_DATE=2026-07-01 python -m jobs.build_fact_bike_risk
SNAPSHOT_DATE=2026-07-01 python -m jobs.build_fact_bike_decision
```

(Airflow BashSensor들이 보는 watermark/snapshot 조건은 이미 그 값을 넘어서 있어
DAG를 실제로 띄우지 않고 job만 직접 실행해도 DAG가 하는 것과 동일한 결과를 낸다.)

### 2) serving_sync 본검증

README(`pipeline/serving_sync/README.md`)의 로컬 실행 절차 그대로:

```bash
cd pipeline/serving_sync
export PYTHONPATH=../../ingestion:jobs:$PYTHONPATH
set -a && source ../../ingestion/.env && set +a

SNAPSHOT_DATE=2026-07-01 python -m jobs.build_mart_bike_risk_daily
SNAPSHOT_DATE=2026-07-01 python -m jobs.write_bike_risk_daily
ICEBERG_TABLE=bike_catalog.gold.mart_bike_risk_daily POSTGRES_TABLE=bike_risk_daily \
    SNAPSHOT_DATE=2026-07-01 python -m jobs.verify_serving_sync

SNAPSHOT_DATE=2026-07-01 python -m jobs.build_mart_station_daily
SNAPSHOT_DATE=2026-07-01 python -m jobs.write_station_daily
ICEBERG_TABLE=bike_catalog.gold.mart_station_daily POSTGRES_TABLE=station_daily \
    SNAPSHOT_DATE=2026-07-01 python -m jobs.verify_serving_sync
```

### 3) 서빙 Postgres 직접 조회로 최종 확인

```sql
SELECT snapshot_date, count(*) FROM serving.bike_risk_daily GROUP BY snapshot_date;
SELECT snapshot_date, count(*) FROM serving.station_daily GROUP BY snapshot_date;
```

## 결과

| 항목 | Iceberg (`gold.mart_*`) | Postgres (`serving.*`) | verify_serving_sync |
|---|---|---|---|
| `bike_risk_daily` (2026-07-01) | 37,079행 | 37,079행 | 통과 |
| `station_daily` (2026-07-01) | 2,735행 | 2,735행 | 통과 |

`serving.bike_risk_daily` 샘플 조회로 `risk_grade` 등 실제 값이 정상적으로
채워졌음을 확인.

## 진행 중 발견/수정한 이슈

1. **서빙 테이블이 `public` 스키마에 생성되던 버그**: `serving_db.py`가 스키마를
   지정하지 않고 `CREATE TABLE`/`INSERT`/`DELETE`를 실행해 `station_daily`/
   `bike_risk_daily`가 (이미 만들어져 있던) `serving` 스키마가 아니라 `public`
   스키마에 생성되고 있었다. `SCHEMA = "serving"` 상수를 추가해 DDL/DML을 전부
   `serving.station_daily` / `serving.bike_risk_daily`로 스키마 한정하도록 고쳤다.
   호출부(`write_*.py`의 `TABLE = "station_daily"` 등, `verify_serving_sync`의
   `POSTGRES_TABLE` 환경변수)는 그대로 두고 스키마 한정을 `serving_db.py` 내부로만
   캡슐화했다.
2. **`gold.mart_station_daily` <-> `serving.station_daily` 컬럼명 불일치**: 마트
   출력 단계에서 상류 소스(`station_active`의 `latitude`/`longitude`,
   `fact_station_inventory`의 `bike_cnt`, `station_risk_shared`의 `risk_cnt`)를
   `x`/`y`/`bike_count`/`risk_count`로 이름을 바꿔 내보내고 있었다. 두 스키마의
   컬럼명을 동일하게 맞추기 위해 이 리네임을 제거하고 상류 이름 그대로
   (`latitude`/`longitude`/`bike_cnt`/`risk_cnt`) 유지하도록 `build_mart_station_daily.py`
   / `write_station_daily.py` / `serving_db.py`(DDL) / 관련 테스트를 수정했다.
   `bike_risk_daily`는 원래부터 마트-서빙 컬럼명이 일치해 변경 없음.

## 참고

- `services/api/app/state.py`는 여전히 스키마 없이(`public` 기준)
  `station_daily`/`bike_risk_daily`를 조회하고, 컬럼명도 옛 이름(`x`/`y`/
  `bike_count`/`risk_count`)을 그대로 쓰고 있다. app 연동은 별도 작업으로 남겨둔
  상태라 이번 검증에서는 손대지 않았다 - app을 `serving` 스키마 + 새 컬럼명에 맞게
  연결하는 작업이 필요하다.
