# risk_model

위험도 Feature 생성

- 위험도 계산 결과 생성

## jobs

- `build_dim_bike.py`: `silver.rental_history` -> `gold.dim_bike` (자전거별 최초 등장일, append-only INSERT + PyDeequ 검증)
  - 컬럼: `snapshot_date`(파티션, 최초 등장일), `bike_id`(PK), `first_seen_at`, `start_year`
  - 한 자전거는 최초 등장한 날짜 파티션에 딱 한 번만 존재 (MERGE/UPDATE 없음 - first_seen_at은 불변)
  - 상한선: Silver 워터마크(`config/watermark_keys.py`의 `SILVER_RENTAL_HISTORY`), 하한선: Gold 전용 워터마크(`GOLD_DIM_BIKE`)
  - `MAX_DAYS_PER_RUN` 미지정 시 기본 31일로 캡됨 - Silver와 동일한 이유(cold start 시 무제한 배치 방지)

## Airflow

- DAG: `silver_gold_daily_batch_rental_history` (`airflow/dags/silver_gold_daily_batch_rental_history_dag.py`)
  - `transform_silver_rental_history`(`staging`) -> `build_gold_dim_bike` 순서로 실행 (Silver가 먼저 끝나야 그 워터마크를 상한선으로 읽을 수 있음)
  - 매일 07:30 KST

## 로컬 실행

```bash
cd pipeline/risk_model
export PYTHONPATH=../..:../../ingestion:$PYTHONPATH  # 최상위 config/ 패키지 + ingestion/common
set -a && source ../../ingestion/.env && set +a
python -m jobs.build_dim_bike
MAX_DAYS_PER_RUN=30 python -m jobs.build_dim_bike   # 백필: 30일씩 나눠 처리
```

**주의**: Gold 워터마크도 Silver와 마찬가지로 cold start 시 `BACKFILL_START_DATE`(2015-01-01)부터 시작하려 든다. Silver 워터마크를 먼저 실제 데이터 시작일 하루 전으로 맞춰뒀다면, Gold도 동일하게 맞춰둬야 한다(`ingestion/jobs/set_watermark.py`, `DATASET=gold_dim_bike`) - 안 그러면 이 잡만 따로 몇 번을 돌려도 매번 "신규 등장 자전거 없음"만 반복하며 빈 과거 구간만 훑는다.
