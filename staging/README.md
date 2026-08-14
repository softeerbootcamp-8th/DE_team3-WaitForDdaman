# staging

Bronze / Silver 생성

- Spark ETL

## jobs

- `transform_silver_rental_history.py`: `bronze.rental_history` -> `silver.rental_history` 타입 캐스팅 + PyDeequ 검증 (대여이력 전용, 워터마크 기반 증분 처리)
  - 컬럼: `bike_id`, `rent_dt`, `return_dt`, `use_distance_m`, `rent_station_id`, `return_station_id`, `rent_date_partition`, `source_file`, `ingested_at`(Bronze lineage 승계)
  - `rent_dt`/`return_dt`는 소스(API/CSV 백필)마다 포맷이 달라 알려진 포맷을 순서대로 시도하고, 전부 실패하면 배치를 중단시킴(조용히 드롭/오염 방지)
  - 상한선: Bronze 워터마크(`_meta/watermark/rental_history.json`), 하한선: Silver 전용 워터마크(`config/watermark_keys.py`의 `SILVER_RENTAL_HISTORY`)
  - `MAX_DAYS_PER_RUN` 미지정 시 기본 31일로 캡됨(`DEFAULT_MAX_DAYS_PER_RUN`) - 워터마크가 오래 밀린 채 처음 돌아도 통째로 큰 배치가 되지 않게 함

## Airflow

- DAG: `silver_gold_daily_batch_rental_history` (`airflow/dags/silver_gold_daily_batch_rental_history_dag.py`)
  - `transform_silver_rental_history` -> `build_gold_dim_bike`(`pipeline/risk_model`) 순서로 실행
  - 매일 07:30 KST(Bronze 06:00 시작 이후로 고정 오프셋 - 실제 의존관계 아님)

## 로컬 실행

```bash
cd staging
export PYTHONPATH=..:../ingestion:$PYTHONPATH  # 최상위 config/ 패키지 + ingestion/common
set -a && source ../ingestion/.env && set +a
python -m jobs.transform_silver_rental_history
MAX_DAYS_PER_RUN=30 python -m jobs.transform_silver_rental_history   # 백필: 30일씩 나눠 처리
```

**주의**: Silver/Gold 워터마크가 한 번도 안 찍힌 상태(cold start)에서 이 잡을 돌리면 기본값 `BACKFILL_START_DATE`(2015-01-01)부터 시작하려 든다. 실제 Bronze 데이터가 그보다 훨씬 뒤(예: 2026년)부터 있다면, Silver로 넘어가기 전에 데이터 시작일 하루 전으로 워터마크를 먼저 찍어둘 것(`ingestion/jobs/set_watermark.py`, `DATASET=silver_rental_history`) - 안 그러면 매 실행마다 데이터 없는 과거 구간만 훑느라 여러 번 트리거해야 실제 데이터에 도달한다.

로컬 Spark 설정(`local[2]`, driver memory 6g)은 여러 달치를 한 번에 처리하기엔 부족할 수 있다(실측: 5개월치를 한 번에 시도하면 JVM이 죽음) - `MAX_DAYS_PER_RUN`을 30 정도로 잡고 여러 번 나눠 트리거할 것.
