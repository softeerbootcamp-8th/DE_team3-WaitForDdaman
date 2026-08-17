# collection_priority

대여소 수거 우선순위 리스트 생성을 위한 Gold 데이터 파이프라인

- 수거 우선순위 계산에 필요한 Gold 스키마(dim_bike / bike_location / station_active / fact_station_inventory)를 만든다
- 우선순위 산정 로직(모델 학습/추론)은 이 폴더의 책임이 아니다 - `pipeline/risk_model`이 그쪽을 담당하고, 여기는 그 입력이 되는 데이터를 만드는 ETL 잡만 둔다

## jobs

- `build_dim_bike.py`: `silver.rental_history` -> `gold.dim_bike` (자전거별 최초 등장일, append-only INSERT + PyDeequ 검증)
  - 컬럼: `snapshot_date`(파티션, 최초 등장일), `bike_id`(PK), `first_seen_at`, `start_year`
  - 한 자전거는 최초 등장한 날짜 파티션에 딱 한 번만 존재 (MERGE/UPDATE 없음 - first_seen_at은 불변, UPSERT 시 기존값 절대 덮어쓰지 않음)
  - 상한선: Silver 워터마크(`config/watermark_keys.py`의 `SILVER_RENTAL_HISTORY`), 하한선: Gold 전용 워터마크(`GOLD_DIM_BIKE`)
  - `MAX_DAYS_PER_RUN` 미지정 시 기본 31일로 캡됨 - Silver와 동일한 이유(cold start 시 무제한 배치 방지)
- `build_bike_location.py`: `silver.rental_history` -> `gold.bike_location` (TEMP, 자전거별 현재 위치)
  - 컬럼: `bike_id`, `last_station_id`, `last_event_at`, `snapshot_date`
  - 자전거별 가장 최근 대여이력 1건만 봐서, 반납 완료면 반납 대여소, 아직 운행 중(반납 전)이면 위치 없음(NULL)으로 판정
  - 파티션 없이 매 실행마다 전체 덮어쓰기 (이력을 쌓지 않는 "현재 상태" 테이블)
- `build_station_active.py`: `silver.station_master` + `silver.station_active` -> `gold.station_active` (TEMP, 운영 중인 대여소만)
  - 컬럼: `station_id`, `station_name`, `region`, `district`, `hold_num`, `latitude`, `longitude`, `snapshot_date`
  - station_master(전체 등록 대여소) 중 station_active(실시간 상태가 보고되는 대여소)에도 존재하는 것만 INNER JOIN으로 필터링
  - `silver.station_active`는 `station_id` 필터 테이블(컬럼 `snapshot_date`, `station_id` 2개뿐, `staging/jobs/silver_station_active.py`)이라 이 잡은 station_id만 뽑아 쓰고 나머지 속성은 전부 station_master에서 가져온다
- `build_fact_station_inventory.py`: `gold.bike_location` + `gold.station_active` + `silver.bike_man_action` -> `gold.fact_station_inventory`
  - 컬럼: `station_id`, `bike_cnt`, `hold_num`, `target_bike_cnt`, `snapshot_date`
  - 자전거의 최종 위치는 대여이력 기준 위치(`bike_location`)와 수거(COLLECT)/배치(DEPLOY) 이벤트 중 더 최신인 쪽을 따른다 - COLLECT가 최신이면 재고 집계에서 제외, DEPLOY가 최신이면 그 station_id로 위치를 덮어씀 (자세한 규칙은 파일 docstring 참고)
  - `target_bike_cnt`는 거치대 수(`hold_num`)를 목표치로 사용
  - `gold.station_active`(운영 중인 대여소만) 기준으로 집계하므로, 자전거가 0대인 대여소도 0으로 나온다

## Airflow

- DAG: `dag_gold_dim_fact` (`airflow/dags/dag_gold_dim_fact.py`)
  - `wait_for_silver_rental_history` -> `build_dim_bike` / `build_bike_location`
  - `wait_for_silver_station_master` + `wait_for_silver_station_active` -> `build_station_active`
  - `build_bike_location` + `build_station_active` + `wait_for_silver_bike_man_action` -> `build_fact_station_inventory`
  - 각 build 태스크는 실제로 읽는 Silver 소스의 센서에만 연결되어 있다 (이미 다른 태스크를 거쳐 간접 보장되는 센서는 중복 연결하지 않음 - 자세한 설계는 DAG 파일 docstring 참고)
  - 매일 08:00 KST
- (구) `silver_gold_daily_batch_rental_history` DAG는 더 이상 `build_dim_bike`를 실행하지 않음 (2026-08-17, `dag_gold_dim_fact`로 이관)

## 로컬 실행

```bash
cd pipeline/collection_priority
export PYTHONPATH=../..:../../ingestion:$PYTHONPATH  # 최상위 config/ 패키지 + ingestion/common
set -a && source ../../ingestion/.env && set +a
python -m jobs.build_dim_bike
MAX_DAYS_PER_RUN=30 python -m jobs.build_dim_bike   # 백필: 30일씩 나눠 처리

SNAPSHOT_DATE=2026-08-17 python -m jobs.build_bike_location
SNAPSHOT_DATE=2026-08-17 python -m jobs.build_station_active
SNAPSHOT_DATE=2026-08-17 python -m jobs.build_fact_station_inventory
```

**주의**: `build_dim_bike`의 Gold 워터마크도 Silver와 마찬가지로 cold start 시 `BACKFILL_START_DATE`(2015-01-01)부터 시작하려 든다. Silver 워터마크를 먼저 실제 데이터 시작일 하루 전으로 맞춰뒀다면, Gold도 동일하게 맞춰둬야 한다(`ingestion/jobs/set_watermark.py`, `DATASET=gold_dim_bike`) - 안 그러면 이 잡만 따로 몇 번을 돌려도 매번 "신규 등장 자전거 없음"만 반복하며 빈 과거 구간만 훑는다. 나머지 3개 잡은 워터마크가 없고 `SNAPSHOT_DATE`(미지정 시 오늘) 하루치만 처리한다.
