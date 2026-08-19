# collection_priority

대여소 수거 우선순위 리스트 생성을 위한 Gold 데이터 파이프라인

- 수거 우선순위 계산에 필요한 Gold 스키마(dim_bike / bike_location / station_active / fact_station_inventory)를 만든다
- 우선순위 산정 로직(모델 학습/추론)은 이 폴더의 책임이 아니다 - `pipeline/train_risk_model`이 그쪽을 담당하고, 여기는 그 입력이 되는 데이터를 만드는 ETL 잡만 둔다

## 데이터 흐름

```
silver.rental_history ─────┬──> gold.dim_bike
                            └──> gold.bike_location ───────────────┐
                                                                    │
silver.station_master ─────┐                                      │
silver.station_active ─────┴──> gold.station_active ───────────────┼──> gold.fact_station_inventory
                                                                    │
silver.bike_man_action ─────────> gold.bike_last_action ────────────┘
```

- `gold.bike_last_action`은 최종 산출물이 아니라 `gold.fact_station_inventory`를 만들기 위한 중간 상태 테이블이다 - `silver.bike_man_action`(계속 쌓이는 이벤트 로그)에서 자전거별 "가장 최근 수거/배치 이벤트가 뭐였나"만 증분으로 유지해둔 것 (자세한 이유는 아래 `build_fact_station_inventory.py` 항목 참고)
- 위 4개 build 잡 중 `gold.bike_location`을 만드는 `build_bike_location.py`와 `gold.bike_last_action`을 만드는 로직(`build_fact_station_inventory.py` 내부)만 baseline+delta 증분 처리이고, 나머지(`build_dim_bike.py`의 append, `build_station_active.py`/`build_fact_station_inventory.py`의 최종 집계)는 매번 전체 재계산/전체 덮어쓰기다

## jobs

- `build_dim_bike.py`: `silver.rental_history` -> `gold.dim_bike` (자전거별 최초 등장일, append-only INSERT + PyDeequ 검증)
  - 컬럼: `snapshot_date`(파티션, 최초 등장일), `bike_id`(PK), `first_seen_at`, `start_year`
  - 한 자전거는 최초 등장한 날짜 파티션에 딱 한 번만 존재 (MERGE/UPDATE 없음 - first_seen_at은 불변, UPSERT 시 기존값 절대 덮어쓰지 않음)
  - 상한선: Silver 워터마크(`config/watermark_keys.py`의 `SILVER_RENTAL_HISTORY`), 하한선: Gold 전용 워터마크(`GOLD_DIM_BIKE`)
  - `MAX_DAYS_PER_RUN` 미지정 시 기본 31일로 캡됨 - Silver와 동일한 이유(cold start 시 무제한 배치 방지)
- `build_bike_location.py`: `silver.rental_history` -> `gold.bike_location` (TEMP, 자전거별 현재 위치)
  - 컬럼: `bike_id`, `last_station_id`, `last_event_at`, `snapshot_date`
  - 증분 처리(2026-08-17): 기존 `gold.bike_location`을 baseline으로, 아직 반영 안 된 `rent_date_partition` 구간만 델타로 스캔해서 병합 - 전체 히스토리 재스캔 안 함
  - 델타 시작일은 `gold.bike_location` 자신의 `MAX(snapshot_date)`를 워터마크로 재사용해서 정한다(별도 워터마크 파일 없음) - 평소엔 구간이 하루뿐이지만, DAG 실행이 하루 이상 밀린 뒤에도 그 사이 파티션을 전부 다시 스캔해 자동 복구된다(2026-08-17, 리뷰로 발견: "어제 하루"로 고정하면 실행이 밀렸을 때 그 사이 파티션이 영영 재스캔 안 되는 조용한 데이터 유실이 있었음)
  - `last_station_id`가 null이면 데이터 결측이 아니라 대여소가 아닌 곳에 반납된 경우(노상 방치 등) - `fact_station_inventory` 재고 집계에서 자연스럽게 제외됨
  - 파티션 없이 매 실행마다 전체 덮어쓰기 (이력을 쌓지 않는 "현재 상태" 테이블)
  - 적재 전 PyDeequ 검증(`bike_id` 유일성/완전성) - 실패 시 적재 없이 배치 중단
- `build_station_active.py`: `silver.station_master` + `silver.station_active` -> `gold.station_active` (TEMP, 운영 중인 대여소만)
  - 컬럼: `station_id`, `station_name`, `region`, `district`, `hold_num`, `latitude`, `longitude`, `snapshot_date`
  - station_master(전체 등록 대여소) 중 station_active(실시간 상태가 보고되는 대여소)에도 존재하는 것만 INNER JOIN으로 필터링
  - `silver.station_active`는 `station_id` 필터 테이블(컬럼 `snapshot_date`, `station_id` 2개뿐, `staging/jobs/silver_station_active.py`)이라 이 잡은 station_id만 뽑아 쓰고 나머지 속성은 전부 station_master에서 가져온다 - 대여소 수가 적어(수백~수천 건) `F.broadcast()`로 셔플 없이 조인한다
  - 적재 전 PyDeequ 검증(`station_id` 유일성/완전성) - 실패 시 적재 없이 배치 중단
- `build_fact_station_inventory.py`: `gold.bike_location` + `gold.station_active` + `silver.bike_man_action` -> `gold.fact_station_inventory`
  - 컬럼: `station_id`, `bike_cnt`, `hold_num`, `target_bike_cnt`, `snapshot_date`
  - 자전거의 최종 위치는 대여이력 기준 위치(`bike_location`)와 수거(COLLECT)/배치(DEPLOY) 이벤트 중 더 최신인 쪽을 따른다 - COLLECT가 최신이면 재고 집계에서 제외, DEPLOY가 최신이면 그 station_id로 위치를 덮어씀 (자세한 규칙은 파일 docstring 참고)
  - `target_bike_cnt`는 거치대 수(`hold_num`)를 목표치로 사용
  - `gold.station_active`(운영 중인 대여소만) 기준으로 집계하므로, 자전거가 0대인 대여소도 0으로 나온다 - `station_active`(left/outer 쪽)를 기준으로 `bike_cnt` 집계 결과(대여소당 최대 1행이라 더 작음)를 `F.broadcast()`로 왼쪽에 조인한다(left outer join은 build 대상이 오른쪽이어야 해서 작은 쪽을 오른쪽에 둠)
  - `bike_cnt` 집계 자체는 매번 전체 재계산(자전거 하나만 바뀌어도 대여소 합계가 통째로 바뀌므로 carry-forward 불가). 대신 그 재료인 자전거별 최신 수거/배치 이벤트는 `gold.bike_last_action`(신규, 증분 유지 상태 테이블)에서 가져온다 - `silver.bike_man_action`(계속 커지는 이벤트 로그)을 매번 전체 스캔하지 않고, 아직 반영 안 된 구간만 스캔해서 병합(델타 구간 산정 방식은 `build_bike_location.py`와 동일한 self-tracking watermark 방식)
  - 적재 전 PyDeequ 검증 - `gold.bike_last_action`은 `bike_id` 유일성/완전성, `gold.fact_station_inventory`는 `station_id` 유일성/완전성 + `bike_cnt` 음수 아님 - 실패 시 적재 없이 배치 중단

## Airflow

- DAG: `dag_gold_dim_fact` (`airflow/dags/gold_dim_fact_dag.py`)
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

## 테스트

```bash
cd pipeline/collection_priority
export PYTHONPATH=../..:../../ingestion:$PYTHONPATH  # 최상위 config/ 패키지 + ingestion/common
python -m pytest tests/ -v
```

Iceberg/S3 카탈로그가 필요 없는 순수 병합/조인 로직(baseline+delta 병합, 최종 위치 결정,
재고 집계, station 조인)만 검증한다 - `staging/tests`와 동일한 방식으로 카탈로그 설정 없는
최소 SparkSession을 직접 만들어 DataFrame을 주입한다. `spark.read.table(...)`로 실제
Iceberg 테이블을 읽는 부분(`_baseline`/`_delta`/`_latest_snapshot` 등)은 여기서 다루지
않는다 - LocalStack + 실제 카탈로그가 있어야 하므로 "로컬 실행" 절의 잡 실행으로 검증한다.

## Iceberg 컴팩션 / 스냅샷 관리

`bike_location`/`station_active`/`fact_station_inventory`/`bike_last_action`은 전부
파티션 없이 매 실행마다 전체를 `overwritePartitions()`로 새로 쓰는 TEMP류 테이블이다.
"덮어쓴다"는 건 쿼리 결과(현재 스냅샷) 기준이지 물리 파일 기준이 아니다 - Iceberg는
기존 파일을 고쳐 쓰지 않고 매번 완전히 새 파일 + 새 스냅샷을 만들고, 어제 스냅샷이
가리키던 파일은 삭제되지 않은 채 그대로 남는다(타임트래블을 위해 스냅샷 이력을
기본적으로 계속 보관함). 그래서 매일 "오늘 것만 보이는" 전체 사본을 새로 써도,
스토리지에는 지나간 날짜 수만큼의 사본이 계속 쌓인다 - 이걸 다루는 유지보수
프로시저는 서로 다른 두 가지다:

- `rewrite_data_files` - 지금 보이는(현재) 스냅샷 안의 작은 파일들을 큰 파일로 병합
  (스캔 시 파일 오픈 오버헤드 감소)
- `expire_snapshots` - 이미 안 보이는(과거) 스냅샷 자체를 만료시켜, 그 스냅샷만
  참조하던 파일을 실제로 삭제 대상으로 만듦 - TEMP류 테이블의 스토리지 증가를 막는
  핵심은 사실 이쪽이다(`rewrite_data_files`만으로는 과거 스냅샷 파일이 안 지워짐)

둘 다 데이터 내용(쿼리 결과)은 바꾸지 않고 물리 파일/스냅샷 이력만 정리하므로 언제
다시 돌려도 안전(멱등)하다.

- `jobs/compact_gold_tables.py`: 위 4개 테이블에 대해 `expire_snapshots`(7일보다 오래된
  스냅샷 만료, 단 최소 3개는 나이와 무관하게 항상 보존 - 롤백 여지)를 먼저 돌리고
  `rewrite_data_files`를 그 뒤에 돌린다. 테이블이 아직 없으면 조용히 건너뛴다.
- DAG: `dag_gold_maintenance` (`airflow/dags/gold_maintenance_dag.py`) - 매주 일요일
  03:00 KST. daily 배치(`dag_gold_dim_fact`, 08:00 KST)와 분리한 이유는 파일/스냅샷이
  하루에 1개씩만 늘어나 매일 돌릴 필요가 없고, 유지보수 실패가 daily 배치 SLA에
  영향을 주지 않게 하기 위함(자세한 설계는 두 파일의 docstring 참고).

```bash
# 로컬에서 수동 실행
cd pipeline/collection_priority
export PYTHONPATH=../..:../../ingestion:$PYTHONPATH
python -m jobs.compact_gold_tables
```
