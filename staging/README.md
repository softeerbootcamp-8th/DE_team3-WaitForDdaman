# staging

Bronze / Silver 생성

- DuckDB(변환) + PyIceberg(입출력) ETL. Spark 세션을 쓰지 않는다 (#143)
  - 볼륨상 엔진이 필요한 건 `transform_silver_rental_history`의 중복 제거 윈도우뿐이고,
    나머지는 기존 Spark SQL 표현을 번역 오류 없이 옮기려고 같은 SQL 엔진으로 통일했다
  - 공통 모듈은 `ingestion/common/`을 그대로 재사용한다
    (`iceberg_catalog`, `iceberg_io`, `duckdb_io`, `sql_assert`, `partition_listing`, `watermark`)
  - 별도 requirements 파일이 없다 - `duckdb` / `pyarrow` / `pyiceberg`는 이미
    `ingestion/requirements.txt`에 있고 컨테이너 이미지가 그걸 설치한다

## jobs

- `transform_silver_rental_history.py`: `bronze.rental_history` -> `silver.rental_history` 타입 캐스팅 + 품질 검증 (`common/sql_assert.py`, PyDeequ 대체) (대여이력 전용, 워터마크 기반 증분 처리)
  - 컬럼: `bike_id`, `rent_dt`, `return_dt`, `use_distance_m`, `rent_station_id`, `return_station_id`, `rent_date_partition`, `source_file`, `ingested_at`(Bronze lineage 승계)
  - `rent_dt`/`return_dt`는 소스(API/CSV 백필)마다 포맷이 달라 알려진 포맷을 순서대로 시도하고, 전부 실패하면 배치를 중단시킴(조용히 드롭/오염 방지)
  - 상한선: Bronze 워터마크(`_meta/watermark/rental_history.json`), 하한선: Silver 전용 워터마크(`config/watermark_keys.py`의 `SILVER_RENTAL_HISTORY`)
  - `MAX_DAYS_PER_RUN` 미지정 시 기본 31일로 캡됨(`DEFAULT_MAX_DAYS_PER_RUN`) - 워터마크가 오래 밀린 채 처음 돌아도 통째로 큰 배치가 되지 않게 함
  - 중복 제거 윈도우 `(bike_id, rent_dt)`는 하루 안에서 닫힌다(`rent_date_partition`이 `rent_dt`에서 파생됨) - 날짜 청크로 나눠 여러 번 돌려도 결과가 같다. 정렬키를 전순서로 잡아 재실행 결과가 값까지 동일하다

- `silver_failure_report.py`: `bronze.failure_report` -> `silver.failure_report` 전체 재처리 (워터마크로 구간을 자르지 않음, 태스크 1개)
  - 컬럼: `bike_no`, `reg_dttm`, `failure_type` + 파티션 컬럼 `reg_date_partition`
  - **파티션 `reg_date_partition`(identity, `yyyy-MM-dd` = `date(reg_dttm)`)** - 브론즈의 동명 컬럼(적재일)과 의미가 다르다. Gold 담당자와의 인터페이스 계약

- `silver_station_master.py`: `bronze.station_master`(api 파티션만) -> `silver.station_master` 타입 캐스팅 + region 파생 + 대여소명 공백 정규화

- `silver_station_active.py`: `bronze.station_active` -> `silver.station_active` station_id 필터 테이블 (날짜 파라미터 없는 전체 스냅샷, 워터마크 없음)
  - 컬럼: `snapshot_date`, `station_id` 두 개뿐 — 대여소명/위경도/자치구 등은 `silver.station_master`에서 station_id로 조인해서 사용
  - station_id가 null이거나 같은 스냅샷 내 중복이면 드롭(경고 로그), 정제 후 0행이면 배치 실패
  - `SNAPSHOT_DATE` 미지정 시 Bronze의 최신 스냅샷을 처리

## Airflow

- DAG: `silver_gold_daily_batch_rental_history` (`airflow/dags/silver_gold_daily_batch_rental_history_dag.py`)
  - `transform_silver_rental_history`만 실행 (2026-08-17부터 `build_gold_dim_bike`는 `gold_dim_fact`로 이관됨)
  - 매일 07:30 KST(Bronze 06:00 시작 이후로 고정 오프셋 - 실제 의존관계 아님)

- DAG: `silver_station_active` (`airflow/dags/silver_station_active_dag.py`)
  - 매일 07:00 KST (Bronze 06:00 이후 고정 오프셋)
  - catchup 없음 (API가 과거 스냅샷을 소급 조회 불가)

## 로컬 실행

```bash
cd staging
export PYTHONPATH=..:../ingestion:$PYTHONPATH  # 최상위 config/ 패키지 + ingestion/common
set -a && source ../ingestion/.env && set +a
python -m jobs.transform_silver_rental_history
MAX_DAYS_PER_RUN=30 python -m jobs.transform_silver_rental_history   # 백필: 30일씩 나눠 처리

python -m jobs.silver_station_active
SNAPSHOT_DATE=2026-08-14 python -m jobs.silver_station_active   # 특정 날짜 재처리
```

**주의**: Silver/Gold 워터마크가 한 번도 안 찍힌 상태(cold start)에서 이 잡을 돌리면 기본값 `BACKFILL_START_DATE`(2015-01-01)부터 시작하려 든다. 실제 Bronze 데이터가 그보다 훨씬 뒤(예: 2026년)부터 있다면, Silver로 넘어가기 전에 데이터 시작일 하루 전으로 워터마크를 먼저 찍어둘 것(`ingestion/jobs/set_watermark.py`, `DATASET=silver_rental_history`) - 안 그러면 매 실행마다 데이터 없는 과거 구간만 훑느라 여러 번 트리거해야 실제 데이터에 도달한다.

로컬 Spark 설정(`local[2]`, driver memory 6g)은 여러 달치를 한 번에 처리하기엔 부족할 수 있다(실측: 5개월치를 한 번에 시도하면 JVM이 죽음) - `MAX_DAYS_PER_RUN`을 30 정도로 잡고 여러 번 나눠 트리거할 것.

## silver.station_active 인터페이스 (담당 4 전달용)

| 항목 | 값 |
|---|---|
| 테이블 | `silver.station_active` |
| 컬럼 | `snapshot_date DATE`, `station_id STRING` |
| 유일키 | `(snapshot_date, station_id)` |
| 기준일 컬럼 | `snapshot_date` |
| 파티션 | `snapshot_date` |
| 의미 | 그 날 station_id가 있으면 = 그 날 실시간 대여정보 API 응답에 실제로 잡힌 대여소 (운영 중 최종 판정은 Gold 몫) |
| NULL 처리 | station_id 없는/중복 행은 Silver에서 이미 제거됨 |
| 재실행 동작 | 같은 snapshot_date 파티션을 덮어씀(`common/iceberg_io.py`의 `overwrite_partition`), 멱등 |
| 적재 완료 시점 | `silver_station_active` DAG, 매일 07:00 KST |
| 다른 속성 필요시 | `silver.station_master`를 `station_id`로 조인 |
