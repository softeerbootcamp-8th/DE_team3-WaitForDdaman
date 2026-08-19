# serving_sync

Gold Iceberg 마트를 서빙 Postgres(station_daily/bike_risk_daily)로 동기화하는 파이프라인

- Gold(`gold.fact_bike_risk` / `gold.fact_bike_decision` / `gold.station_active` /
  `gold.fact_station_inventory` 등)를 join해 API/프론트가 바로 쓸 수 있는 모양의
  일별 마트(`gold.mart_bike_risk_daily` / `gold.mart_station_daily`)를 만들고, 그걸
  Postgres로 옮긴다 (services/api가 그 Postgres 테이블만 읽는다)
- 마트 산정 로직의 입력이 되는 위험도/대여중단 결정(`gold.fact_bike_risk` /
  `gold.fact_bike_decision`)은 이 폴더의 책임이 아니다 - `pipeline/risk_model`이
  그쪽을 담당하고, 여기는 그 결과 + 재고/대여소 Gold 테이블을 조합해 서빙용 마트를
  만들고 Postgres에 반영하는 job만 둔다

## jobs

- `build_mart_bike_risk_daily.py`: 여러 gold 테이블 join -> `gold.mart_bike_risk_daily`
  (`{{ ds }}` 파티션 단위 OVERWRITE). `action` 컬럼은 mart/serving 레이어에 싣지 않고,
  수거 대상 선정은 `bikeman_event_generator`에서 `risk_score` 상위 N대 기준으로 처리한다
- `build_mart_station_daily.py`: `station_active` + `fact_station_inventory` + 위험도
  집계 -> `gold.mart_station_daily` (`{{ ds }}` 파티션 단위 OVERWRITE)
- `write_bike_risk_daily.py` / `write_station_daily.py`: 각 mart의 `SNAPSHOT_DATE`
  파티션을 collect() 후 postgres로 반영. UPSERT가 아니라 해당 파티션을 삭제하고
  재삽입하는 **파티션 교체**(`serving_db.replace_partition`, 같은 트랜잭션) -
  mart가 이전 실행보다 줄어드는 경우(대여소 비활성화 등)에도 postgres에 오래된 행이
  남지 않아 `verify_serving_sync`의 row count 비교가 항상 유효하다 (Iceberg 쪽
  build_mart_*가 overwritePartitions를 쓰는 것과 동일한 의미)
- `verify_serving_sync.py`: Iceberg mart와 postgres 테이블의 `SNAPSHOT_DATE` 파티션
  row count를 비교하는 공용 검증기 (build/write와 원자적으로 분리된 별도 태스크,
  spec §6)
- `serving_db.py`: 서빙 Postgres 접속 + `replace_partition`/`count_rows`/DDL 유틸.
  `db_client.py`(bikeman)와 동일한 이유로 psycopg2 직접 연결 - `python -m jobs.X`로
  Airflow 없이 단독 실행 가능해야 한다
- `station_risk_shared.py`: 두 build_mart_* job이 공용으로 쓰는 대여소별 위험도 집계
  헬퍼

## 필요한 환경변수

`.env`에 다음을 설정한다 (docker-compose의 postgres 롤과 반드시 일치해야 함):

```
SERVING_DB_HOST=postgres
SERVING_DB_PORT=5432
SERVING_DB_NAME=<루트 .env의 POSTGRES_DB와 동일>
SERVING_DB_USER=<루트 .env의 POSTGRES_USER와 동일>
SERVING_DB_PASSWORD=<루트 .env의 POSTGRES_PASSWORD와 동일>
SLACK_WEBHOOK_URL=<실패 알림용, 미설정 시 알림만 건너뜀>
BIKEMAN_COLLECT_LIMIT=500   # 미설정 시 risk_score 상위 500대를 COLLECT 대상으로 사용
```

## Airflow

- DAG: `gold_to_serving_sync` (`airflow/dags/gold_to_serving_sync_dag.py`)
  - `dag_risk_decision`의 마지막 태스크(`build_fact_bike_decision`)가 끝나면
    `TriggerDagRunOperator`로 트리거된다 (`dag_gold_dim_fact`가 아님 - bike_risk_daily가
    필요로 하는 `fact_bike_risk`/`fact_bike_decision`은 `dag_risk_decision`의 산출물)
  - `SNAPSHOT_DATE`는 트리거한 `dag_risk_decision`이 conf로 넘긴 날짜를 우선하고,
    없으면(수동 트리거 등) 이 DAG 자신의 `ds`로 폴백한다 - 두 DAG가 각자 다른 경로로
    날짜를 정해 백필/미래 스케줄 실행에서 어긋나는 문제를 막기 위함(자세한 설명은
    DAG 파일의 주석 참고)
  - `build_mart_* -> write_* -> verify_*` 두 브랜치(bike_risk_daily / station_daily)는
    서로 의존하지 않아 병렬 실행
  - 실패해도 `dag_risk_decision`을 실패로 만들지 않음(`wait_for_completion=False`) -
    이미 만들어진 gold 데이터 자체는 유효하므로. 대신 각 태스크에 Slack 알림을 건다

## 로컬 실행

```bash
cd pipeline/serving_sync
export PYTHONPATH=../../ingestion:jobs:$PYTHONPATH   # ingestion/common + 이 폴더 jobs를 최상위 모듈처럼 import
set -a && source ../../ingestion/.env && set +a

SNAPSHOT_DATE=2026-08-17 python -m jobs.build_mart_bike_risk_daily
SNAPSHOT_DATE=2026-08-17 python -m jobs.write_bike_risk_daily
ICEBERG_TABLE=bike_catalog.gold.mart_bike_risk_daily POSTGRES_TABLE=bike_risk_daily \
    SNAPSHOT_DATE=2026-08-17 python -m jobs.verify_serving_sync

SNAPSHOT_DATE=2026-08-17 python -m jobs.build_mart_station_daily
SNAPSHOT_DATE=2026-08-17 python -m jobs.write_station_daily
ICEBERG_TABLE=bike_catalog.gold.mart_station_daily POSTGRES_TABLE=station_daily \
    SNAPSHOT_DATE=2026-08-17 python -m jobs.verify_serving_sync
```

Airflow 컨테이너 안에서 실행할 때(`docker exec airflow-scheduler ...`)는 `PYTHONPATH`를
`/opt/airflow/ingestion:/opt/airflow/pipeline/serving_sync/jobs`로 바꿔주면 된다
(DAG의 `_bash` 헬퍼가 실제로 구성하는 값과 동일).

## 테스트

```bash
cd pipeline/serving_sync
PYTHONPATH=jobs pytest tests/ -v
```

`PYTHONPATH=jobs`가 없으면 `tests/`의 각 테스트 파일이 쓰는 `from build_mart_bike_risk_daily
import ...`류의 임포트가 실패한다 (jobs 모듈들이 `jobs.X`가 아니라 `X`로 서로를 참조하기
때문 - `python -m jobs.X` 단독 실행/Airflow BashOperator 양쪽에서 그대로 동작하게 하려는
설계).

**로컬 PySpark 드라이버/워커 파이썬 버전 불일치 주의**: 이 저장소는 `ingestion/.venv`를
pytest 실행에 쓰는데, 로컬 시스템 파이썬 버전과 그 venv의 파이썬 버전이 다르면(예: 시스템
3.12, venv 3.11) PySpark가 드라이버-워커 버전 불일치로 조용히 죽거나 이상한 에러를 낸다.
`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`을 명시적으로 venv의 인터프리터로 고정하면 해결된다:

```bash
cd pipeline/serving_sync
PYSPARK_PYTHON=$(pwd)/../../ingestion/.venv/bin/python \
PYSPARK_DRIVER_PYTHON=$(pwd)/../../ingestion/.venv/bin/python \
PYTHONPATH=jobs \
../../ingestion/.venv/bin/python -m pytest tests/ -v
```
