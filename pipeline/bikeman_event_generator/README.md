# bikeman_event_generator

`gold_to_serving_sync`가 `serving.bike_risk_daily`를 갱신한 직후, 그 결과(action='수거')를
근거로 bikeman(현장 작업자)의 수거/배치 행동을 시뮬레이션해 `bikeman.fact_worker_event`에
이벤트를 적재하는 파이프라인. 이 이벤트는 이후 `ingestion/jobs/daily_batch_bikeman_event.py`가
다시 읽어 Bronze로 수집하고, 위험도 모델의 레이블/피처로 재사용된다 (피드백 루프).

## jobs

- `event_ids.py`: `(bike_id, event_type, target_date)` 기반 결정론적 UUID5 생성 (순수 함수)
- `event_builder.py`: COLLECT/DEPLOY 이벤트 dict 생성 - occurred_at/received_at은 고정 시각
  (`target_date` 09:00/09:15), worker_id는 20명 풀에서 호출부가 랜덤 배정 (순수 함수)
- `bikeman_db.py`: `serving.bike_risk_daily`/`bikeman.fact_worker_event` 조회·적재. psycopg2
  connection 객체를 인자로 받을 뿐 airflow를 import하지 않음 (DB 연결이 필요해 pytest
  대상이 아님 - E2E_VERIFICATION.md로 검증)
- `generate_collect_events.py`: 최신 `snapshot_date`(<= target_date) 기준 action='수거'
  자전거 전부에 COLLECT 이벤트 생성
- `deploy_returned_bikes.py`: 자전거별 가장 최근 이벤트가 COLLECT이고 그 발생일이
  `target_date`의 전날인 자전거에, 그 COLLECT 이벤트의 station_id로 DEPLOY 이벤트 생성

## DB 접속

이 파이프라인은 저장소의 다른 job들(psycopg2 + .env 직접 연결)과 다르게, Airflow
Connection `bikeman_postgres` + `PostgresHook`을 사용한다 (사용자 확정 사항). Connection
필드는 `docs/superpowers/specs/2026-08-18-bikeman-event-generator-design.md` 참고.

**이 Connection은 docker-compose/초기화 스크립트 어디에도 자동 생성되지 않는다** - 새
환경에서는 아래처럼 직접 등록해야 한다 (Airflow UI: Admin → Connections에서 등록해도 동일):

```bash
docker exec airflow-scheduler airflow connections add bikeman_postgres \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema hamzzi \
  --conn-login bikeman_writer \
  --conn-password bikeman_writer_pw \
  --conn-port 5432
```

(`bikeman_writer` 롤/비밀번호는 `sql/bike_man/bikeman_seed_init.sql`에서 생성되므로, 그
스크립트를 먼저 실행해야 한다.) 등록 안 하고 DAG를 트리거하면 태스크가
`AirflowNotFoundException: The conn_id 'bikeman_postgres' isn't defined`로 실패한다.

## Airflow

- DAG: `bikeman_event_generator` (`airflow/dags/bikeman_event_generator_dag.py`)
  - `gold_to_serving_sync`의 `verify_bike_risk_daily_sync` 직후
    `TriggerDagRunOperator(wait_for_completion=False)`로 트리거됨 (station_daily 브랜치와는 무관)
  - `deploy_returned_bikes >> generate_collect_events` 순서로 실행됨 (병렬 아님) - `fetch_deploy_targets`가
    이제 `occurred_at < target_date`로 날짜 경계를 두므로 필수는 아니지만, 방어적 안전장치로 유지한다.
    자세한 배경은 `E2E_VERIFICATION.md` 참고
  - `target_date`는 `dag_run.conf.get("snapshot_date") or ds`

## 로컬/컨테이너 실행

**주의**: `PostgresHook`이 쓰는 Airflow Connection은 실제 태스크 실행 컨텍스트(Execution
API) 안에서만 resolve된다 - 이 저장소의 다른 job들과 달리 `docker exec ... python3 -c
"..."`처럼 Airflow 밖에서 직접 `generate_collect_events.run(...)`/`deploy_returned_bikes.run(...)`을
호출하면 `AirflowNotFoundException: The conn_id 'bikeman_postgres' isn't defined`로 실패한다
(Task 5/6 개발 중 실측 확인). 반드시 실제 DAG 트리거로 실행해야 한다:

```bash
docker exec airflow-scheduler airflow dags trigger bikeman_event_generator \
  --conf '{"snapshot_date": "2026-07-01"}'
```

## 테스트

순수 로직(`event_ids.py`, `event_builder.py`)만 pytest 대상이다:

```bash
cd pipeline/bikeman_event_generator
PYTHONPATH=jobs pytest tests/ -v
```

`bikeman_db.py`/`generate_collect_events.py`/`deploy_returned_bikes.py`는 실제 DB 연결이
필요해 유닛테스트 대상이 아니다 - `E2E_VERIFICATION.md`의 라이브 스모크 테스트로 검증한다.
