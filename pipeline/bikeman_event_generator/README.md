# bikeman_event_generator

`gold_to_serving_sync`가 `serving.bike_risk_daily`를 갱신한 직후, bikeman(현장 작업자)의
수거/배치 행동을 시뮬레이션해 `bikeman.fact_worker_event`에 이벤트를 적재하는 파이프라인.
이 이벤트는 이후 `ingestion/jobs/daily_batch_bikeman_event.py`가 다시 읽어 Bronze로
수집하고, 위험도 모델의 레이블/피처로 재사용된다 (피드백 루프).

`serving.bike_risk_daily.action` 컬럼이 제거된 뒤에는 최신 snapshot의 `risk_score`
상위 500대를 COLLECT 대상으로 삼는다. DEPLOY 이벤트는 기존 `bikeman.fact_worker_event`
이력 기준으로 계속 생성된다.

## jobs

- `event_ids.py`: `(bike_id, event_type, target_date)` 기반 결정론적 UUID5 생성 (순수 함수)
- `event_builder.py`: COLLECT/DEPLOY 이벤트 dict 생성 - occurred_at/received_at은 고정 시각
  (`target_date` 09:00/09:15), worker_id는 20명 풀에서 호출부가 랜덤 배정 (순수 함수)
- `bikeman_db.py`: `serving.bike_risk_daily`/`bikeman.fact_worker_event` 조회·적재. psycopg2
  connection 객체를 인자로 받을 뿐 airflow를 import하지 않음 (DB 연결이 필요해 pytest
  대상이 아님 - E2E_VERIFICATION.md로 검증)
- `generate_collect_events.py`: 최신 `snapshot_date`(<= target_date) 기준 `risk_score`
  상위 N대에 COLLECT 이벤트 생성. N은 `BIKEMAN_COLLECT_LIMIT`로 조정할 수 있고 기본값은 500
- `deploy_returned_bikes.py`: 자전거별 가장 최근 이벤트가 COLLECT이고 그 발생일이
  `target_date`의 전날인 자전거에, 그 COLLECT 이벤트의 station_id로 DEPLOY 이벤트 생성

## DB 접속

이 파이프라인은 Lambda(`infra/lambdas/bikeman_event_generator`, #186)로 실행된다. Airflow
워커는 DB 자격증명을 갖지 않고 Lambda를 invoke만 한다. DB 접속 정보는 AWS Secrets
Manager에 저장되어 있고, Lambda 콜드 스타트 시 `infra/lambdas/bikeman_event_generator/
app/_secrets.py`의 `load_bikeman_db_secret()`이 그 시크릿을 읽어
`BIKEMAN_WRITER_DB_HOST/PORT/NAME/USER/PASSWORD` 환경변수를 채운 뒤 잡 모듈을 호출한다.
잡 코드(`generate_collect_events.py`/`deploy_returned_bikes.py`)는 이 환경변수들을
`os.environ`에서 직접 읽는 psycopg2 직접 연결 방식으로, 저장소의 다른 job들과 동일한
컨벤션을 따른다 (Airflow Connection/PostgresHook을 쓰지 않음).

## Airflow

- DAG: `bikeman_event_generator` (`airflow/dags/bikeman_event_generator_dag.py`)
  - `gold_to_serving_sync`의 `verify_bike_risk_daily_sync` 직후
    `TriggerDagRunOperator(wait_for_completion=False)`로 트리거됨 (station_daily 브랜치와는 무관)
  - `deploy_returned_bikes >> generate_collect_events` 순서로 실행됨 (병렬 아님) - `fetch_deploy_targets`가
    이제 `occurred_at < target_date`로 날짜 경계를 두므로 필수는 아니지만, 방어적 안전장치로 유지한다.
    자세한 배경은 `E2E_VERIFICATION.md` 참고
  - `target_date`는 `dag_run.conf.get("snapshot_date") or ds`

## 로컬/컨테이너 실행

`_connect()`가 `BIKEMAN_WRITER_DB_HOST/NAME/USER/PASSWORD`를 `os.environ`에서 직접
읽으므로(없으면 `KeyError`), 실행 전에 이 환경변수들을 채워야 한다:

```bash
docker exec -e BIKEMAN_WRITER_DB_HOST=... -e BIKEMAN_WRITER_DB_NAME=... \
  -e BIKEMAN_WRITER_DB_USER=... -e BIKEMAN_WRITER_DB_PASSWORD=... \
  airflow-scheduler python3 -c "
import sys
sys.path.insert(0, '/opt/airflow/pipeline/bikeman_event_generator/jobs')
import generate_collect_events, deploy_returned_bikes
deploy_returned_bikes.run('2026-07-01')
generate_collect_events.run('2026-07-01')
"
```

## 테스트

순수 로직(`event_ids.py`, `event_builder.py`)만 pytest 대상이다:

```bash
cd pipeline/bikeman_event_generator
PYTHONPATH=jobs pytest tests/ -v
```

`bikeman_db.py`/`generate_collect_events.py`/`deploy_returned_bikes.py`는 실제 DB 연결이
필요해 유닛테스트 대상이 아니다 - `E2E_VERIFICATION.md`의 라이브 스모크 테스트로 검증한다.
