# bikeman_event_generator — 설계 스펙

## 배경 / 목적

`gold_to_serving_sync` 완료 후, Gold 마트 동기화 결과(`serving.bike_risk_daily.action = '수거'`)를
근거로 bikeman(현장 작업자)의 수거·배치 행동을 시뮬레이션하는 이벤트 생성 DAG를 추가한다.
생성된 이벤트는 `bikeman.fact_worker_event`에 적재되어, 향후 위험도 모델의 레이블/피처로
재사용된다 (피드백 루프: 조치 수행 → 원천 이벤트로 회귀).

## 사전 조사에서 확인된 사실

- `gold_to_serving_sync_dag.py`, `serving.bike_risk_daily`(action 컬럼 포함)는 `feat/42/create-serving-dag`
  브랜치에서 개발되었고, PR #64로 `develop`에 병합 완료됨. 이 스펙 작성 시점에 `feat/65`를
  최신 `develop`으로 rebase하여 반영함.
- `serving.bike_risk_daily.action` 값은 `수거` / `대여중단` / `조치없음` 3가지
  (`pipeline/serving_sync/jobs/build_mart_bike_risk_daily.py`의 capacity 기반 분리 로직 산출물).
- `bikeman.fact_worker_event`는 `sql/bike_man/bikeman_seed_init.sql`에 정의되어 있고,
  현재 6/30 콜드스타트 시드 500건(전부 COLLECT, DEPLOY 없음)이 이미 적재되어 있음.
  헤더 주석에 "7/1 06:00 파이프라인이 '전날 수거 & 미배치 = 오늘 무조건 배치' 룰로
  이 500건을 자동 DEPLOY 처리하는 것이 정상 사이클의 첫 실행"이라고 명시되어 있어,
  본 DAG의 `deploy_returned_bikes` 로직이 그 첫 실행을 담당한다.
- 이 저장소의 다른 모든 Postgres job(`ingestion/common/db_client.py`, `pipeline/serving_sync/jobs/serving_db.py`)은
  Airflow `PostgresHook`을 쓰지 않고 `psycopg2` 직접 연결 + `.env` 변수 + `python -m jobs.X`
  단독 실행 컨벤션을 따른다. 이번 DAG는 **사용자 확정에 따라 이 컨벤션과 다르게**
  Airflow UI Connection(`bikeman_postgres`) + `PostgresHook`을 사용한다 (아래 "확정된 결정" 참고).
- 현재 실제 환경의 Postgres는 단일 컨테이너(`postgres`), DB명 `hamzzi`, 슈퍼유저 `hamzzi/hamzzi`.
  `bikeman` 스키마 전용 읽기 롤 `airflow_reader`(SELECT만 가능, 쓰기 원천 오염 방지 목적)가
  이미 존재함. 쓰기가 필요한 이번 DAG를 위해 별도 최소권한 쓰기 롤을 신설한다.
- `apache-airflow-providers-postgres`는 베이스 이미지에 이미 포함되어 있음 (`docker exec airflow-scheduler
  python -c "import airflow.providers.postgres"` 확인 완료) — 추가 의존성 설치 불필요.
- 현재 `serving.bike_risk_daily`에는 `snapshot_date = 2026-07-01` 스냅샷만 존재함(1일치).
  `generate_collect_events`의 `MAX(snapshot_date) <= target_date` 조회 조건은 이런 희소한
  마트 상태에서도 항상 "그 시점에 사용 가능한 가장 최신 스냅샷"을 재사용하도록 설계된 것.

## 확정된 결정 (사용자 확인 완료)

1. **브랜치**: `feat/65`를 병합된 `develop`(PR #64 포함)으로 rebase하여 `gold_to_serving_sync_dag.py`/
   `serving.bike_risk_daily` 등을 확보함.
2. **DB 접속 방식**: 작업 지시서대로 Airflow UI Connection(`bikeman_postgres`) + `PostgresHook`을
   사용한다. (저장소의 기존 psycopg2+.env 컨벤션과는 다른 방식임을 인지하고 의도적으로 선택함)
3. **쓰기 권한**: 신규 최소권한 롤 `bikeman_writer`를 생성한다 (`airflow` 슈퍼유저나 기존
   `SERVING_DB_*` 자격증명을 재사용하지 않음).
4. **코드 위치**: `pipeline/bikeman_event_generator/jobs/` 신규 디렉토리 (`pipeline/serving_sync/`와
   동일한 구조로, 이 기능만의 독립된 파이프라인 단계로 취급).
5. **트리거 시점**: `gold_to_serving_sync`의 `verify_bike_risk_daily_sync` 태스크 직후에만 건다.
   `station_daily` 브랜치는 이 DAG와 무관하므로 그 완료를 기다리지 않는다.
6. **Connection 필드값**: 작업 지시서의 예시값(Schema=airflow, Login=airflow)이 아니라 실제
   환경 값 + 신규 최소권한 롤을 사용한다 — Host=`postgres`, Schema=`hamzzi`, Login=`bikeman_writer`,
   Port=`5432`.
7. **이벤트 생성 로직**: 아래 "이벤트 생성 로직" 절 그대로 진행 (uuid5 결정론적 event_id,
   occurred_at/received_at 고정 시각, worker_id 매 실행 랜덤 배정).
8. **Slack 실패 알림**: `gold_to_serving_sync_dag.py`의 `_notify_slack_on_failure`를 그대로
   복사해서 이 DAG 파일에도 둔다 (공용 모듈로 추출하지 않음 — 각 DAG가 독립적이라는
   이 저장소의 기존 관례와 동일).
9. **테스트 계획**: 아래 "백필 / 테스트 계획" 절 그대로 진행.

## DB 스키마 변경

`sql/bike_man/bikeman_seed_init.sql`에 다음을 추가한다 (idempotent, 기존 `airflow_reader`
생성 블록과 동일한 패턴):

```sql
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bikeman_writer') THEN
        CREATE ROLE bikeman_writer LOGIN PASSWORD 'bikeman_writer_pw';
    END IF;
END
$$;
GRANT USAGE ON SCHEMA bikeman TO bikeman_writer;
GRANT SELECT, INSERT ON bikeman.fact_worker_event TO bikeman_writer;
GRANT USAGE ON SCHEMA serving TO bikeman_writer;
GRANT SELECT ON serving.bike_risk_daily TO bikeman_writer;
```

`bikeman_writer`는 `bikeman.fact_worker_event`에 SELECT(최근 이벤트 조회용)+INSERT만,
`serving.bike_risk_daily`에는 SELECT만 가능 — 그 외 권한 없음 (최소 권한 원칙, `airflow_reader`와
동일한 설계 사상).

## Airflow Connection

Airflow UI(Admin → Connections)에서 다음 값으로 등록:

| 필드 | 값 |
|---|---|
| Connection Id | `bikeman_postgres` |
| Connection Type | Postgres |
| Host | `postgres` |
| Schema | `hamzzi` |
| Login | `bikeman_writer` |
| Password | `bikeman_writer_pw` |
| Port | `5432` |

## 이벤트 생성 로직

### event_id (결정론적 UUID5)

```python
import uuid

EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "bikeman.fact_worker_event")

def make_event_id(bike_id: str, event_type: str, target_date: str) -> uuid.UUID:
    return uuid.uuid5(EVENT_NAMESPACE, f"{bike_id}:{event_type}:{target_date}")
```

같은 `(bike_id, event_type, target_date)`는 재실행해도 항상 같은 UUID를 생성 →
`INSERT ... ON CONFLICT (event_id) DO NOTHING`으로 재실행 시 중복 삽입을 막는다.

### occurred_at / received_at

완전 결정론적 고정 시각을 사용한다 (시드 데이터처럼 하루 중 무작위 분산은 두지 않음):
- `occurred_at` = `{target_date} 09:00:00`
- `received_at` = `{target_date} 09:15:00` (고정 15분 지연)

### worker_id

`worker_0001` ~ `worker_0020` 중 `random.choice()`로 매 실행 시 랜덤 배정한다 (작업 지시서
원문 그대로). event_id가 이미 결정론적 키이므로 재실행 시 신규 INSERT 자체가 스킵되어
worker 재배정 위험은 최초 삽입 실패/재시도 상황에서만 발생하며, 이는 허용 범위로 본다.

### generate_collect_events

```sql
SELECT bike_id, station_id
FROM serving.bike_risk_daily
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) FROM serving.bike_risk_daily WHERE snapshot_date <= %(target_date)s
)
AND action = '수거'
```

각 행에 대해 `event_type='COLLECT'`, `station_id`=조회된 값(수거 위치)으로 INSERT.

### deploy_returned_bikes

```sql
WITH latest AS (
    SELECT DISTINCT ON (bike_id) bike_id, event_type, station_id
    FROM bikeman.fact_worker_event
    ORDER BY bike_id, occurred_at DESC
)
SELECT bike_id, station_id FROM latest
WHERE event_type = 'COLLECT' AND occurred_at::date = %(target_date)s::date - INTERVAL '1 day'
```

자전거별 가장 최근 이벤트가 COLLECT이고 그 발생일이 `target_date`의 전날인 경우만 대상.
해당 COLLECT 이벤트의 `station_id`(원래 대여소)로 `event_type='DEPLOY'` INSERT.

## DAG 구조 (`airflow/dags/bikeman_event_generator_dag.py`)

```python
@dag(
    dag_id="bikeman_event_generator",
    schedule=None,  # gold_to_serving_sync의 TriggerDagRunOperator로만 실행
    start_date=pendulum.datetime(2026, 8, 18, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,  # gold_to_serving_sync_dag.py와 동일 패턴 (retries=2,
                                 # retry_exponential_backoff, Slack on_failure_callback)
    tags=["daily_batch", "bikeman"],
)
def bikeman_event_generator():
    generate_collect_events = PythonOperator(...)  # PostgresHook("bikeman_postgres")
    deploy_returned_bikes = PythonOperator(...)     # PostgresHook("bikeman_postgres")
    # 서로 다른 event_type/자전거 집합을 다루는 독립 작업이라 병렬 실행
    # (gold_to_serving_sync의 bike_risk_daily/station_daily 브랜치 병렬 설계와 동일한 이유)
```

- `target_date` = `dag_run.conf.get("snapshot_date") or ds` (gold_to_serving_sync/dag_risk_decision과
  동일 컨벤션 — 트리거 체인 전체에서 날짜가 어긋나지 않게 함)
- 두 태스크는 서로 의존관계 없이 병렬 실행, DAG는 둘 다 성공해야 완료
- Slack 실패 알림은 `gold_to_serving_sync_dag.py`의 `_notify_slack_on_failure`를 그대로 복사

## gold_to_serving_sync_dag.py 수정

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

trigger_bikeman_event_generator = TriggerDagRunOperator(
    task_id="trigger_bikeman_event_generator",
    trigger_dag_id="bikeman_event_generator",
    logical_date="{{ logical_date }}",
    conf={"snapshot_date": "{{ dag_run.conf.get(\"snapshot_date\") or ds }}"},
    wait_for_completion=False,
    reset_dag_run=True,
)

build_mart_bike_risk_daily >> write_bike_risk_daily >> verify_bike_risk_daily_sync >> trigger_bikeman_event_generator
```

`station_daily` 브랜치와는 독립적으로 연결 (완료를 기다리지 않음).

## 백필 / 테스트 계획

1. `sql/bike_man/bikeman_seed_init.sql`에 `bikeman_writer` 롤 블록 추가 후 재실행 (idempotent)
2. Airflow UI에서 `bikeman_postgres` Connection 등록, Test 버튼으로 연결 확인
3. `bikeman_event_generator` DAG를 `conf={"snapshot_date": "YYYY-MM-DD"}`로 한 달 전
   날짜부터 하루씩 수동 트리거 — `MAX(snapshot_date) <= target_date` 조회 덕분에
   `serving.bike_risk_daily`가 `2026-07-01` 스냅샷만 있는 현재 상태에서도 그 이후 날짜들은
   전부 `2026-07-01` 스냅샷을 재사용하게 됨 (의도된 동작, 에러 아님)
4. 같은 날짜로 재실행 → `bikeman.fact_worker_event` row count 불변 확인 (`ON CONFLICT DO NOTHING` 검증)
5. `2026-07-01` 최초 실행 시 `deploy_returned_bikes`가 시드된 500건(6/30 COLLECT)을 전부
   DEPLOY 처리하는지 확인 (콜드스타트 사이클의 첫 실행 검증)
6. `SLACK_WEBHOOK_URL` 미설정 상태에서 태스크를 강제 실패시켜 콜백이 예외 없이 no-op하는지,
   설정 시 실제 Slack 알림이 전송되는지 확인

## 완료 조건 (작업 지시서 원문 유지)

- `bikeman_event_generator` DAG가 `gold_to_serving_sync` 완료 후 자동 트리거됨
- 최신 `snapshot_date` 하루치 수거 대상 자전거에 대해 COLLECT 이벤트가 정확히 생성됨
- 전날 COLLECT & 미배치 자전거에 대해 원래 `station_id`로 DEPLOY 이벤트가 생성됨
- 동일 날짜로 재실행해도 중복 삽입 없음 (`ON CONFLICT DO NOTHING` 검증)
- 한 달치 백필 수동 트리거 시 일자별로 정상 적재됨
- Slack 실패 알림 동작 확인
