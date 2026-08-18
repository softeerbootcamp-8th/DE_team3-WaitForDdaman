# bikeman_event_generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new Airflow DAG (`bikeman_event_generator`) that, triggered right after `gold_to_serving_sync` writes `serving.bike_risk_daily`, simulates bikeman (현장 작업자) COLLECT/DEPLOY events into `bikeman.fact_worker_event` — creating a feedback loop from Gold-mart decisions back into the raw event source the risk model eventually re-consumes.

**Architecture:** Two independent `PythonOperator` tasks (`generate_collect_events`, `deploy_returned_bikes`) run in parallel inside a new DAG, each calling a thin orchestration function in `pipeline/bikeman_event_generator/jobs/`. Those orchestration functions get a raw psycopg2 connection from a new Airflow Connection (`bikeman_postgres`, via `PostgresHook`) and delegate to pure/DB-layer helper modules (`event_ids.py`, `event_builder.py`, `bikeman_db.py`). `gold_to_serving_sync_dag.py` gets one new `TriggerDagRunOperator` task wired after `verify_bike_risk_daily_sync`.

**Tech Stack:** Python 3.11, Apache Airflow 3.3 (`airflow.sdk` TaskFlow-less `@dag`, `PythonOperator`, `TriggerDagRunOperator`), `apache-airflow-providers-postgres` (`PostgresHook`, already present in the `airflow-scheduler` image — confirmed via `docker exec airflow-scheduler python -c "import airflow.providers.postgres"`), `psycopg2` (already a transitive dependency via the Postgres provider), `pytest` for pure-function unit tests.

## Global Constraints

- DB access for this DAG uses Airflow Connection `bikeman_postgres` + `PostgresHook` — an explicit, confirmed deviation from this repo's usual `psycopg2` + `.env` + `python -m jobs.X` standalone-execution convention. Do not "fix" this back to the usual pattern.
- New Postgres role `bikeman_writer` (least-privilege: `SELECT, INSERT` on `bikeman.fact_worker_event`, `SELECT` on `serving.bike_risk_daily`, nothing else) — do not reuse `airflow_reader` (read-only, would fail on INSERT) or the `hamzzi` superuser.
- `event_id` MUST be a deterministic `uuid5` derived from `(bike_id, event_type, target_date)` — never `uuid4`/random — so re-running the same `target_date` is a no-op via `ON CONFLICT (event_id) DO NOTHING`.
- `occurred_at` = `{target_date} 09:00:00`, `received_at` = `{target_date} 09:15:00` — fixed, not wall-clock `now()` or randomized-within-day.
- `worker_id` is randomly chosen (`random.choice`) from the pool `worker_0001`..`worker_0020` at insert time — this is the one intentionally non-deterministic field, per explicit user instruction.
- Follow this repo's established convention (confirmed via `pipeline/serving_sync`, `pipeline/collection_priority`, and the explicit removal of `pipeline/serving_sync/tests/test_serving_db.py` in commit `d05bddb`): **pytest unit tests cover pure/deterministic logic only.** Anything that opens a real DB connection (`PostgresHook`, `psycopg2`) is verified via a live smoke test against the real `postgres` container, documented in an `E2E_VERIFICATION.md`-style file — do not write mocked-connection pytest tests for `bikeman_db.py` or the `run()` orchestration functions.
- Korean literal strings must match exactly: action value `'수거'`, event types `'COLLECT'`/`'DEPLOY'` (already enforced by the `CHECK` constraint in `bikeman.fact_worker_event`).
- `bikeman.fact_worker_event` column order for INSERT: `event_id, event_type, bike_id, station_id, worker_id, occurred_at, received_at` (matches `CREATE TABLE` order in `sql/bike_man/bikeman_seed_init.sql`).
- All new DAG-related default_args/Slack-callback code mirrors `airflow/dags/gold_to_serving_sync_dag.py` verbatim (copied, not shared/extracted — matches this repo's per-DAG-independence convention).
- No PySpark anywhere in this feature — this is pure Python + Postgres, unlike the sibling `serving_sync`/`collection_priority` pipelines.

---

## File Structure

```
sql/bike_man/bikeman_seed_init.sql                          (modify: + bikeman_writer role)

pipeline/bikeman_event_generator/
  jobs/
    event_ids.py              # pure: deterministic uuid5 event_id
    event_builder.py           # pure: build COLLECT/DEPLOY event dicts
    bikeman_db.py               # DB layer: fetch_collect_targets/fetch_deploy_targets/insert_events (takes a raw psycopg2 conn — no airflow import)
    generate_collect_events.py  # orchestration: PostgresHook -> bikeman_db + event_builder
    deploy_returned_bikes.py    # orchestration: PostgresHook -> bikeman_db + event_builder
  tests/
    test_event_ids.py
    test_event_builder.py
  README.md
  E2E_VERIFICATION.md

airflow/dags/
  bikeman_event_generator_dag.py   (new)
  gold_to_serving_sync_dag.py       (modify: + trigger_bikeman_event_generator task)
```

`bikeman_db.py` deliberately never imports `airflow` — it only knows about a psycopg2-style connection object (duck-typed), so it can be pytest-imported in a plain Python environment (no Airflow install needed) even though it's not unit-tested itself (per the Global Constraints note above, only used for manual/live verification). Only `generate_collect_events.py`/`deploy_returned_bikes.py` import `PostgresHook`.

---

### Task 1: `bikeman_writer` Postgres role + `bikeman_postgres` Airflow Connection

**Files:**
- Modify: `sql/bike_man/bikeman_seed_init.sql` (append role block after the existing `airflow_reader` block, around line 33)

**Interfaces:**
- Produces: Postgres role `bikeman_writer` (password `bikeman_writer_pw`) with `SELECT, INSERT` on `bikeman.fact_worker_event` and `SELECT` on `serving.bike_risk_daily`. Airflow Connection id `bikeman_postgres` pointing at this role. Every later task in this plan depends on both existing.

- [ ] **Step 1: Add the role/grants block to the seed SQL**

Open `sql/bike_man/bikeman_seed_init.sql` and locate the existing `airflow_reader` block (around lines 22-33):

```sql
-- Airflow가 bikeman을 조회만 하도록 최소권한 롤 (선택사항, 필요 없으면 이 블록 삭제 가능)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow_reader') THEN
        CREATE ROLE airflow_reader LOGIN PASSWORD 'airflow_reader_pw';
    END IF;
END
$$;
GRANT USAGE ON SCHEMA bikeman TO airflow_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA bikeman TO airflow_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA bikeman GRANT SELECT ON TABLES TO airflow_reader;
```

Immediately after it, insert:

```sql
-- bikeman_event_generator DAG 전용 최소권한 쓰기 롤. bikeman.fact_worker_event에는
-- SELECT(최근 이벤트 조회)+INSERT만, serving.bike_risk_daily에는 SELECT만 허용한다.
-- airflow_reader(읽기 전용)로는 이 DAG가 요구하는 INSERT를 할 수 없어 별도로 만든다.
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

- [ ] **Step 2: Apply the migration against the running Postgres container**

```bash
docker compose -f docker-compose.local.yml exec -T postgres psql -U hamzzi -d hamzzi < sql/bike_man/bikeman_seed_init.sql
```

Expected: script runs to completion with no errors (the pre-existing `500` seeded `COLLECT` rows will hit `ON CONFLICT (event_id) DO NOTHING` and be silently skipped — this is expected, not a failure).

- [ ] **Step 3: Verify the role and grants**

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "\du bikeman_writer"
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT grantee, table_schema, table_name, privilege_type FROM information_schema.role_table_grants WHERE grantee = 'bikeman_writer' ORDER BY 1,2,3,4;"
```

Expected second command's output (order may vary):
```
   grantee     | table_schema |     table_name      | privilege_type
----------------+--------------+----------------------+-----------------
bikeman_writer | bikeman      | fact_worker_event    | INSERT
bikeman_writer | bikeman      | fact_worker_event    | SELECT
bikeman_writer | serving      | bike_risk_daily      | SELECT
```

- [ ] **Step 4: Confirm re-running the migration is idempotent**

```bash
docker compose -f docker-compose.local.yml exec -T postgres psql -U hamzzi -d hamzzi < sql/bike_man/bikeman_seed_init.sql
```

Expected: no errors, no duplicate-role errors (the `DO $$ ... IF NOT EXISTS ...` guard prevents `CREATE ROLE` from erroring on the second run).

- [ ] **Step 5: Register the `bikeman_postgres` Airflow Connection**

This is the same effect as using Admin → Connections → "+" in the Airflow UI with the fields below; doing it via CLI here lets later tasks verify against a real connection without a manual UI step blocking automated testing. The user can inspect/edit it afterward in the UI at any time — same underlying record.

```bash
docker exec airflow-scheduler airflow connections add bikeman_postgres \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema hamzzi \
  --conn-login bikeman_writer \
  --conn-password bikeman_writer_pw \
  --conn-port 5432
```

Expected: `Successfully added connection with connection_id bikeman_postgres`

- [ ] **Step 6: Verify the connection actually works**

```bash
docker exec airflow-scheduler airflow connections get bikeman_postgres
docker exec airflow-scheduler python3 -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
conn = PostgresHook(postgres_conn_id='bikeman_postgres').get_conn()
with conn.cursor() as cur:
    cur.execute('SELECT COUNT(*) FROM bikeman.fact_worker_event')
    print('fact_worker_event rows:', cur.fetchone()[0])
    cur.execute(\"SELECT COUNT(*) FROM serving.bike_risk_daily WHERE action = '수거'\")
    print('수거 rows:', cur.fetchone()[0])
conn.close()
"
```

Expected: prints `fact_worker_event rows: 500` and `수거 rows: 700` (matches the row counts already confirmed live in this environment).

- [ ] **Step 7: Commit**

```bash
git add sql/bike_man/bikeman_seed_init.sql
git commit -m "feat: bikeman_event_generator용 bikeman_writer 최소권한 롤 추가"
```

---

### Task 2: `event_ids.py` — deterministic UUID5 event id

**Files:**
- Create: `pipeline/bikeman_event_generator/jobs/event_ids.py`
- Test: `pipeline/bikeman_event_generator/tests/test_event_ids.py`

**Interfaces:**
- Produces: `make_event_id(bike_id: str, event_type: str, target_date: str) -> uuid.UUID`. Used by Task 3's `event_builder.py`.

- [ ] **Step 1: Write the failing test**

Create `pipeline/bikeman_event_generator/tests/test_event_ids.py`:

```python
from event_ids import make_event_id


def test_same_inputs_produce_same_id():
    id1 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    id2 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    assert id1 == id2


def test_different_event_type_produces_different_id():
    collect_id = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    deploy_id = make_event_id("SPB-12345", "DEPLOY", "2026-08-18")
    assert collect_id != deploy_id


def test_different_date_produces_different_id():
    id1 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    id2 = make_event_id("SPB-12345", "COLLECT", "2026-08-19")
    assert id1 != id2


def test_different_bike_produces_different_id():
    id1 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    id2 = make_event_id("SPB-99999", "COLLECT", "2026-08-18")
    assert id1 != id2
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mkdir -p pipeline/bikeman_event_generator/jobs pipeline/bikeman_event_generator/tests
cd pipeline/bikeman_event_generator && PYTHONPATH=jobs pytest tests/test_event_ids.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'event_ids'`

- [ ] **Step 3: Write minimal implementation**

Create `pipeline/bikeman_event_generator/jobs/event_ids.py`:

```python
"""
결정론적 이벤트 UUID5 생성.

같은 (bike_id, event_type, target_date) 조합이면 몇 번을 재실행해도 항상 같은
event_id가 나온다 - bikeman.fact_worker_event.event_id(PK)에 대한
INSERT ... ON CONFLICT DO NOTHING과 결합해 재실행 시 중복 삽입을 막는다.
"""
import uuid

EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "bikeman.fact_worker_event")


def make_event_id(bike_id: str, event_type: str, target_date: str) -> uuid.UUID:
    return uuid.uuid5(EVENT_NAMESPACE, f"{bike_id}:{event_type}:{target_date}")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd pipeline/bikeman_event_generator && PYTHONPATH=jobs pytest tests/test_event_ids.py -v
```

Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add pipeline/bikeman_event_generator/jobs/event_ids.py pipeline/bikeman_event_generator/tests/test_event_ids.py
git commit -m "feat: bikeman_event_generator 결정론적 event_id 생성 함수 추가"
```

---

### Task 3: `event_builder.py` — build COLLECT/DEPLOY event dicts

**Files:**
- Create: `pipeline/bikeman_event_generator/jobs/event_builder.py`
- Test: `pipeline/bikeman_event_generator/tests/test_event_builder.py`

**Interfaces:**
- Consumes: `event_ids.make_event_id(bike_id, event_type, target_date) -> uuid.UUID` (Task 2)
- Produces: `WORKER_POOL: list[str]` (20 entries, `worker_0001`..`worker_0020`); `build_collect_event(bike_id, station_id, target_date, worker_id) -> dict`; `build_deploy_event(bike_id, station_id, target_date, worker_id) -> dict`. Each dict has keys `event_id, event_type, bike_id, station_id, worker_id, occurred_at, received_at` (types: `str, str, str, str|None, str, datetime, datetime`). Used by Task 5/6's orchestration functions and Task 4's `bikeman_db.insert_events`.

- [ ] **Step 1: Write the failing test**

Create `pipeline/bikeman_event_generator/tests/test_event_builder.py`:

```python
from datetime import datetime

from event_builder import WORKER_POOL, build_collect_event, build_deploy_event
from event_ids import make_event_id


def test_worker_pool_has_20_workers_zero_padded():
    assert len(WORKER_POOL) == 20
    assert WORKER_POOL[0] == "worker_0001"
    assert WORKER_POOL[-1] == "worker_0020"


def test_build_collect_event_fields():
    event = build_collect_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0007")
    assert event["event_type"] == "COLLECT"
    assert event["bike_id"] == "SPB-12345"
    assert event["station_id"] == "ST-0001"
    assert event["worker_id"] == "worker_0007"
    assert event["occurred_at"] == datetime(2026, 8, 18, 9, 0, 0)
    assert event["received_at"] == datetime(2026, 8, 18, 9, 15, 0)
    assert event["event_id"] == str(make_event_id("SPB-12345", "COLLECT", "2026-08-18"))


def test_build_deploy_event_fields():
    event = build_deploy_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0007")
    assert event["event_type"] == "DEPLOY"
    assert event["event_id"] == str(make_event_id("SPB-12345", "DEPLOY", "2026-08-18"))


def test_build_collect_event_allows_null_station_id():
    event = build_collect_event("SPB-12345", None, "2026-08-18", "worker_0007")
    assert event["station_id"] is None


def test_worker_id_does_not_affect_event_id_or_timestamps():
    e1 = build_collect_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0001")
    e2 = build_collect_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0002")
    assert e1["event_id"] == e2["event_id"]
    assert e1["occurred_at"] == e2["occurred_at"]
    assert e1["received_at"] == e2["received_at"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd pipeline/bikeman_event_generator && PYTHONPATH=jobs pytest tests/test_event_builder.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'event_builder'`

- [ ] **Step 3: Write minimal implementation**

Create `pipeline/bikeman_event_generator/jobs/event_builder.py`:

```python
"""
COLLECT/DEPLOY 이벤트 dict 생성.

occurred_at/received_at은 완전히 결정론적인 고정 시각을 쓴다(target_date 09:00/09:15) -
시드 데이터처럼 하루 중 무작위 분산은 재실행 시 동일 결과를 보장해야 한다는 요구사항과
맞지 않는다. worker_id만 호출부(generate_collect_events.py/deploy_returned_bikes.py)에서
매 실행 랜덤으로 골라 넘긴다 - event_id가 이미 (bike_id, event_type, target_date) 기반
결정론적 키라 worker_id가 실행마다 달라져도 재실행 시 중복 삽입은 발생하지 않는다.
"""
from datetime import datetime, timedelta

from event_ids import make_event_id

WORKER_POOL = [f"worker_{i:04d}" for i in range(1, 21)]

OCCURRED_HOUR = 9
RECEIVED_DELAY_MINUTES = 15


def build_collect_event(bike_id: str, station_id: str | None, target_date: str, worker_id: str) -> dict:
    return _build_event(bike_id, "COLLECT", station_id, target_date, worker_id)


def build_deploy_event(bike_id: str, station_id: str | None, target_date: str, worker_id: str) -> dict:
    return _build_event(bike_id, "DEPLOY", station_id, target_date, worker_id)


def _build_event(bike_id: str, event_type: str, station_id: str | None, target_date: str, worker_id: str) -> dict:
    occurred_at = datetime.strptime(target_date, "%Y-%m-%d").replace(hour=OCCURRED_HOUR, minute=0, second=0)
    received_at = occurred_at + timedelta(minutes=RECEIVED_DELAY_MINUTES)
    return {
        "event_id": str(make_event_id(bike_id, event_type, target_date)),
        "event_type": event_type,
        "bike_id": bike_id,
        "station_id": station_id,
        "worker_id": worker_id,
        "occurred_at": occurred_at,
        "received_at": received_at,
    }
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd pipeline/bikeman_event_generator && PYTHONPATH=jobs pytest tests/test_event_builder.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add pipeline/bikeman_event_generator/jobs/event_builder.py pipeline/bikeman_event_generator/tests/test_event_builder.py
git commit -m "feat: bikeman_event_generator COLLECT/DEPLOY 이벤트 빌더 추가"
```

---

### Task 4: `bikeman_db.py` — DB layer (fetch + insert)

**Files:**
- Create: `pipeline/bikeman_event_generator/jobs/bikeman_db.py`

**Interfaces:**
- Consumes: a raw psycopg2-style connection (`conn`, e.g. from `PostgresHook(...).get_conn()`) — no `airflow` import in this file.
- Produces: `fetch_collect_targets(conn, target_date: str) -> list[dict]` (keys `bike_id`, `station_id`); `fetch_deploy_targets(conn, target_date: str) -> list[dict]` (same keys); `insert_events(conn, events: list[dict]) -> int` (returns actual inserted row count, excluding `ON CONFLICT` skips). Used by Task 5/6.
- No pytest unit test for this file — per Global Constraints, DB-touching code in this repo is verified live, not mocked (this task's Step 2 is a live smoke test instead of a failing-test cycle).

- [ ] **Step 1: Write the implementation**

Create `pipeline/bikeman_event_generator/jobs/bikeman_db.py`:

```python
"""
bikeman.fact_worker_event 조회/적재 DB 레이어.

이 모듈은 airflow를 import하지 않는다 - psycopg2 스타일 connection 객체(conn)를
인자로 받기만 한다(어디서 얻어왔는지는 모른다). 실제로는
generate_collect_events.py/deploy_returned_bikes.py가 PostgresHook(bikeman_postgres)
.get_conn()으로 얻은 연결을 넘겨준다.

이 파일은 실제 DB 연결이 필요해 pytest 유닛테스트 대상이 아니다(pipeline/serving_sync가
동일한 이유로 test_serving_db.py를 만들었다가 제거한 것과 같은 판단) - 대신
E2E_VERIFICATION.md의 라이브 스모크 테스트로 검증한다.
"""
import psycopg2.extras

SCHEMA = "bikeman"
TABLE = "fact_worker_event"
BATCH_SIZE = 500

EVENT_COLUMNS = ["event_id", "event_type", "bike_id", "station_id", "worker_id", "occurred_at", "received_at"]

_FETCH_COLLECT_TARGETS_SQL = """
    SELECT bike_id, station_id
    FROM serving.bike_risk_daily
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM serving.bike_risk_daily WHERE snapshot_date <= %(target_date)s
    )
    AND action = '수거'
"""

_FETCH_DEPLOY_TARGETS_SQL = """
    WITH latest AS (
        SELECT DISTINCT ON (bike_id) bike_id, event_type, station_id
        FROM bikeman.fact_worker_event
        ORDER BY bike_id, occurred_at DESC
    )
    SELECT bike_id, station_id FROM latest
    WHERE event_type = 'COLLECT' AND occurred_at::date = %(target_date)s::date - INTERVAL '1 day'
"""


def fetch_collect_targets(conn, target_date: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_COLLECT_TARGETS_SQL, {"target_date": target_date})
        return [dict(r) for r in cur.fetchall()]


def fetch_deploy_targets(conn, target_date: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_DEPLOY_TARGETS_SQL, {"target_date": target_date})
        return [dict(r) for r in cur.fetchall()]


def insert_events(conn, events: list[dict]) -> int:
    """event_id(PK) 충돌 시 스킵(ON CONFLICT DO NOTHING) - 반환값은 실제로 새로 삽입된
    행 수(충돌로 스킵된 행 제외). 동일 target_date로 재실행하면 0을 반환해야 정상."""
    if not events:
        return 0

    rows = [tuple(e[c] for c in EVENT_COLUMNS) for e in events]
    query = (
        f"INSERT INTO {SCHEMA}.{TABLE} ({', '.join(EVENT_COLUMNS)}) VALUES %s "
        "ON CONFLICT (event_id) DO NOTHING"
    )

    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            psycopg2.extras.execute_values(cur, query, rows[i : i + BATCH_SIZE], page_size=BATCH_SIZE)
            inserted += cur.rowcount
    conn.commit()
    return inserted
```

- [ ] **Step 2: Live smoke test against the real Postgres container**

```bash
docker exec airflow-scheduler python3 -c "
import sys
sys.path.insert(0, '/opt/airflow/pipeline/bikeman_event_generator/jobs')
from airflow.providers.postgres.hooks.postgres import PostgresHook
import bikeman_db

conn = PostgresHook(postgres_conn_id='bikeman_postgres').get_conn()

collect_targets = bikeman_db.fetch_collect_targets(conn, '2026-07-01')
print('collect targets:', len(collect_targets), collect_targets[:2])

deploy_targets = bikeman_db.fetch_deploy_targets(conn, '2026-07-01')
print('deploy targets (should be exactly the 500 seeded COLLECT bikes):', len(deploy_targets))

conn.close()
"
```

Expected: `collect targets: 700 [...]` (matches the `수거` row count confirmed in Task 1) and `deploy targets (should be exactly the 500 seeded COLLECT bikes): 500` (every seeded bike's *only* event is a `2026-06-30` `COLLECT`, so for `target_date=2026-07-01` all 500 qualify).

- [ ] **Step 3: Commit**

```bash
git add pipeline/bikeman_event_generator/jobs/bikeman_db.py
git commit -m "feat: bikeman_event_generator DB 조회/적재 레이어 추가"
```

---

### Task 5: `generate_collect_events.py` — orchestration

**Files:**
- Create: `pipeline/bikeman_event_generator/jobs/generate_collect_events.py`

**Interfaces:**
- Consumes: `bikeman_db.fetch_collect_targets`, `bikeman_db.insert_events` (Task 4); `event_builder.build_collect_event`, `event_builder.WORKER_POOL` (Task 3)
- Produces: `run(target_date: str) -> int` (returns count of newly-inserted rows). Called by Task 7's DAG.
- No pytest unit test (imports `airflow.providers.postgres`, same rationale as Task 4) — verified live.

- [ ] **Step 1: Write the implementation**

Create `pipeline/bikeman_event_generator/jobs/generate_collect_events.py`:

```python
"""
serving.bike_risk_daily에서 action='수거'인 자전거에 대해 COLLECT 이벤트를 생성한다.

사용법 (Airflow PythonOperator에서 호출됨, 단독 실행 시):
    python -c "import generate_collect_events; generate_collect_events.run('2026-07-01')"
"""
import logging
import random

from airflow.providers.postgres.hooks.postgres import PostgresHook

import bikeman_db
import event_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONN_ID = "bikeman_postgres"


def run(target_date: str) -> int:
    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    try:
        targets = bikeman_db.fetch_collect_targets(conn, target_date)
        events = [
            event_builder.build_collect_event(
                t["bike_id"], t["station_id"], target_date, random.choice(event_builder.WORKER_POOL)
            )
            for t in targets
        ]
        written = bikeman_db.insert_events(conn, events)
    finally:
        conn.close()

    logger.info("%s: COLLECT 대상 %d건 중 %d건 신규 삽입", target_date, len(targets), written)
    return written
```

- [ ] **Step 2: Live smoke test (first run inserts, second run is a no-op)**

Use `2026-09-01` here, not `2026-07-01` — 24 of the 500 seeded bikes are *also* in the `2026-07-01` `수거` list (confirmed live: `SELECT count(*) FROM bikeman.fact_worker_event seed JOIN serving.bike_risk_daily r ON r.bike_id = seed.bike_id WHERE seed.event_type='COLLECT' AND seed.occurred_at::date='2026-06-30' AND r.snapshot_date='2026-07-01' AND r.action='수거'` → `24`). If this smoke test ran on `2026-07-01`, it would give those 24 bikes a *newer* COLLECT event (dated `2026-07-01`), which would make them stop looking like "still-COLLECTed-from-6/30" — corrupting Task 6's clean cold-start test below. `2026-07-01` is deliberately left untouched until Task 6.

```bash
docker exec airflow-scheduler python3 -c "
import sys
sys.path.insert(0, '/opt/airflow/pipeline/bikeman_event_generator/jobs')
import generate_collect_events
print('first run inserted:', generate_collect_events.run('2026-09-01'))
print('second run inserted (must be 0):', generate_collect_events.run('2026-09-01'))
"
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT occurred_at::date, event_type, count(*) FROM bikeman.fact_worker_event GROUP BY 1,2 ORDER BY 1;"
```

Expected: `first run inserted: 700`, `second run inserted (must be 0): 0`, and the psql breakdown shows a new `2026-09-01 | COLLECT | 700` row alongside the untouched `2026-06-30 | COLLECT | 500`.

- [ ] **Step 3: Commit**

```bash
git add pipeline/bikeman_event_generator/jobs/generate_collect_events.py
git commit -m "feat: bikeman_event_generator COLLECT 이벤트 생성 job 추가"
```

---

### Task 6: `deploy_returned_bikes.py` — orchestration + README + E2E doc skeleton

**Files:**
- Create: `pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py`
- Create: `pipeline/bikeman_event_generator/README.md`
- Create: `pipeline/bikeman_event_generator/E2E_VERIFICATION.md`

**Interfaces:**
- Consumes: `bikeman_db.fetch_deploy_targets`, `bikeman_db.insert_events` (Task 4); `event_builder.build_deploy_event`, `event_builder.WORKER_POOL` (Task 3)
- Produces: `run(target_date: str) -> int`. Called by Task 7's DAG.

- [ ] **Step 1: Write the implementation**

Create `pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py`:

```python
"""
전날(target_date - 1일) COLLECT되고 아직 미배치인 자전거를 원래 station_id로 DEPLOY한다.
"미배치"는 별도 플래그가 아니라 "그 자전거의 가장 최근 이벤트가 COLLECT"라는 사실 자체로
정의된다 - 이미 DEPLOY됐다면 가장 최근 이벤트는 DEPLOY이므로 자동으로 제외된다
(bikeman_db.fetch_deploy_targets의 WITH latest ... 쿼리 참고).

사용법 (Airflow PythonOperator에서 호출됨, 단독 실행 시):
    python -c "import deploy_returned_bikes; deploy_returned_bikes.run('2026-07-01')"
"""
import logging
import random

from airflow.providers.postgres.hooks.postgres import PostgresHook

import bikeman_db
import event_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONN_ID = "bikeman_postgres"


def run(target_date: str) -> int:
    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    try:
        targets = bikeman_db.fetch_deploy_targets(conn, target_date)
        events = [
            event_builder.build_deploy_event(
                t["bike_id"], t["station_id"], target_date, random.choice(event_builder.WORKER_POOL)
            )
            for t in targets
        ]
        written = bikeman_db.insert_events(conn, events)
    finally:
        conn.close()

    logger.info("%s: DEPLOY 대상(전날 COLLECT & 미배치) %d건 중 %d건 신규 삽입", target_date, len(targets), written)
    return written
```

- [ ] **Step 2: Live smoke test — this is the cold-start cycle's first real execution**

`2026-07-01` is still untouched at this point (Task 5 deliberately used `2026-09-01` instead), so all 500 seeded bikes' latest event is still their original `2026-06-30` COLLECT — this is a clean test of the cold-start rule with no contamination from later COLLECT events.

```bash
docker exec airflow-scheduler python3 -c "
import sys
sys.path.insert(0, '/opt/airflow/pipeline/bikeman_event_generator/jobs')
import deploy_returned_bikes
print('first run inserted (expect exactly 500 - the 6/30 seed):', deploy_returned_bikes.run('2026-07-01'))
print('second run inserted (must be 0):', deploy_returned_bikes.run('2026-07-01'))
"
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT occurred_at::date, event_type, count(*) FROM bikeman.fact_worker_event GROUP BY 1,2 ORDER BY 1;"
```

Expected: `first run inserted (expect exactly 500 - the 6/30 seed): 500`, `second run inserted (must be 0): 0`, and psql shows a new `2026-07-01 | DEPLOY | 500` row. `2026-07-01` still has **no** `COLLECT` row yet at this point — `generate_collect_events` has never been run for that date (Task 5 used `2026-09-01`); that gap gets filled in Task 7.

- [ ] **Step 3: Write the README**

Create `pipeline/bikeman_event_generator/README.md`:

```markdown
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

## Airflow

- DAG: `bikeman_event_generator` (`airflow/dags/bikeman_event_generator_dag.py`)
  - `gold_to_serving_sync`의 `verify_bike_risk_daily_sync` 직후
    `TriggerDagRunOperator(wait_for_completion=False)`로 트리거됨 (station_daily 브랜치와는 무관)
  - `generate_collect_events`/`deploy_returned_bikes` 두 태스크는 서로 독립이라 병렬 실행
  - `target_date`는 `dag_run.conf.get("snapshot_date") or ds`

## 로컬/컨테이너 실행

```bash
docker exec airflow-scheduler python3 -c "
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
```

- [ ] **Step 4: Write the E2E verification doc skeleton**

Create `pipeline/bikeman_event_generator/E2E_VERIFICATION.md`:

```markdown
# bikeman_event_generator E2E 검증 기록

Task 9(전체 백필/Slack 검증)에서 실제로 실행한 명령과 결과를 여기에 기록한다.
아직 미실행 - Task 9에서 채운다.
```

- [ ] **Step 5: Commit**

```bash
git add pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py pipeline/bikeman_event_generator/README.md pipeline/bikeman_event_generator/E2E_VERIFICATION.md
git commit -m "feat: bikeman_event_generator DEPLOY 이벤트 생성 job + README 추가"
```

---

### Task 7: `bikeman_event_generator_dag.py` — the DAG itself

**Files:**
- Create: `airflow/dags/bikeman_event_generator_dag.py`

**Interfaces:**
- Consumes: `generate_collect_events.run(target_date: str)` (Task 5), `deploy_returned_bikes.run(target_date: str)` (Task 6)
- Produces: DAG id `bikeman_event_generator` with tasks `generate_collect_events`, `deploy_returned_bikes`. `trigger_dag_id` target for Task 8.

- [ ] **Step 1: Write the DAG file**

Create `airflow/dags/bikeman_event_generator_dag.py`:

```python
"""
bikeman_event_generator - Gold 마트 동기화 결과(serving.bike_risk_daily.action='수거')를
근거로 bikeman(현장 작업자)의 수거·배치 행동을 시뮬레이션해 bikeman.fact_worker_event에
이벤트를 적재한다. gold_to_serving_sync의 verify_bike_risk_daily_sync가 끝나면
TriggerDagRunOperator로 트리거된다 (station_daily 브랜치와는 무관 - 이 DAG가 읽는 건
bike_risk_daily뿐이라 그 완료를 기다리지 않는다).

generate_collect_events/deploy_returned_bikes 두 태스크는 서로 다른 event_type/자전거
집합을 다루는 독립 작업이라 병렬 실행한다 (gold_to_serving_sync의 bike_risk_daily/
station_daily 브랜치 병렬 설계와 동일한 이유).

### 왜 BashOperator가 아니라 PythonOperator + PostgresHook인가
이 저장소의 다른 모든 job은 psycopg2 + .env 직접 연결 + `python -m jobs.X` 단독 실행
컨벤션을 따르지만, 이 DAG는 사용자 확정에 따라 Airflow UI Connection(bikeman_postgres)
+ PostgresHook을 쓴다 (docs/superpowers/specs/2026-08-18-bikeman-event-generator-design.md 참고).
"""
import sys
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

JOBS_DIR = "/opt/airflow/pipeline/bikeman_event_generator/jobs"
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

import deploy_returned_bikes  # noqa: E402
import generate_collect_events  # noqa: E402

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _notify_slack_on_failure(context: dict) -> None:
    import os

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    ti = context["task_instance"]
    message = f":x: *{ti.dag_id}.{ti.task_id}* 실패\n실행일: {context['ds']}\n로그: {ti.log_url}"
    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except requests.RequestException:
        pass


default_args["on_failure_callback"] = _notify_slack_on_failure


def _target_date(context: dict) -> str:
    return context["dag_run"].conf.get("snapshot_date") or context["ds"]


def _run_generate_collect_events(**context) -> None:
    generate_collect_events.run(_target_date(context))


def _run_deploy_returned_bikes(**context) -> None:
    deploy_returned_bikes.run(_target_date(context))


@dag(
    dag_id="bikeman_event_generator",
    schedule=None,  # gold_to_serving_sync의 TriggerDagRunOperator로만 실행
    start_date=pendulum.datetime(2026, 8, 18, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["daily_batch", "bikeman"],
    doc_md=__doc__,
)
def bikeman_event_generator():
    PythonOperator(
        task_id="generate_collect_events",
        python_callable=_run_generate_collect_events,
        execution_timeout=timedelta(minutes=10),
    )
    PythonOperator(
        task_id="deploy_returned_bikes",
        python_callable=_run_deploy_returned_bikes,
        execution_timeout=timedelta(minutes=10),
    )


bikeman_event_generator()
```

- [ ] **Step 2: Verify the DAG parses with no import errors**

```bash
docker exec airflow-scheduler airflow dags list-import-errors
```

Expected: output does not include `bikeman_event_generator_dag.py` (empty table, or a table with other unrelated pre-existing entries only).

```bash
docker exec airflow-scheduler airflow dags list | grep bikeman_event_generator
```

Expected: a row for `bikeman_event_generator`.

- [ ] **Step 3: Trigger a real, isolated run of just this DAG**

`2026-07-01` has a `DEPLOY` row from Task 6 but **no** `COLLECT` row yet (Task 5's smoke test deliberately used `2026-09-01`). So this run is a mix: `generate_collect_events` does real, first-time work for this date; `deploy_returned_bikes` is a pure idempotency no-op (Task 6 already inserted its 500). This single trigger proves both the DAG wiring *and* idempotency in one shot:

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;" # note this number (call it N)
docker exec airflow-scheduler airflow dags trigger bikeman_event_generator --conf '{"snapshot_date": "2026-07-01"}'
```

Wait ~10-30s, then:

```bash
docker exec airflow-scheduler airflow dags list-runs -d bikeman_event_generator | head -5
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;" # expect N + 700
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT event_type, count(*) FROM bikeman.fact_worker_event WHERE occurred_at::date = '2026-07-01' GROUP BY 1;"
```

Expected: the latest run shows state `success` for both tasks; total row count increased by exactly `700` (the new `2026-07-01` COLLECT batch, `generate_collect_events`'s first real run for this date); the per-date breakdown for `2026-07-01` shows `COLLECT | 700` and `DEPLOY | 500` (the DEPLOY count unchanged from Task 6 — confirms `deploy_returned_bikes` correctly no-op'd).

- [ ] **Step 4: Commit**

```bash
git add airflow/dags/bikeman_event_generator_dag.py
git commit -m "feat: bikeman_event_generator DAG 추가"
```

---

### Task 8: Wire `trigger_bikeman_event_generator` into `gold_to_serving_sync_dag.py`

**Files:**
- Modify: `airflow/dags/gold_to_serving_sync_dag.py`

**Interfaces:**
- Consumes: DAG id `bikeman_event_generator` (Task 7)
- Produces: task `trigger_bikeman_event_generator` inside `gold_to_serving_sync`, wired after `verify_bike_risk_daily_sync`.

- [ ] **Step 1: Add the import**

In `airflow/dags/gold_to_serving_sync_dag.py`, add alongside the existing operator imports:

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
```

- [ ] **Step 2: Add the trigger task and wire it after `verify_bike_risk_daily_sync`**

Inside the `gold_to_serving_sync()` function body, after the `verify_bike_risk_daily_sync = BashOperator(...)` block and before the two `>>` dependency lines at the end, add:

```python
    trigger_bikeman_event_generator = TriggerDagRunOperator(
        task_id="trigger_bikeman_event_generator",
        trigger_dag_id="bikeman_event_generator",
        logical_date="{{ logical_date }}",
        conf={"snapshot_date": "{{ dag_run.conf.get(\"snapshot_date\") or ds }}"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
```

Then change the final dependency lines from:

```python
    build_mart_bike_risk_daily >> write_bike_risk_daily >> verify_bike_risk_daily_sync
    build_mart_station_daily >> write_station_daily >> verify_station_daily_sync
```

to:

```python
    build_mart_bike_risk_daily >> write_bike_risk_daily >> verify_bike_risk_daily_sync >> trigger_bikeman_event_generator
    build_mart_station_daily >> write_station_daily >> verify_station_daily_sync
```

(`station_daily` branch is untouched — `trigger_bikeman_event_generator` only depends on the `bike_risk_daily` branch.)

- [ ] **Step 3: Also update the docstring's discrepancy note (optional but keeps docs honest)**

At the top of the file's module docstring, after the existing paragraph about `wait_for_completion=False`, add one line:

```python
### trigger_bikeman_event_generator (2026-08-18 추가)
verify_bike_risk_daily_sync가 끝나면 bikeman_event_generator를 트리거한다
(station_daily 브랜치와는 무관 - 그 DAG가 읽는 건 bike_risk_daily뿐이라 완료를
기다리지 않는다). 세부 설계는
docs/superpowers/specs/2026-08-18-bikeman-event-generator-design.md 참고.
```

- [ ] **Step 4: Verify the DAG still parses**

```bash
docker exec airflow-scheduler airflow dags list-import-errors
```

Expected: no entry for `gold_to_serving_sync_dag.py`.

- [ ] **Step 5: Trigger a real run of the full chain and confirm the downstream DAG fires**

By this point `2026-07-01` already has both its `COLLECT` (700, from Task 7) and `DEPLOY` (500, from Task 6) rows, so this run of the *entire* upstream chain (real Spark jobs rebuilding the same `2026-07-01` mart, then cascading all the way down to `bikeman_event_generator`) should be a full idempotency no-op end to end:

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;" # note this number (call it N)
docker exec airflow-scheduler airflow dags trigger gold_to_serving_sync --conf '{"snapshot_date": "2026-07-01"}'
```

Wait for it to complete (poll every ~15s, this DAG's tasks run real Spark jobs so may take a few minutes):

```bash
docker exec airflow-scheduler airflow dags list-runs -d gold_to_serving_sync | head -3
docker exec airflow-scheduler airflow dags list-runs -d bikeman_event_generator | head -3
```

Expected: `gold_to_serving_sync`'s latest run is `success`, and a **new** `bikeman_event_generator` run appears (triggered, not the manual one from Task 7) also `success`.

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;" # expect still N (unchanged)
```

Expected: row count is exactly `N` (unchanged) — confirms both idempotency (nothing new gets inserted for a date already fully processed) and that the trigger wiring genuinely reaches `bikeman_event_generator` (the new DAG run in the list above is proof it fired at all).

- [ ] **Step 6: Commit**

```bash
git add airflow/dags/gold_to_serving_sync_dag.py
git commit -m "feat: gold_to_serving_sync 완료 후 bikeman_event_generator 트리거 추가"
```

---

### Task 9: Full backfill test + Slack failure notification check + E2E doc

**Files:**
- Modify: `pipeline/bikeman_event_generator/E2E_VERIFICATION.md` (fill in the skeleton from Task 6)

**Interfaces:**
- None (this is a verification-only task, no new interfaces produced).

- [ ] **Step 1: Manually backfill a full month, one day at a time**

`serving.bike_risk_daily` currently only has the `2026-07-01` snapshot, so every one of these will resolve to that same snapshot via the `MAX(snapshot_date) <= target_date` fallback — that's expected, not a bug (already exercised once in Task 7/8's smoke runs for `2026-07-01` itself). Loop through every single day for a month, from `2026-07-18` (a month before today, 2026-08-18) through `2026-08-17` (the day before the dates already covered by Tasks 5-8):

```bash
d="2026-07-18"
end="2026-08-17"
while [ "$d" != "$(date -I -d "$end + 1 day" 2>/dev/null || date -j -v+1d -f "%Y-%m-%d" "$end" +%Y-%m-%d)" ]; do
  echo "=== $d ==="
  docker exec airflow-scheduler airflow dags trigger bikeman_event_generator --conf "{\"snapshot_date\": \"$d\"}"
  sleep 8
  d=$(date -I -d "$d + 1 day" 2>/dev/null || date -j -v+1d -f "%Y-%m-%d" "$d" +%Y-%m-%d)
done
docker exec airflow-scheduler airflow dags list-runs -d bikeman_event_generator | head -35
```

(the `date -I -d` / `date -j -v+1d` fallback handles both GNU date (Linux/CI) and BSD date (macOS) — run whichever branch works on your shell). Expected: 31 triggered runs, every one `success`.

- [ ] **Step 2: Verify each backfilled date actually inserted distinct events (not silently deduped by mistake)**

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT occurred_at::date, event_type, count(*) FROM bikeman.fact_worker_event GROUP BY 1,2 ORDER BY 1;"
```

Expected: a row for `2026-06-30` (seed, 500 COLLECT), a row for `2026-09-01` (700 COLLECT, from Task 5), a row for `2026-07-01` (700 COLLECT + 500 DEPLOY, from Tasks 6/7), and then for the 31 backfilled dates (`2026-07-18`..`2026-08-17`, all resolving to the same reused `수거` snapshot so each date's COLLECT events are still distinct rows — different `target_date` in the `uuid5` input means different `event_id`s even though the underlying bike/station data is identical):
- every one of the 31 dates: `700 COLLECT`
- the **first** date (`2026-07-18`) only: **no** `DEPLOY` row (no `2026-07-17` COLLECT exists — there's a 16-day gap between `2026-07-01` and `2026-07-18` that this test deliberately skips, so "yesterday" has nothing to deploy)
- every date **from the second one on** (`2026-07-19` through `2026-08-17`, 30 dates): `700 DEPLOY` — because the loop is contiguous day-by-day, each day's 700 freshly-COLLECTed bikes become exactly the input for the *next* day's "COLLECT yesterday" check. This is the steady-state COLLECT-day-N → DEPLOY-day-N+1 cycle working continuously across the whole backfill window, not just the cold-start case from Task 6.

- [ ] **Step 3: Re-run one already-processed date and confirm zero new inserts**

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;" # note this number
docker exec airflow-scheduler airflow dags trigger bikeman_event_generator --conf '{"snapshot_date": "2026-07-25"}'
sleep 10
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;" # must be identical
```

Expected: the two counts match exactly.

- [ ] **Step 4: Verify Slack failure notification — unset case (no-op)**

```bash
docker exec airflow-scheduler bash -c "unset SLACK_WEBHOOK_URL; airflow dags trigger bikeman_event_generator --conf '{\"snapshot_date\": \"not-a-real-date\"}'"
sleep 10
docker exec airflow-scheduler airflow dags list-runs -d bikeman_event_generator --state failed | head -5
```

Expected: a `failed` run appears (invalid date breaks the SQL date cast inside Postgres). Open that run's task logs in the Airflow UI (Grid view → click the failed task → Logs) and confirm they show a psycopg2 date-parsing error — and confirm the run did **not** raise a second, unrelated exception from the failure callback itself (since `SLACK_WEBHOOK_URL` is unset, `_notify_slack_on_failure`'s `if not webhook_url: return` guard should make it a silent no-op; if the callback itself errors, that would show as an *additional* traceback in the same logs beyond the original SQL error).

- [ ] **Step 5: Verify Slack failure notification — configured case (real send)**

Requires a real Slack Incoming Webhook URL. If the user has one available:

```bash
docker exec -e SLACK_WEBHOOK_URL='<real webhook url>' airflow-scheduler \
  airflow dags trigger bikeman_event_generator --conf '{"snapshot_date": "not-a-real-date"}'
```

Expected: a message like `:x: *bikeman_event_generator.generate_collect_events* 실패...` appears in the target Slack channel. If no webhook URL is available in this environment, document that this step was skipped and why (mirrors how the sibling `gold_to_serving_sync` plan's Task 13 handled the same gap).

- [ ] **Step 6: Clean up the failed test run's bad data (if any got written before the failure)**

The `not-a-real-date` runs should fail before any INSERT happens (the date cast fails inside the SQL itself), so no cleanup should be needed — verify:

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event WHERE occurred_at::date = 'not-a-real-date'::date;" 2>&1
```

Expected: this itself errors (invalid date literal) or, if somehow parsed, returns `0`.

- [ ] **Step 7: Write up the results in `E2E_VERIFICATION.md`**

Replace the skeleton content in `pipeline/bikeman_event_generator/E2E_VERIFICATION.md` with the actual commands run and actual output observed in Steps 1-6 above (copy real terminal output, not placeholders).

- [ ] **Step 8: Commit**

```bash
git add pipeline/bikeman_event_generator/E2E_VERIFICATION.md
git commit -m "docs: bikeman_event_generator E2E 백필/Slack 검증 기록"
```

---

## Self-Review Notes

- **Spec coverage:** role/grants (Task 1), Connection (Task 1), `event_id` uuid5 (Task 2), event builder incl. fixed timestamps + worker pool (Task 3), `generate_collect_events` SQL/logic (Task 4/5), `deploy_returned_bikes` SQL/logic (Task 4/6), DAG structure (Task 7), `gold_to_serving_sync` trigger wiring (Task 8), month-long backfill + re-run idempotency + Slack check (Task 9) — every spec requirement maps to a task.
- **Placeholder scan:** no TBD/TODO; all code blocks are complete; Task 9 Step 5's Slack webhook is conditionally skippable with an explicit, non-vague fallback instruction (document-if-skipped), matching real precedent from this repo's sibling plan rather than being a vague placeholder.
- **Type/name consistency checked:** `EVENT_COLUMNS` order in `bikeman_db.py` (Task 4) matches the dict keys produced by `event_builder.py` (Task 3) and the `CREATE TABLE` column order in `bikeman_seed_init.sql`; `CONN_ID = "bikeman_postgres"` in Task 5/6 matches the Connection id registered in Task 1; `trigger_dag_id="bikeman_event_generator"` in Task 8 matches `dag_id="bikeman_event_generator"` in Task 7.
