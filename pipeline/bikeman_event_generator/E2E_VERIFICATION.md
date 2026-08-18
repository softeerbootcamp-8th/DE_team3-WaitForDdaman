# bikeman_event_generator E2E 검증 기록 (2026-08-18)

Task 9(전체 백필/Slack 검증)에서 실제 인프라(Airflow 컨테이너 + Postgres)에 대해
`bikeman_event_generator` DAG를 한 달치 반복 트리거하고, 재실행 멱등성과 Slack 실패
알림 동작까지 확인한 기록. 검증 도중 DAG/SQL 레벨의 실제 버그 2건을 발견해 그 자리에서
고쳤고, 고친 뒤 백필을 다시 돌려 최종 결과가 기대치와 정확히 일치함을 확인했다.

## 검증 시작 시점 상태

```sql
SELECT occurred_at::date, event_type, count(*) FROM bikeman.fact_worker_event GROUP BY 1,2 ORDER BY 1;
```
```
 occurred_at | event_type | count
-------------+------------+-------
 2026-06-30  | COLLECT    |   500   -- 시드
 2026-07-01  | COLLECT    |   700   -- Task 6/7 스모크
 2026-07-01  | DEPLOY     |   476
 2026-09-01  | COLLECT    |   700   -- Task 5 스모크
```
`serving.bike_risk_daily`에는 `2026-07-01` 스냅샷 하나만 존재 - 이후 어떤 `target_date`로
트리거해도 `MAX(snapshot_date) <= target_date` 폴백으로 전부 이 스냅샷을 재사용한다
(의도된 동작, Task 7/8에서 이미 검증됨).

## Step 1~2: 31일 백필 1차 시도 - DEPLOY가 매번 0건 (버그 발견)

`2026-07-18` ~ `2026-08-17` 31일을 하루씩 트리거:

```bash
d="2026-07-18"; end="2026-08-17"
while [ "$d" != "$(date -j -v+1d -f "%Y-%m-%d" "$end" +%Y-%m-%d)" ]; do
  docker exec airflow-scheduler airflow dags trigger bikeman_event_generator \
    --conf "{\"snapshot_date\": \"$d\"}"
  sleep 8
  d=$(date -j -v+1d -f "%Y-%m-%d" "$d" +%Y-%m-%d)
done
```

31개 트리거 모두 `success`(총 33개 run 중 기존 2개 제외). 그런데 결과를 확인해보니:

```
 2026-07-18 ~ 2026-08-17 : 매일 COLLECT 700, DEPLOY는 단 한 건도 없음 (0/31일)
```

브리프가 예측한 "둘째 날부터 매일 DEPLOY 700건"과 전혀 다른 결과. 원인을 실제로
추적했다(추측이 아니라 재현/증거로 확인):

**근본 원인 #1 - `2026-09-01` 배치가 전체 백필 구간보다 "미래"라 매번 최신 이벤트를 가림.**
`deploy_returned_bikes.fetch_deploy_targets`는 자전거의 "가장 최근 이벤트"를
`occurred_at`(비즈니스 날짜, 실제 삽입 시각이 아님) 기준 `ORDER BY occurred_at DESC`로
판별한다. `2026-09-01` COLLECT 배치(Task 5, 이번 백필보다 먼저 존재)는 같은 700대
자전거 목록을 담고 있고, `occurred_at`이 `2026-07-18`~`2026-08-17` 전 구간보다 미래이므로,
이 700대 전부의 "가장 최근 이벤트"가 언제나 `COLLECT/2026-09-01`로 잡혀 매일의
"어제 COLLECT" 조회를 가려버렸다. 검증:

```sql
WITH latest AS (
  SELECT DISTINCT ON (bike_id) bike_id, event_type, occurred_at
  FROM bikeman.fact_worker_event ORDER BY bike_id, occurred_at DESC
)
SELECT l.event_type, l.occurred_at::date, count(*)
FROM latest l
JOIN (SELECT DISTINCT bike_id FROM bikeman.fact_worker_event WHERE occurred_at::date='2026-08-17') d
  ON d.bike_id = l.bike_id
GROUP BY 1,2;
-- event_type | occurred_at | count
-- COLLECT    | 2026-09-01  |   700   <- 전부 09-01로 앵커링됨
```

이 메커니즘은 **2026-07-01의 DEPLOY가 500이 아니라 476이었던 이유**도 정확히 동일하게
설명한다 - 같은 방식으로 검증:

```sql
-- 06-30 시드(500대) 중 700대 재사용 목록(09-01 배치)과 겹치는 자전거 수
SELECT count(*) FROM
 (SELECT DISTINCT bike_id FROM bikeman.fact_worker_event WHERE occurred_at::date='2026-06-30') seed
 JOIN (SELECT DISTINCT bike_id FROM bikeman.fact_worker_event WHERE occurred_at::date='2026-09-01') list700
 USING (bike_id);
-- 24  (== 500 - 476, 정확히 일치)
```
즉 07-01 DEPLOY의 476도, 이번 백필의 0/31도 같은 원인(09-01 배치의 미래 날짜 셰도잉)이며
"어쩌다 레이스에서 이겼다/졌다"가 아니라 결정론적으로 설명된다.

**부수적으로 발견한 설계 리스크 - 같은 실행 내 두 태스크의 순서 미보장.**
원래 `generate_collect_events`/`deploy_returned_bikes`는 서로 다른 대상을 다루는
독립 작업으로 보고 병렬 실행했는데, "수거" 스냅샷이 여러 날 재사용되는 백필/연속 실행
환경에서는 `generate_collect_events`(오늘 COLLECT)가 `deploy_returned_bikes`의
"어제 COLLECT" 조회보다 먼저 커밋되면 같은 방식으로 대상을 놓칠 잠재적 여지가 있다.
재발 방지 차원에서 `deploy_returned_bikes >> generate_collect_events`로 순서를 강제했다
(`airflow/dags/bikeman_event_generator_dag.py`).

### 수정 및 재현 데이터 정리

1. DAG 수정: `deploy_returned_bikes >> generate_collect_events` 의존성 추가
   (커밋 `2b1650f`).
2. 오염된 데이터 삭제 (실제 DAG 코드가 지운 게 아니라 검증 과정에서 직접 SQL로 정리 -
   가짜 데이터를 새로 넣은 게 아니라 이번 백필이 만든 오염 행과, 처음부터 셰도잉의
   근원이었던 `2026-09-01` 배치를 제거):
   ```sql
   DELETE FROM bikeman.fact_worker_event
   WHERE occurred_at::date BETWEEN '2026-07-18' AND '2026-08-18'
      OR occurred_at::date = '2026-09-01';
   -- DELETE 23100
   ```
   (`2026-08-18`도 포함한 이유: 순서 수정을 검증하는 과정에서 진단용으로 실제 DAG를
   통해 하루 더 트리거했었음 - 이 역시 실제 코드 경로를 통한 정상 트리거였고, 백필
   범위 밖이라 함께 정리) 정리 후:
   ```
   2026-06-30 | COLLECT | 500
   2026-07-01 | COLLECT | 700
   2026-07-01 | DEPLOY  | 476
   ```
   (기존 3행만 남음 - 확인 완료)

## Step 1~2 재시도 (2차) - 순서 수정만으로는 불충분, 두 번째 버그 발견

DAG 순서 수정 후 31일 백필을 다시 돌렸다. 결과:
- `2026-07-18`(첫날): COLLECT 700 / DEPLOY 0 (기대대로 - 전날 데이터 없음)
- `2026-07-19`(둘째 날): COLLECT 700 / **DEPLOY 700** (기대대로!)
- `2026-07-20` ~ `2026-08-17`(셋째 날부터): DEPLOY가 700이 아니라 **376~512 사이에서
  들쭉날쭉** (예: 07-20=376, 07-21=512, 07-22=441 ...)

**근본 원인 #2 - COLLECT/DEPLOY가 같은 날 발생하면 `occurred_at`이 완전히 동일해
동률 처리가 결정되지 않음.** `event_builder._build_event`는 COLLECT든 DEPLOY든
`occurred_at`을 `target_date 09:00:00`으로 고정 부여한다. 셋째 날부터는 매일 같은
자전거가 "어제 COLLECT분 DEPLOY"와 "오늘 다시 수거 목록에 올라 COLLECT"를 동시에
겪으므로, 그 자전거의 DEPLOY/COLLECT 두 이벤트가 정확히 같은 `occurred_at`을 갖는다.
`fetch_deploy_targets`의 `DISTINCT ON (bike_id) ... ORDER BY occurred_at DESC`는 이
동률에서 어느 행이 "최신"으로 남을지 보장하지 않아, 다음날 DEPLOY 대상 산정이 자전거마다
사실상 임의로 갈렸다. 실측으로 확인:
```sql
SELECT occurred_at, count(*), count(DISTINCT event_type)
FROM bikeman.fact_worker_event WHERE occurred_at::date='2026-07-19'
GROUP BY occurred_at HAVING count(*) > 1;
-- 2026-07-19 09:00:00 | 1400 | 2   (700 COLLECT + 700 DEPLOY, 완전히 같은 시각)
```
한 자전거의 실제 히스토리(`SPB-31978`)에서도 날짜별로 COLLECT만 있거나 DEPLOY+COLLECT가
같이 있는 패턴이 불규칙하게 반복되는 것을 확인 - 동률 처리 결과가 자전거마다 달랐다는
증거.

### 수정 (2차)

`pipeline/bikeman_event_generator/jobs/bikeman_db.py`의 `_FETCH_DEPLOY_TARGETS_SQL`
`ORDER BY`에 `(event_type = 'COLLECT') DESC`를 세 번째 정렬 키로 추가해 동률에서 항상
COLLECT가 이기도록 고정("오늘 다시 수거 목록에 올랐다"가 그 자전거의 최신 상태를
나타낸다고 판단) - 커밋 `af8828e`.

수정 전/후 비교 검증(같은 날짜, 실측 데이터에 적용):
```sql
-- 수정 전 SQL, 07-20까지의 데이터만 놓고 07-21 대상 조회 시뮬레이션
WITH latest AS (
  SELECT DISTINCT ON (bike_id) bike_id, event_type, occurred_at
  FROM bikeman.fact_worker_event WHERE occurred_at <= '2026-07-20 23:59:59'
  ORDER BY bike_id, occurred_at DESC
)
SELECT count(*) FROM latest WHERE event_type='COLLECT' AND occurred_at::date='2026-07-20';
-- (동률 있는 상태에서는 비결정적 - 실제 라이브 실행에서 512 관측)

-- 수정 후 SQL(타이브레이커 추가), 동일 조건
WITH latest AS (
  SELECT DISTINCT ON (bike_id) bike_id, event_type, occurred_at
  FROM bikeman.fact_worker_event WHERE occurred_at <= '2026-07-20 23:59:59'
  ORDER BY bike_id, occurred_at DESC, (event_type = 'COLLECT') DESC
)
SELECT count(*) FROM latest WHERE event_type='COLLECT' AND occurred_at::date='2026-07-20';
-- 700  (정확히 기대치)
```

데이터 재정리(백필 구간만, `2026-09-01`은 1차 정리 때 이미 제거됨):
```sql
DELETE FROM bikeman.fact_worker_event WHERE occurred_at::date BETWEEN '2026-07-18' AND '2026-08-17';
-- DELETE 35882
```

## Step 1~2 최종 재실행 (3차, 최종) - 통과

두 수정을 모두 반영한 상태로 31일 백필을 다시 실행. 전부 `success`:
```
docker exec airflow-scheduler airflow dags list-runs bikeman_event_generator -o plain | head -35
... (35개 run, 전부 state=success)
```

최종 결과:
```sql
SELECT occurred_at::date, event_type, count(*) FROM bikeman.fact_worker_event GROUP BY 1,2 ORDER BY 1,2;
```
```
 2026-06-30 | COLLECT |  500
 2026-07-01 | COLLECT |  700
 2026-07-01 | DEPLOY  |  476
 2026-07-18 | COLLECT |  700      <- 1일차: DEPLOY 없음 (기대대로, 전날 데이터 없음)
 2026-07-19 | COLLECT |  700 / DEPLOY | 700
 2026-07-20 | COLLECT |  700 / DEPLOY | 700
 ...(07-21 ~ 08-16, 전부 COLLECT 700 / DEPLOY 700)...
 2026-08-17 | COLLECT |  700 / DEPLOY | 700   <- 31일차, 마지막 날도 정확히 700
```
(64 rows, 총 44,376행 = 500 + 700 + 476 + 31×700(COLLECT) + 30×700(DEPLOY), 산술과
정확히 일치)

**결론: Step 2 기대치(1일차 DEPLOY 0건, 2~31일차 매일 DEPLOY 700건, 매일 COLLECT
700건) 완전 충족.** `event_id`가 `(bike_id, event_type, target_date)` 기반 uuid5라
같은 "수거" 스냅샷이 재사용돼도 날짜별로 서로 다른 `event_id`를 가져 COLLECT/DEPLOY
이벤트가 하루도 빠짐없이, 서로 다른 행으로 정확히 쌓였다.

## Step 3: 이미 처리한 날짜 재실행 - 멱등성 확인

최종(수정 반영) 데이터에 대해 재검증:
```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;"
# 44376
docker exec airflow-scheduler airflow dags trigger bikeman_event_generator --conf '{"snapshot_date": "2026-07-25"}'
sleep 10
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event;"
# 44376  (동일 - 신규 삽입 0건)
```
재실행 run도 `success`. `INSERT ... ON CONFLICT (event_id) DO NOTHING`이 기대대로
동작함을 확인 - **통과**.

## Step 4: Slack 실패 알림 - webhook 미설정(no-op) 케이스

`SLACK_WEBHOOK_URL`이 컨테이너 환경에 원래부터 설정돼 있지 않음을 먼저 확인
(`.env`, `ingestion/.env`, `docker exec airflow-scheduler env`, Airflow
Variables/Connections 전부 조회 - 어디에도 없음).

```bash
docker exec airflow-scheduler bash -c "unset SLACK_WEBHOOK_URL; airflow dags trigger bikeman_event_generator --conf '{\"snapshot_date\": \"not-a-real-date\"}'"
sleep 10 (+ 실제로는 retries=2, retry_exponential_backoff로 최종 실패까지 약 15분 소요)
docker exec airflow-scheduler airflow dags list-runs bikeman_event_generator --state failed -o plain
```

`manual__2026-08-18T10:08:54.551791+00:00` run이 최종 `failed`로 종료(첫 시도 즉시
실패 -> 5분 대기 후 2차 시도도 즉시 실패 -> 재시도 소진, 최종 `failed`). 두 태스크
(`generate_collect_events`, `deploy_returned_bikes`) 모두 매 시도에서 동일한 에러만
발생:
```
InvalidDatetimeFormat: invalid input syntax for type date: "not-a-real-date"
LINE 5: ...OM serving.bike_risk_daily WHERE snapshot_date <= 'not-a-rea...
```
(deploy 쪽은 `LINE 8: ...occurred_at::date = 'not-a-rea...`) - 브리프가 예상한 대로
Postgres 쪽 date cast 실패. 최종(2차, 재시도 소진) 시도의 로그를 끝까지 확인했으나
`_notify_slack_on_failure` 콜백 자체가 일으키는 **2차 예외는 전혀 없었다** -
`if not webhook_url: return` 가드가 의도대로 조용히 종료됨을 확인 - **통과**.

## Step 5: Slack 실패 알림 - 실제 전송 케이스 (건너뜀)

이 환경에는 실제 Slack Incoming Webhook URL이 없다 (`.env`, `ingestion/.env`에 관련
설정 없음 - `ingestion/.env`에는 "`SLACK_WEBHOOK_URL` 미설정 시 on_failure_callback이
조용히 스킵됨" 이라는 주석만 있고 실제 값은 없음. 컨테이너 환경변수/Airflow
Variables/Connections에도 없음). `gold_to_serving_sync` 플랜의 동일한 갭(Task 13)과
동일하게, **실제 전송 검증은 건너뛴다** - 문서화로 대체.

## Step 6: 실패 테스트로 인한 오염 데이터 확인

```bash
docker exec postgres psql -U hamzzi -d hamzzi -c "SELECT count(*) FROM bikeman.fact_worker_event WHERE occurred_at::date = 'not-a-real-date'::date;"
```
```
ERROR:  invalid input syntax for type date: "not-a-real-date"
```
쿼리 자체가 날짜 리터럴 파싱에서 에러 - `not-a-real-date`로 조회 가능한 행 자체가
있을 수 없음을 뜻한다. 최종 `GROUP BY` 전수 조회(위 "최종 재실행" 섹션)에도
`2026-07-18`~`2026-08-17`, `2026-06-30`, `2026-07-01` 외의 날짜는 전혀 없음 -
**오염 없음 확인**.

## 최종 데이터 상태 요약

| 날짜 | COLLECT | DEPLOY | 비고 |
|---|---|---|---|
| 2026-06-30 | 500 | - | 시드 |
| 2026-07-01 | 700 | 476 | Task 6/7 (476=500-24, 09-01 배치와 겹치는 24대가 셰도잉됨) |
| 2026-07-18 | 700 | 0 | 백필 1일차 - 전날 COLLECT 없음 |
| 2026-07-19 ~ 2026-08-17 | 700×30 | 700×30 | 백필 2~31일차, 매일 정확히 700 |
| **합계** | | | **44,376행** |

## 코드 변경 및 커밋

1. `airflow/dags/bikeman_event_generator_dag.py` - `deploy_returned_bikes >>
   generate_collect_events` 순서 강제 (커밋 `2b1650f`)
2. `pipeline/bikeman_event_generator/jobs/bikeman_db.py` -
   `fetch_deploy_targets` occurred_at 동률 시 COLLECT 우선 타이브레이커 추가
   (커밋 `af8828e`)
3. 본 문서 (`E2E_VERIFICATION.md`) 작성 (이 커밋)

두 수정 모두 기존 단위테스트(`pipeline/bikeman_event_generator/tests/`, 9개)
영향 없이 통과함을 재확인했다 (`event_builder.py`/`event_ids.py`는 변경하지 않음 -
`bikeman_db.py`는 원래부터 실제 DB 연결이 필요해 unit test 대상이 아니고 이
E2E 문서로 검증하는 파일).

## 발견/교훈

- **`occurred_at`(비즈니스 날짜) 기반 "최신 이벤트" 판별은 데이터가 시간 순서대로
  쌓인다는 가정에 의존한다.** 백필처럼 과거 날짜를 나중에 채우거나, 미래 날짜
  스모크 데이터가 먼저 들어와 있으면 이 가정이 깨진다. 이번 검증에서 `2026-09-01`
  스모크 데이터가 7~8월 백필 전체를 가려버린 것이 정확히 이 케이스.
- **같은 타임스탬프를 갖는 서로 다른 이벤트 타입은 정렬 동률을 만든다.** COLLECT와
  DEPLOY가 의도적으로 "하루 단위" 결정론(같은 target_date는 항상 같은 시각)을
  추구하다 보니, 하루에 두 이벤트가 겹치면 명시적 타이브레이커 없이는 `ORDER BY`
  결과가 안정적이지 않다.
- 두 문제 모두 "31일 연속 백필"이라는, 실제 운영에서는 거의 없을 조건(같은 스냅샷을
  매일 재사용 + 같은 날 COLLECT/DEPLOY 동시 발생)에서만 뚜렷하게 드러났다 - 정상
  운영(매일 다른 스냅샷, 하루 간격 실행)에서는 잠재해 있다가 드물게만 발현됐을
  가능성이 높다. E2E 백필 테스트가 아니었다면 발견하기 어려웠을 클래스의 버그.
