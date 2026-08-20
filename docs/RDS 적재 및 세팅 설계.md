# RDS 적재 및 세팅 설계

- 범위: 체크리스트 "RDS 적재 및 세팅" 중 **RDS/DB 계층만**. EC2/ECS로의 컴퓨트 배포는
  별도 설계로 다룬다 (이 문서에서는 "같은 VPC 내부, private RDS"라는 네트워킹
  전제만 가져다 쓴다).
- 전제: 예산 제약 없음 - 아키텍처 정합성 우선으로 판단. `APP_ENV=aws`로 이미
  전환되어 S3는 실 AWS를 쓰는 중이고, RDS가 로컬 docker postgres로 남은 마지막
  조각이다.

## 1. 현재 상태

- 로컬 `docker-compose.yml`/`docker-compose.local.yml`은 `postgres:16` 컨테이너
  하나에 Airflow 메타데이터(`public` 스키마)와 도메인 데이터(`app`/`bikeman`/
  `serving` 스키마)를 함께 올린다.
- `sql/bike_man/bikeman_seed_init.sql`이 스키마(`app`/`bikeman`/`serving`)와
  최소권한 role(`airflow_reader`: bikeman 읽기 전용, `bikeman_writer`: bikeman
  쓰기 + serving 읽기)을 이미 정의해 idempotent하게 실행 가능한 상태다.
- 서비스 시작일이 2026-08-20이라 필요한 건 "서비스 시작 전날"인 **8/19
  하루치** `bikeman.fact_worker_event` 데이터다. `domain-db`가 준비되면
  `bikeman_event_generator`를 실제로 한 번 실행해 8/19치를 직접 만든다
  (4.2/6절 참고).
- `services/api`는 현재 `develop` 브랜치에는 존재하지 않는다 (`develop-services`
  브랜치로 이동됨, `2ea8ccd`). 이 문서에서는 다루지 않는다.

## 2. 물리적 분리 구조 (결정됨)

RDS 인스턴스 **2개**로 분리한다:

1. **`airflow-metadata-db`**: Airflow 자체 메타데이터 전용. 컨트롤 플레인(스케줄러
   부하, 버전 마이그레이션, 락)이라 비즈니스 데이터와 장애/부하 도메인이 다르다.
2. **`domain-db`**: `app`/`bikeman`/`serving` 스키마를 스키마+role로 계속
   구분해서 하나의 인스턴스에 둔다.

**왜 도메인 스키마 3개는 안 쪼개는가**: `bikeman_db.py`의
`_FETCH_COLLECT_TARGETS_SQL`이 `bikeman`과 `serving` 스키마를 **같은 커넥션
안에서 직접 조인**한다. 인스턴스를 쪼개면 이 조인이 깨져 FDW/dblink 같은
우회가 필요해지는데, 이미 강하게 결합된 두 스키마를 억지로 물리 분리할
근거가 없다.

## 3. 서빙 레이어: PostgreSQL 유지 (결정됨)

### 근거
SQLite(동시 writer/reader 환경에 부적합)와 캐시 레이어(Redis/ElastiCache,
현재 배치가 1일 1회라 캐시가 풀어줄 실시간 부하 문제 자체가 없음 - 무효화
로직만 새 실패 지점으로 추가됨)를 검토했으나 도입하지 않는다. 서빙 레이어는
이미 Postgres 문법(`ON CONFLICT DO UPDATE`) 기준으로 멱등성이 설계돼 있어
(`docs/DAG_설계_원칙.md`) 바꿀 근거가 없다.

캐시는 `services/api` 응답 레이턴시가 실측으로 문제가 될 때, 이 domain-db
앞에 얹는 **선택적 확장**으로 재검토한다 (지금 구조를 바꿀 필요 없이 추가만
하면 되는 지점).

## 4. 인스턴스 구성

### 4.1 `airflow-metadata-db`
- 새로 생성. 데이터 이관 불필요 - `airflow-init`의 `_AIRFLOW_DB_MIGRATE: true`가
  첫 부팅 시 스스로 스키마를 구성한다.
- 접근 주체: Airflow 컴포넌트(apiserver/scheduler/dag-processor/triggerer/init)만.

### 4.2 `domain-db`
- 스키마: `app`, `bikeman`, `serving` (`sql/bike_man/bikeman_seed_init.sql`의 DDL
  부분만 실행 - 5번 섹션의 6/30 시드 500건 INSERT는 로컬 개발용이므로 이관
  대상에서 제외)
- 데이터: `bikeman.fact_worker_event`는 초기엔 비워두고, `domain-db` 준비 후
  `bikeman_event_generator`를 2026-08-19 대상으로 1회 실행해 실제 8/19치를
  만든다 (6절 참고)
- 기존 role: `airflow_reader`(bikeman 읽기 전용), `bikeman_writer`(bikeman 쓰기 +
  serving 읽기)
- **신규 role 필요**: `serving_writer` - `serving.station_daily`/
  `serving.bike_risk_daily`/`serving.mart_bike_risk_current`에 대한
  INSERT/UPDATE 권한. 아래 5절 갭 참고.

## 5. 연결 지점별 설정 변경 (배경 조사로 확인된 실제 연결 방식)

| 소비자 | 현재 연결 방식 | 대상 인스턴스 | 필요 변경 |
|---|---|---|---|
| Airflow 자체 | `docker-compose.yml`의 `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (`POSTGRES_*` 조합) | `airflow-metadata-db` | RDS 엔드포인트로 conn string 교체 |
| `bikeman_event_generator` (`generate_collect_events.py`/`deploy_returned_bikes.py`) | Airflow Connection `bikeman_postgres` (`PostgresHook`), `bikeman_writer` 역할 | `domain-db` | Connection 값을 RDS 엔드포인트로 교체 |
| `ingestion/common/db_client.py` | `.env`의 `BIKEMAN_DB_HOST/PORT/NAME/USER/PASSWORD` (raw psycopg2), `airflow_reader` 읽기 전용 | `domain-db` | 값을 RDS 엔드포인트로 교체 |
| `serving_sync` (`pipeline/serving_sync/jobs/serving_db.py`) | `.env`의 `SERVING_DB_HOST/PORT/NAME/USER/PASSWORD` (raw psycopg2) | `domain-db` | **갭**: 이 변수들이 현재 `.env`에 없다. README 주석상으로는 루트 `.env`의 `POSTGRES_*`(=Airflow 슈퍼유저 `airflow`/`airflow`)를 그대로 쓰도록 문서화만 돼 있고 실제 값은 미설정 상태. RDS 전환 시 ① `SERVING_DB_*`를 신규로 채우고 ② 슈퍼유저 대신 `serving_writer`(4.2절) 최소권한 role을 쓰도록 같이 고친다 |

`ingestion`/`serving_sync`의 `jobs/*.py`는 원래 Airflow 없이도
`python -m jobs.X`로 단독 실행 가능하게 설계돼 있다 (repo 컨벤션) - 이 성질은
RDS 전환 후에도 유지된다.

## 6. 마이그레이션 순서

1. `airflow-metadata-db`, `domain-db` 두 RDS 인스턴스 생성 (같은 VPC, private,
   보안그룹은 EC2/ECS로 옮겨갈 컴퓨트 쪽에서만 5432 인바운드 허용 - 실제
   프로비저닝 방식·IaC 여부는 별도 결정)
2. `domain-db`에 `sql/bike_man/bikeman_seed_init.sql`의 DDL(스키마/테이블/
   role/GRANT)만 실행, 5번 섹션(6/30 시드 500건 INSERT)은 제외 → `serving_writer`
   role 추가분 포함하도록 이 SQL 파일 자체를 먼저 갱신
3. `.env`에 `SERVING_DB_*` 신규 추가, `BIKEMAN_DB_*` 값을 `domain-db`
   엔드포인트로 교체, Airflow Connection `bikeman_postgres`를 `domain-db`
   엔드포인트로 갱신
4. `docker-compose.yml`의 `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`을
   `airflow-metadata-db` 엔드포인트로 교체
5. `bikeman_event_generator`를 2026-08-19 대상으로 1회 실행
   (`generate_collect_events.run('2026-08-19')` → `serving.bike_risk_daily`의
   실제 위험도 상위 bike_id를 COLLECT로 적재. `deploy_returned_bikes.run
   ('2026-08-19')`도 같이 돌려도 안전 - 전날(8/18) COLLECT 이력이 없어 0건
   반환하는 게 정상, 6/30 콜드스타트와 같은 패턴)
6. (`docker-compose.local.yml`은 로컬 개발용이므로 그대로 로컬 `postgres`
   컨테이너를 계속 가리키게 둔다 - 변경 대상 아님)

## 7. 검증 계획

1. `domain-db`: `bikeman.fact_worker_event`에 `occurred_at::date = '2026-08-19'`
   COLLECT 이벤트가 존재하고, 그 `bike_id`가 `serving.bike_risk_daily`의 실제
   위험도 상위 bike_id와 일치하는지 확인
2. `airflow-metadata-db`: `airflow-init` 마이그레이션 정상 완료, `airflow version` 확인
3. `serving_sync`/`ingestion` 잡을 `python -m jobs.X`로 스탠드얼론 실행해 새
   env var로 RDS 연결되는지 확인
4. `bikeman_event_generator` DAG를 RDS 대상으로 1회 트리거 →
   `E2E_VERIFICATION.md` 스모크 테스트 재현

## 8. 이번 스펙에서 제외 (Out of scope)

- EC2/ECS로의 컴퓨트 배포 (오케스트레이션 선택, ALB, 태스크 정의, IAM 롤) - 별도 설계
- RDS 실제 프로비저닝 방식(Terraform vs AWS CLI vs 콘솔) - 아직 미결정, 이 문서
  승인 후 별도로 결정
- 캐시 레이어 도입 - 3절 참고, 필요해지면 별도로 재검토
