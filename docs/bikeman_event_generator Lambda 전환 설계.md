# bikeman_event_generator Lambda 전환 설계

- 관련 이슈: #186 (3-H)
- 배경: `gold_to_serving_sync`의 write/verify 4개 태스크를 Lambda로 옮겨(#172) 워커의 DB
  자격증명을 제거했다. `docs/RDS 적재 및 세팅 설계.md` 2절/5절에 따르면 `bikeman_event_
  generator`가 쓰는 `domain-db`와 `serving_sync`가 쓰는 `domain-db`는 **같은 RDS
  인스턴스**다(`bikeman`/`serving` 스키마가 한 커넥션 안에서 직접 조인하므로 물리
  분리 불가) - `.env.prod`의 `SERVING_DB_HOST`/`BIKEMAN_DB_HOST`가 동일한 호스트임을
  실측으로 확인함. 같은 인스턴스인데 이 DAG만 워커에서 RDS에 직접 붙는 걸 남겨두면
  #172의 실질 이득(워커 DB 권한 0)이 없어진다.

## 1. 현재 상태와 문제

`generate_collect_events.py`/`deploy_returned_bikes.py` 둘 다 Airflow Connection
`bikeman_postgres`를 `PostgresHook(...).get_conn()`으로 얻어 쓴다. 이건 애초에
"사용자 확정"으로 이 저장소의 표준 컨벤션(psycopg2 + `.env` 직접 연결)을 일부러
벗어난 선택이었다(`bikeman_event_generator_dag.py` 상단 주석 참고). Lambda는 Airflow
컨텍스트가 없어 Airflow Connection을 못 쓰므로, 이 예외를 다시 표준 컨벤션(Secrets
Manager + psycopg2, `#172`의 `_secrets.py` 패턴)으로 되돌려야 한다.

다행히 실제 DB 레이어(`bikeman_db.py`)는 이미 Airflow와 무관하게 짜여 있다 - "이
모듈은 airflow를 import하지 않는다"고 자체 docstring에 명시돼 있고, psycopg2 스타일
`conn` 객체만 인자로 받는다. 바뀌는 지점은 **연결을 얻는 방법 하나뿐**이고, 쿼리·
비즈니스 로직(`bikeman_db.py`, `event_builder.py`, `event_ids.py`)은 그대로 둔다.

## 2. 시크릿 구조

`bikeman_writer` 역할(bikeman 쓰기 + serving 읽기) 자격증명은 지금 Airflow Connection
UI 안에만 있고 어떤 `.env`/Secrets Manager에도 없다. `.env.prod`의 `SERVING_DB_*`
(`hamzzi` 사용자)와 `BIKEMAN_DB_*`(`airflow_reader`, 읽기 전용)는 둘 다 이 잡에
맞지 않는 별개 자격증명이다.

**결정**: 새 시크릿 `BIKEMAN_DB_SECRET_ARN`을 별도로 둔다. `SERVING_DB_SECRET_ARN`에
얹지 않는 이유는 두 시크릿의 쓰기 권한 범위가 다르기 때문 - 섞으면 어느 Lambda가
어느 권한으로 동작하는지 코드만 보고 알 수 없어진다.

## 3. Lambda 이미지/인프라

**결정**: `infra/lambdas/bikeman_event_generator/`에 새 경량 이미지를 만든다.
`serving_sync` 이미지는 pyiceberg+pyarrow+psycopg2가 다 필요하지만, 이 잡은 psycopg2
하나면 충분하다 - 안 쓰는 무거운 의존성을 얹으면 콜드스타트/이미지 크기만 늘어난다.

`infra/terraform/bikeman_event_generator.tf`(신규)에 `serving_sync.tf`와 동일한
패턴으로 구성한다:

- ECR 리포지토리 1개, Lambda 함수 2개(이미지 1개 공유, `image_config.command`만
  `app.generate_collect_events.handler` / `app.deploy_returned_bikes.handler`로 분기)
- IAM 롤: `AWSLambdaBasicExecutionRole` + `AWSLambdaVPCAccessExecutionRole` +
  `BIKEMAN_DB_SECRET_ARN`에 대한 `secretsmanager:GetSecretValue` 정책
- 보안그룹: RDS(5432) 아웃바운드만 필요 (S3 접근이 없으므로 `serving_sync`의 S3
  Gateway Endpoint는 이 그룹엔 불필요)
- 기존 `var.vpc_id`/`var.subnet_ids`/`var.rds_security_group_id`를 그대로 재사용
  (같은 VPC, 같은 RDS 인스턴스)
- DLQ(SQS) + CloudWatch 알람 - `serving_sync.tf`와 동일 패턴

핸들러는 얇게 유지한다 - `_secrets.py`로 `BIKEMAN_DB_SECRET_ARN`을 읽어
`BIKEMAN_DB_HOST/PORT/NAME/USER/PASSWORD`를 환경변수로 채운 뒤, 기존
`generate_collect_events.run(target_date)`/`deploy_returned_bikes.run(target_date)`를
그대로 호출한다. 로컬 실행 경로(`python -c "import generate_collect_events; ...`)는
그대로 유지된다.

## 4. 연결 획득부 변경

`generate_collect_events.py`/`deploy_returned_bikes.py`의 `PostgresHook(postgres_conn_id=
"bikeman_postgres").get_conn()`을, `serving_db.py`가 이미 하듯 환경변수
(`BIKEMAN_DB_HOST/PORT/NAME/USER/PASSWORD`)로 만든 `psycopg2.connect(...)`로 바꾼다.
Airflow에서 직접 실행할 일이 없어지므로 `airflow.providers.postgres` import와
`CONN_ID` 상수를 제거한다.

## 5. 같이 고치는 기존 gap 2개

PR #190(gold_risk_decision 전환) 이후 검토 과정에서 `serving_sync.tf`에 남아있던
gap을 발견했고, 이번에 새로 만드는 Lambda 2개도 똑같이 겪을 문제라 이번 작업에서
같이 고친다.

1. **Secrets Manager Interface VPC Endpoint 누락**: `serving_sync.tf`에 S3 Gateway
   Endpoint는 있지만 Secrets Manager용 Interface Endpoint(`com.amazonaws.<region>.
   secretsmanager`)가 없다. NAT Gateway도 없는 구조라 지금 상태로는 두 Lambda 그룹
   다 프라이빗 서브넷에서 Secrets Manager에 도달할 방법이 없다. `main.tf` 또는
   `serving_sync.tf`에 VPC 레벨 리소스로 추가하고, `bikeman_event_generator.tf`의
   보안그룹도 이 엔드포인트로 나가는 아웃바운드를 허용한다.
2. **Airflow → Lambda 호출 권한 누락**: `LambdaInvokeFunctionOperator`가 쓰는 Airflow
   워커의 실행 자격증명에 `lambda:InvokeFunction` 권한이 없다. EC2 인스턴스 롤이
   Terraform으로 관리되고 있지 않아(#109가 수동 구성한 것으로 보임), 기존 롤을
   가리키는 새 변수 `var.airflow_worker_role_name`(placeholder)을 추가하고, 그 롤에
   기존 Lambda 5개(serving_sync 3개 + bikeman_event_generator 2개) 전체를 호출할 수
   있는 IAM 정책을 붙인다.

## 6. DAG 변경

`bikeman_event_generator_dag.py`의 `PythonOperator` 2개를 `LambdaInvokeFunctionOperator`
2개로 바꾼다. `invocation_type="RequestResponse"`(동기), payload는
`{"snapshot_date": target_date}` 형태로 `serving_sync`와 동일하게 맞춘다.
`deploy_returned_bikes_task >> generate_collect_events_task` 순서 의존성은 그대로
유지한다(태스크 순서 자체는 이 전환과 무관 - 방어적 안전장치로 계속 둔다는 기존
주석의 판단도 유지).

## 7. 테스트

`bikeman_db.py`는 원래도 실제 DB 연결이 필요해 pytest 유닛테스트 대상이 아니었다
(docstring에 명시, `E2E_VERIFICATION.md`의 라이브 스모크 테스트로 검증하는 판단을
유지). 이번에 새로 생기는 코드는 연결 획득부(`_secrets.py` 스타일 얇은 함수)뿐이라
동일한 판단을 적용한다 - 로컬 실행 경로(`python -m app.X` 또는 기존 `python -c`
호출)로 검증한다.

## 8. apply 여부

`serving_sync`와 동일하게, VPC/RDS/시크릿 ARN의 실제 값은 모르므로 Terraform
코드는 작성하되 `terraform apply`는 하지 않는다. 값이 없는 변수(`var.bikeman_db_
secret_arn`, `var.airflow_worker_role_name`)로 남겨두고, 실제 값을 아는 사람이
검토 후 apply한다.

## 9. 비범위

- `bikeman_db.py`/`event_builder.py`/`event_ids.py`의 쿼리·이벤트 생성 로직 변경 - 없음
- 태스크 순서(`deploy_returned_bikes >> generate_collect_events`) 자체의 필요성
  재검토 - 기존 판단(주석) 유지, 이번 전환과 무관
- `serving_sync.tf`의 세 번째 gap(S3 읽기 IAM)은 이 작업과 무관 - bikeman Lambda는
  S3를 쓰지 않는다