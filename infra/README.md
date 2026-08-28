# infra

AWS 인프라 및 Lambda / Terraform 배포 설정입니다.

## 구성 요소

1. **Lambda 함수 (2개)**
   - `fetch_station_master_raw`: 매일 서울시 `tbCycleStationInfo`를 호출하여 S3 raw 영역에 저장 (`s3://<RAW_BUCKET>/raw/station_master/api/snapshot_date=YYYY-MM-DD/payload.json`)
   - `fetch_station_active_raw`: 매일 서울시 `bikeList`를 호출하여 S3 raw 영역에 저장 (`s3://<RAW_BUCKET>/raw/station_active/api/snapshot_date=YYYY-MM-DD/payload.json`)
   - 런타임: Python 3.12, 메모리 256MB, 타임아웃 180s (VPC/NAT 불필요)

2. **스케줄러 (EventBridge)**
   - Rule: `daily-raw-fetch-at-0010-kst` (`cron(10 15 * * ? *)` = 한국시간 매일 00:10 KST)
   - Airflow 일 배치 실행(01:00)보다 앞서 원천 raw 데이터를 확보

3. **장애 대응 및 알림**
   - **DLQ**: Lambda 실행 실패 시 `raw-fetch-lambda-dlq` SQS에 실패 이벤트 보존 (14일 보존)
   - **CloudWatch Alarm**: Lambda 실행 오류(Errors >= 1) 및 DLQ 메시지 발생 시 SNS 토픽(`raw-fetch-lambda-alerts`)으로 알림
   - **Slack 알림** (`notify_slack` Lambda, Issue #180): 위 SNS 토픽을 구독해 알람을 Slack 인커밍 웹훅으로 전달. `slack_webhook_url` 변수가 빈 문자열이면(로컬/dev 기본값) 관련 리소스가 전부 생성되지 않는다 - 운영 환경에서만 값을 채워서 활성화한다.

## 배포 방법 (Terraform)

`seoul_api_key`, `slack_webhook_url`은 민감값이라 커맨드라인 `-var`로 넘기면 셸 히스토리에 남는다.
대신 `terraform.tfvars`(git에 커밋되지 않음, `.gitignore` 처리됨)에 채워서 쓴다.

```bash
cd infra/terraform

# 최초 1회: 템플릿 복사 후 실제 값 채우기
cp terraform.tfvars.example terraform.tfvars
vi terraform.tfvars   # seoul_api_key 채우기, Slack 알림 쓸 거면 slack_webhook_url도 채우기

# 초기화
terraform init

# 계획 검토 / 배포 (terraform.tfvars를 자동으로 읽음)
terraform plan
terraform apply
```

## serving_sync RDS 적재/검증 Lambda (#172)

목적: `gold_to_serving_sync` DAG의 `write_bike_risk_daily` / `write_station_daily` /
`verify_bike_risk_daily_sync` / `verify_station_daily_sync` 4개 태스크가 Airflow
워커에서 직접 Postgres(RDS)에 접속하던 걸 Lambda로 옮겨서, 이 DB 자격증명
(`SERVING_DB_*`)을 워커에서 완전히 제거한다.

### 구성 요소

1. **Lambda 함수 3개, 이미지는 1개만 공유** (`infra/lambdas/serving_sync/`)
   - `serving-sync-write-bike-risk-daily` / `serving-sync-write-station-daily` /
     `serving-sync-verify` - 셋 다 같은 ECR 이미지를 가리키고 `image_config.command`만
     달라서(`app.write_bike_risk_daily.handler` 등) 이미지 안의 다른 진입점을 고른다.
     빌드는 한 번, 함수(리소스)는 셋이라 함수별로 예약 동시성을 따로 걸 수 있다.
   - 핸들러는 얇다 - 실제 로직은 `pipeline/serving_sync/jobs/write_bike_risk_daily.py`
     등에 그대로 있고(로컬 `python -m jobs.write_bike_risk_daily` 경로와 완전히 같은
     코드), 핸들러는 (1) Secrets Manager에서 DB 자격증명을 읽어 os.environ에 채우고
     (2) event를 그 잡의 입력으로 옮겨준 뒤 그대로 호출한다.
   - `verify-serving-sync`는 `table.inspect.partitions()`(매니페스트만 읽음, 데이터
     스캔 없음)로 Iceberg row count를 구한다.
   - 동기 호출(`RequestResponse`) 고정 - 비동기는 Lambda 자체 재시도가 붙어
     `DELETE+INSERT`가 두 번 돌 수 있다.

2. **VPC 연결 + S3 Gateway VPC Endpoint** - 기존 raw-fetch Lambda(위 1번)는 VPC가
   필요 없었지만, 이번엔 RDS에 붙어야 해서 VPC 안에 둔다. NAT 없이 S3(Iceberg 데이터/
   카탈로그)에 접근하려고 Gateway 타입 VPC 엔드포인트를 같이 둔다(무료).

3. **Secrets Manager** - `SERVING_DB_HOST/PORT/NAME/USER/PASSWORD`와
   `ICEBERG_JDBC_CATALOG_USER/PASSWORD`를 담은 시크릿을 Lambda가 콜드 스타트 시
   읽어 온다(`app/_secrets.py`). Terraform에는 시크릿 ARN만 변수로 들어가고 값
   자체는 안 들어간다.

4. **Airflow → Lambda** - `LambdaInvokeFunctionOperator`(`apache-airflow-providers-amazon`,
   이 저장소에서 Airflow가 Lambda를 직접 호출하는 첫 사례)로 위 4개 태스크를 호출한다.

### 아직 채워야 하는 것 - VPC/RDS 실제 값

`infra/terraform/serving_sync.tf`/`variables.tf`는 **작성은 끝났지만 `terraform apply`는
하지 않았다** - 아래 변수들은 기본값이 없어서 실제 배포 전에 반드시 채워야 한다.
값을 몰라서(이 세션에서는 확인 불가) 지금은 코드만 준비해뒀다.

| 변수 | 필요한 값 |
| --- | --- |
| `vpc_id` | RDS가 있는 기존 VPC ID |
| `subnet_ids` | Lambda를 배치할 서브넷(RDS에 접근 가능해야 함) |
| `route_table_ids` | S3 Gateway VPC Endpoint를 연결할 라우트 테이블 |
| `rds_security_group_id` | 기존 RDS 보안그룹 ID (여기에 Lambda발 5432 인바운드 규칙이 추가됨) |
| `serving_db_secret_arn` | 위 자격증명들을 담은 기존(또는 새로 만들) Secrets Manager 시크릿 ARN |
| `iceberg_jdbc_catalog_uri` | 실제 RDS 엔드포인트를 가리키는 jdbc URI |

또한 `airflow/requirements-ci.txt`에 `apache-airflow-providers-amazon`을 추가했는데,
실제 배포 컨테이너(`apache/airflow` 베이스 이미지)에 이 provider가 기본 번들되어
있는지는 확인하지 못했다 - 안 되어 있으면 `airflow/Dockerfile.local`/`.prod`에도
설치 단계 추가가 필요하다.

**진행하려면**: 위 표의 값들을 확인한 뒤

```bash
cd infra/terraform
terraform plan -var="vpc_id=..." -var="subnet_ids=[...]" -var="route_table_ids=[...]" \
  -var="rds_security_group_id=..." -var="serving_db_secret_arn=..." \
  -var="iceberg_jdbc_catalog_uri=..." -var="seoul_api_key=..." -var="raw_bucket=..."
```
로 계획을 검토한 뒤 `apply`하면 된다. 이미지는 별도로 빌드/푸시해야 한다
(`infra/lambdas/serving_sync/Dockerfile` 상단 주석 참고).
