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

## 배포 방법 (Terraform)

```bash
cd infra/terraform

# 초기화
terraform init

# 계획 검토
terraform plan -var="seoul_api_key=YOUR_API_KEY" -var="raw_bucket=ttareungyi-raw"

# 배포
terraform apply -var="seoul_api_key=YOUR_API_KEY" -var="raw_bucket=ttareungyi-raw"
```
