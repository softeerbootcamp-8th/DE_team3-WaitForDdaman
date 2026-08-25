variable "aws_region" {
  description = "AWS region for deployment (config.SETTINGS.s3_region과 동일해야 함)"
  type        = string
  default     = "ap-northeast-2"
}

variable "app_env" {
  description = "Application environment (prod, dev, local)"
  type        = string
  default     = "prod"
}

variable "raw_bucket" {
  description = "S3 bucket name for raw layer storage (config.SETTINGS.raw_bucket과 동일해야 함)"
  type        = string
  default     = "ttareungyi-raw"
}

variable "seoul_api_key" {
  description = "Authentication key for Seoul Open Data Plaza API"
  type        = string
  sensitive   = true
}

variable "seoul_api_base_url" {
  description = "Base URL for Seoul Open Data Plaza API (config.SETTINGS.seoul_api_base_url과 동일해야 함)"
  type        = string
  default     = "http://openapi.seoul.go.kr:8088"
}

variable "slack_webhook_url" {
  description = "Slack incoming webhook URL for failure notifications"
  type        = string
  default     = ""
  sensitive   = true
}

# ------------------------------------------------------------------------------
# serving_sync Lambda (#172) - VPC/RDS는 신규 생성이 아니라 기존 자원을 참조한다.
# 기본값이 없는 변수는 apply 시점에 -var로 반드시 채워야 한다.
# ------------------------------------------------------------------------------
variable "vpc_id" {
  description = "serving_sync Lambda가 붙을 기존 VPC ID (RDS와 같은 VPC)"
  type        = string
}

variable "subnet_ids" {
  description = "serving_sync Lambda를 배치할 서브넷 ID 목록 (RDS가 접근 가능한 서브넷)"
  type        = list(string)
}

variable "route_table_ids" {
  description = "S3 Gateway VPC Endpoint를 연결할 라우트 테이블 ID 목록"
  type        = list(string)
}

variable "rds_security_group_id" {
  description = "기존 RDS(serving DB) 보안그룹 ID - 여기에 Lambda 보안그룹발 5432 인바운드 규칙을 추가한다"
  type        = string
}

# serving_sync/bikeman_event_generator DB 자격증명 - Secrets Manager를 쓰려 했으나
# 이 계정에 secretsmanager:CreateSecret/ListSecrets 권한 자체가 없어(SCP가 아니라
# 순수 IAM 권한 gap, MFA로도 안 풀림) Lambda 환경변수로 직접 주입하는 방식으로
# 우회한다. app/_secrets.py가 SERVING_DB_SECRET_ARN/BIKEMAN_DB_SECRET_ARN이 없으면
# "환경변수가 이미 채워져 있다"고 보고 조용히 넘어가도록 이미 짜여 있어 코드 변경 없이 동작.
variable "serving_db_host" {
  type = string
}
variable "serving_db_port" {
  type    = string
  default = "5432"
}
variable "serving_db_name" {
  type = string
}
variable "serving_db_user" {
  type = string
}
variable "serving_db_password" {
  type      = string
  sensitive = true
}
variable "iceberg_jdbc_catalog_user" {
  type = string
}
variable "iceberg_jdbc_catalog_password" {
  type      = string
  sensitive = true
}
variable "bikeman_writer_db_host" {
  type = string
}
variable "bikeman_writer_db_port" {
  type    = string
  default = "5432"
}
variable "bikeman_writer_db_name" {
  type = string
}
variable "bikeman_writer_db_user" {
  type = string
}
variable "bikeman_writer_db_password" {
  type      = string
  sensitive = true
}

variable "warehouse_bucket" {
  description = "S3 bucket name for Iceberg warehouse storage (config.SETTINGS.warehouse_bucket과 동일해야 함)"
  type        = string
  default     = "ttareungyi-warehouse"
}

variable "iceberg_catalog_name" {
  description = "pyiceberg SqlCatalog 이름 (config.SETTINGS.iceberg_catalog_name과 동일해야 함)"
  type        = string
  default     = "bike_catalog"
}

variable "iceberg_jdbc_catalog_uri" {
  description = "Iceberg jdbc 카탈로그 URI (예: jdbc:postgresql://<rds-endpoint>:5432/iceberg_catalog)"
  type        = string
}

variable "serving_sync_image_tag" {
  description = "infra/lambdas/serving_sync 이미지의 ECR 태그"
  type        = string
  default     = "latest"
}

variable "serving_sync_reserved_concurrency" {
  description = "write_bike_risk_daily / write_station_daily 함수당 예약 동시성 (RDS 커넥션 수 제한). verify_serving_sync는 이 값의 2배를 쓴다."
  type        = number
  default     = 2
}

# ------------------------------------------------------------------------------
# EMR Serverless prod Spark 인프라 (#183) - iceberg_catalog RDS는 신규 생성이지만
# VPC/서브넷그룹/iceberg-catalog-sg는 기존 자원을 참조한다(신규 생성 아님).
# ------------------------------------------------------------------------------
variable "iceberg_catalog_sg_id" {
  description = "기존 iceberg-catalog-sg 보안그룹 ID (콘솔에서 이미 생성됨, sg-0ff85e9c8d00a6c6b)"
  type        = string
}

variable "iceberg_catalog_db_subnet_group_name" {
  description = "iceberg_catalog RDS가 속할 기존 DB 서브넷 그룹 이름"
  type        = string
  default     = "waitforddaman-subnet"
}

variable "iceberg_catalog_master_username" {
  description = "iceberg_catalog RDS 마스터 유저명"
  type        = string
  default     = "iceberg_admin"
}

variable "iceberg_catalog_master_password" {
  description = "iceberg_catalog RDS 마스터 비밀번호 - tfvars로만 채움, 커밋 금지"
  type        = string
  sensitive   = true
}

variable "emr_spark_image_tag" {
  description = "waitforddaman-emr-spark-prod ECR 리포의 이미지 태그"
  type        = string
  default     = "latest"
}

# ------------------------------------------------------------------------------
# bikeman_event_generator Lambda (#186) - serving_sync(#172)와 같은 VPC/RDS를
# 재사용한다(같은 domain-db 인스턴스, docs/RDS 적재 및 세팅 설계.md 2절 참고).
# ------------------------------------------------------------------------------
variable "bikeman_event_generator_image_tag" {
  description = "infra/lambdas/bikeman_event_generator 이미지의 ECR 태그"
  type        = string
  default     = "latest"
}

variable "bikeman_event_generator_reserved_concurrency" {
  description = "generate_collect_events / deploy_returned_bikes 함수당 예약 동시성 (RDS 커넥션 수 제한)"
  type        = number
  default     = 2
}

# ------------------------------------------------------------------------------
# Airflow 워커 -> Lambda 호출 권한 (기존 gap - serving_sync 3개 + bikeman_event_
# generator 2개 전부에 필요했는데 지금까지 빠져있었다)
# ------------------------------------------------------------------------------
variable "airflow_worker_role_name" {
  description = "Airflow 워커(EC2)가 쓰는 기존 IAM 롤 이름 - lambda:InvokeFunction 정책을 여기 붙인다. EC2 인스턴스 롤은 Terraform 밖에서 수동 생성됨(#109)"
  type        = string
}

variable "airflow_ec2_sg_id" {
  description = "Airflow 워커(EC2)가 쓰는 기존 보안그룹 ID - iceberg_catalog RDS 인바운드 허용에 쓴다. EC2 인스턴스는 Terraform 밖에서 수동 생성됨(#109)"
  type        = string
}
