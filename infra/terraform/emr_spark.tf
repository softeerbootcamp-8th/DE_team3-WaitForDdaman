# ------------------------------------------------------------------------------
# EMR Serverless prod Spark 인프라 (#183)
#
# 목적: pipeline/train_risk_model/samples.py, ingestion/jobs/initial_load_*,
# airflow/scripts/check_*.py 등 남은 Spark 잡을 AWS EMR Serverless에서 실행한다.
# VPC/서브넷그룹/iceberg-catalog-sg는 기존 자원을 참조만 한다(신규 생성 아님) -
# infra/terraform/serving_sync.tf(#172)와 동일한 원칙.
#
# iceberg_catalog RDS는 이 파일에서 신규로 만드는 전용 소규모 인스턴스로, prod의
# Iceberg JDBC 카탈로그(메타데이터 포인터) 저장소를 여기로 옮긴다. 기존
# domain-db-v2의 iceberg_catalog 스키마에서 이 인스턴스로 컷오버(.env.prod의
# ICEBERG_JDBC_CATALOG_URI 변경 등)하는 건 이 Terraform 작업 범위 밖의 별도
# 운영 작업이다.
# ------------------------------------------------------------------------------

# ---- 신규 RDS: Iceberg JDBC 카탈로그 전용 ----
resource "aws_db_instance" "iceberg_catalog" {
  identifier     = "iceberg-catalog"
  engine         = "postgres"
  engine_version = "16.14"
  instance_class = "db.t4g.micro"

  db_name  = "iceberg_catalog"
  username = var.iceberg_catalog_master_username
  password = var.iceberg_catalog_master_password

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  multi_az                = false
  publicly_accessible     = false
  db_subnet_group_name    = var.iceberg_catalog_db_subnet_group_name
  vpc_security_group_ids  = [var.iceberg_catalog_sg_id]
  backup_retention_period = 7

  skip_final_snapshot       = false
  final_snapshot_identifier = "iceberg-catalog-final-snapshot"
}

# ---- Secrets Manager: EMR job이 런타임에 읽는 자격증명 ----
# JSON 키는 infra/lambdas/serving_sync/app/_secrets.py의 _SECRET_ENV_KEYS 관례와
# 동일 - 나중에(다음 이슈) EMR StartJobRun 트리거 쪽에서 같은 로더 패턴을 재사용
#할 수 있게 맞춘다.
resource "aws_secretsmanager_secret" "iceberg_catalog" {
  name = "iceberg-catalog-jdbc-credentials"
}

resource "aws_secretsmanager_secret_version" "iceberg_catalog" {
  secret_id = aws_secretsmanager_secret.iceberg_catalog.id
  secret_string = jsonencode({
    ICEBERG_JDBC_CATALOG_USER     = var.iceberg_catalog_master_username
    ICEBERG_JDBC_CATALOG_PASSWORD = var.iceberg_catalog_master_password
  })
}
