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

# ---- 보안그룹: EMR Serverless 워커 -> iceberg_catalog RDS ----
# 인바운드 불필요(워커는 outbound만 발생). 아웃바운드도 전체 허용이 아니라
# iceberg-catalog-sg로 5432만 좁힌다.
resource "aws_security_group" "emr_serverless_worker" {
  name = "emr-serverless-worker-sg"
  # 한글 원문: "EMR Serverless 워커 아웃바운드 전용 보안그룹 (인바운드 불필요)"
  description = "Outbound-only security group for EMR Serverless Spark workers"
  vpc_id      = var.vpc_id

  egress {
    # 한글 원문: "iceberg_catalog RDS(Postgres)로만 아웃바운드 허용"
    description     = "Allow outbound to the iceberg_catalog RDS (Postgres) only"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.iceberg_catalog_sg_id]
  }
}

# ---- iceberg-catalog-sg(기존 SG)에 EMR Serverless 워커발 인바운드만 추가 ----
# SG 자체는 이 리포에서 import/통째로 관리하지 않는다 - 다른 인바운드 규칙은
# 건드리지 않고 이 규칙만 추가한다 (serving_sync.tf가 기존 RDS 보안그룹을
# 다루는 방식과 동일 원칙).
resource "aws_security_group_rule" "iceberg_catalog_allow_emr_serverless" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.iceberg_catalog_sg_id
  source_security_group_id = aws_security_group.emr_serverless_worker.id
  # 한글 원문: "EMR Serverless 워커의 iceberg_catalog RDS 접근 허용"
  description = "Allow iceberg_catalog RDS access from EMR Serverless workers"
}

# ---- (수동 절차, 이 리포에서 실행 안 함) 콘솔에서 잘못 들어간 CIDR 규칙 제거 ----
# iceberg-catalog-sg에 콘솔에서 잘못 추가된 인바운드 규칙(121.160.189.177/32,
# 5432/tcp)이 있다. 이 리포는 SG를 통째로 관리하지 않으므로, 그 규칙 하나만
# 아래 절차로 별도 정리한다 (AWS 자격증명이 있는 담당자가 apply 권한이 있는
# 환경에서 1회 실행 - 이 세션엔 자격증명이 없어 실행 불가):
#
#   1. 아래 스텁 리소스를 이 파일에 잠깐 추가한다:
#        resource "aws_security_group_rule" "iceberg_catalog_remove_stray_cidr" {
#          type              = "ingress"
#          from_port         = 5432
#          to_port           = 5432
#          protocol          = "tcp"
#          security_group_id = var.iceberg_catalog_sg_id
#          cidr_blocks       = ["121.160.189.177/32"]
#        }
#   2. terraform import 'aws_security_group_rule.iceberg_catalog_remove_stray_cidr' \
#        sg-0ff85e9c8d00a6c6b_ingress_tcp_5432_5432_121.160.189.177/32
#   3. 위 스텁 리소스 블록을 코드에서 삭제하고 terraform apply
#      (state에는 남아있다가 "코드에 없음"으로 판정되어 destroy됨 - SG의 다른
#      규칙이나 SG 자체는 전혀 안 건드림)
