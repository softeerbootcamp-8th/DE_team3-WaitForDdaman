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

  # RDS가 마이너 버전을 자동으로 16.14보다 올리면 Terraform이 매번 16.14로
  # 되돌리려 드는 드리프트가 생긴다 - 버전을 명시적으로 고정한 만큼 자동 업그레이드도
  # 끈다(실측: 리뷰에서 발견, 2026-08-23).
  auto_minor_version_upgrade = false

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

  # prod의 유일한 Iceberg 카탈로그 저장소가 되므로(컷오버 후) 실수로 지워지면
  # 전체 warehouse의 테이블 포인터를 잃는다 - 삭제 보호를 켠다(실측: 리뷰에서
  # 발견, 2026-08-23).
  deletion_protection = true

  skip_final_snapshot = false
  # 스냅샷 식별자가 고정값이라, destroy -> 재생성 -> 또 destroy를 반복하면 두 번째
  # destroy에서 "이미 존재하는 스냅샷 이름"으로 실패해 destroy가 중간에 멈춘다.
  # deletion_protection=true라 애초에 destroy 자체가 의도적 2단계 조작이 되므로
  # 이 리스크는 낮지만, 실제로 재파괴가 필요해지면 이 값을 수동으로 바꾸거나
  # 기존 스냅샷을 먼저 지워야 한다.
  final_snapshot_identifier = "iceberg-catalog-final-snapshot"
}

# ---- Secrets Manager: EMR job이 런타임에 읽는 자격증명 ----
# JSON 키는 infra/lambdas/serving_sync/app/_secrets.py의 _SECRET_ENV_KEYS 관례와
# 동일 - 나중에(다음 이슈) EMR StartJobRun 트리거 쪽에서 같은 로더 패턴을 재사용
# 할 수 있게 맞춘다.
resource "aws_secretsmanager_secret" "iceberg_catalog" {
  name = "iceberg-catalog-jdbc-credentials"
  # 기본 30일 소프트 삭제 대기 때문에 destroy 후 짧은 시간 안에 재생성하면 이름
  # 충돌로 실패한다 - 7일로 줄여 재현/반복 작업을 덜 번거롭게 한다(실측: 리뷰에서
  # 발견, 2026-08-23).
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "iceberg_catalog" {
  secret_id = aws_secretsmanager_secret.iceberg_catalog.id
  secret_string = jsonencode({
    ICEBERG_JDBC_CATALOG_USER     = var.iceberg_catalog_master_username
    ICEBERG_JDBC_CATALOG_PASSWORD = var.iceberg_catalog_master_password
  })
}

# ---- 보안그룹: EMR Serverless 워커 -> iceberg_catalog RDS + AWS API ----
# 인바운드 불필요(워커는 outbound만 발생). RDS(5432)는 iceberg-catalog-sg로만
# 좁히지만, 443(S3/Secrets Manager/CloudWatch Logs)은 이 리포에 그 서비스들의
# 인터페이스 VPC 엔드포인트가 없어(S3는 Gateway 엔드포인트뿐 - serving_sync.tf
# 참고) 전체 허용해야 한다 - 안 그러면 실행 Role의 S3/Secrets/Logs 권한이 전부
# 무용지물이 된다(실측: 리뷰에서 발견, 2026-08-23).
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

  egress {
    # 한글 원문: "S3/Secrets Manager/CloudWatch Logs 접근용 HTTPS 아웃바운드 허용 (인터페이스 VPC 엔드포인트 없음)"
    description = "Allow HTTPS outbound for S3/Secrets Manager/CloudWatch Logs (no interface VPC endpoints)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
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

# prod의 Iceberg JDBC 카탈로그가 이 인스턴스로 옮기면(.env.prod 컷오버), 이미
# 같은 카탈로그를 쓰는 serving_sync Lambda(#172, serving_sync.tf)도 이 RDS에
# 붙어야 한다 - 안 해두면 컷오버 시점에 serving_sync가 카탈로그 접근을 잃는다
# (실측: 리뷰에서 발견, 2026-08-23).
resource "aws_security_group_rule" "iceberg_catalog_allow_serving_sync" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.iceberg_catalog_sg_id
  source_security_group_id = aws_security_group.serving_sync_lambda_sg.id
  # 한글 원문: "serving_sync Lambda의 iceberg_catalog RDS 접근 허용 (카탈로그 컷오버 대비)"
  description = "Allow iceberg_catalog RDS access from serving_sync Lambda"
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

# ---- ECR: prod Spark 커스텀 이미지 (spark/Dockerfile.prod push 대상) ----
resource "aws_ecr_repository" "emr_spark" {
  name                 = "waitforddaman-emr-spark-prod"
  image_tag_mutability = "MUTABLE"
}

locals {
  emr_spark_image_uri = "${aws_ecr_repository.emr_spark.repository_url}:${var.emr_spark_image_tag}"
}

# EMR Serverless 서비스가 이 리포에서 이미지를 pull할 수 있게 허용 (AWS 공식
# 커스텀 이미지 가이드의 리포지토리 정책). aws_emrserverless_application의 ARN을
# 직접 참조하면 apply 순서가 "리포 -> Application(이미지 검증 위해 정책 필요) ->
# 정책"으로 역전되어 최초 apply가 실패한다(실측: 리뷰에서 발견, 2026-08-23) -
# 그래서 이 계정의 EMR Serverless application 전체로 범위를 넓혀 그 순환을 끊는다
# (여전히 confused-deputy 방지 목적은 유지 - 다른 계정/서비스는 pull 불가).
data "aws_iam_policy_document" "emr_spark_ecr_policy_doc" {
  statement {
    sid    = "EmrServerlessCustomImageSupport"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    actions = [
      "ecr:BatchGetImage",
      "ecr:DescribeImages",
      "ecr:GetDownloadUrlForLayer",
    ]
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:emr-serverless:${var.aws_region}:${data.aws_caller_identity.current.account_id}:/applications/*"]
    }
  }

}

resource "aws_ecr_repository_policy" "emr_spark" {
  repository = aws_ecr_repository.emr_spark.name
  policy     = data.aws_iam_policy_document.emr_spark_ecr_policy_doc.json
}

# ---- IAM: EMR Serverless job 실행 Role ----
# 트러스트 정책은 AWS 공식 가이드 패턴 그대로 (Principal=emr-serverless.amazonaws.com,
# aws:SourceAccount 조건).
data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "emr_spark_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "emr_spark_execution_role" {
  name               = "emr-spark-execution-role"
  assume_role_policy = data.aws_iam_policy_document.emr_spark_assume_role.json
}

# Glue 권한은 넣지 않는다 - jdbc 카탈로그만 쓰고(ICEBERG_CATALOG_TYPE=jdbc),
# 이 리포에 Glue 사용 흔적이 없음(기존 조사로 확인됨).
data "aws_iam_policy_document" "emr_spark_execution_policy_doc" {
  statement {
    sid       = "S3BucketExistenceCheck"
    effect    = "Allow"
    actions   = ["s3:ListAllMyBuckets"]
    resources = ["*"]
  }

  statement {
    sid    = "S3RawWarehouseAccess"
    effect = "Allow"
    # AbortMultipartUpload/ListBucketMultipartUploads/GetBucketLocation 추가 -
    # S3A/Iceberg의 Parquet 멀티파트 업로드·정리 경로에 필요함(실측: 리뷰에서
    # 발견, 2026-08-23). 나머지는 원래 있던 조회/쓰기/삭제 권한.
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
      "s3:GetBucketLocation",
    ]
    resources = [
      "arn:aws:s3:::${var.raw_bucket}",
      "arn:aws:s3:::${var.raw_bucket}/*",
      "arn:aws:s3:::${var.warehouse_bucket}",
      "arn:aws:s3:::${var.warehouse_bucket}/*",
    ]
  }

  statement {
    sid       = "IcebergCatalogSecretAccess"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.iceberg_catalog.arn]
  }

  # EMR Serverless 전용 관리형 정책이 없어 CloudWatch Logs 권한을 직접 부여한다.
  # 로그 그룹 경로 2개를 모두 허용 - EMR Serverless 기본 관리형 로그 그룹은
  # /aws/emr-serverless이고, 다음 이슈(StartJobRun)에서 monitoringConfiguration으로
  # /emr-serverless/* 아래에 직접 지정할 수도 있어 둘 다 열어둔다(실측: 리뷰에서
  # 발견, 2026-08-23 - 로그 그룹 이름이 안 맞으면 조용히 로그가 사라짐).
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/emr-serverless/*",
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless*",
    ]
  }
}

resource "aws_iam_policy" "emr_spark_execution_policy" {
  name   = "emr-spark-execution-policy"
  policy = data.aws_iam_policy_document.emr_spark_execution_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "emr_spark_execution_attach" {
  role       = aws_iam_role.emr_spark_execution_role.name
  policy_arn = aws_iam_policy.emr_spark_execution_policy.arn
}

# ---- EMR Serverless Application ----
resource "aws_emrserverless_application" "emr_spark" {
  name          = "emr-spark-prod"
  release_label = "emr-7.2.0"
  type          = "SPARK"
  architecture  = "X86_64"

  image_configuration {
    image_uri = local.emr_spark_image_uri
  }

  network_configuration {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.emr_serverless_worker.id]
  }

  # CreateApplication이 커스텀 이미지를 검증하려면 EMR Serverless 서비스가 그
  # 시점에 이미 이 리포에서 pull할 수 있어야 한다 - ECR 리포 정책이 이 리소스보다
  # 먼저(또는 최소한 동시에 실패하지 않게) 생겨야 하므로 명시적으로 순서를 강제한다
  # (실측: 리뷰에서 발견, 2026-08-23 - 정책 조건에서 이 리소스 참조를 없앤 것만으로는
  # 순환은 풀리지만 순서 보장까지는 안 됨).
  depends_on = [aws_ecr_repository_policy.emr_spark]
}
