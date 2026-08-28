# ------------------------------------------------------------------------------
# serving_sync RDS 적재/검증 Lambda (#172)
#
# 목적: gold_to_serving_sync DAG의 write_bike_risk_daily / write_station_daily /
# verify_bike_risk_daily_sync / verify_station_daily_sync 4개 태스크가 Airflow
# 워커에서 직접 RDS(Postgres)에 접속한다 - 이 DB 자격증명을 워커에서 완전히
# 제거하기 위해 Lambda로 옮긴다.
#
# VPC/RDS는 이 저장소의 raw-fetch Lambda(main.tf, #141)와 달리 이미 존재하는
# 자원을 참조해야 한다(신규 생성 아님) - var.vpc_id 등으로 플레이스홀더를 두고,
# 실제 값은 apply 시점에 -var로 채운다.
# ------------------------------------------------------------------------------

# ---- ECR: 이미지 1개를 Lambda 함수 3개가 공유 ----
resource "aws_ecr_repository" "serving_sync" {
  name                 = "serving-sync-lambda"
  image_tag_mutability = "MUTABLE"
}

locals {
  serving_sync_image_uri = "${aws_ecr_repository.serving_sync.repository_url}:${var.serving_sync_image_tag}"
}

# ---- IAM ----
data "aws_iam_policy_document" "serving_sync_lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "serving_sync_lambda_role" {
  name               = "serving-sync-lambda-exec-role"
  assume_role_policy = data.aws_iam_policy_document.serving_sync_lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "serving_sync_basic_execution" {
  role       = aws_iam_role.serving_sync_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# VPC 안에서 ENI를 만들고 정리하려면 이 관리형 정책이 필요하다(AWS 표준 요구사항 -
# VPC에 안 붙는 raw-fetch Lambda의 롤에는 없던 것).
resource "aws_iam_role_policy_attachment" "serving_sync_vpc_access" {
  role       = aws_iam_role.serving_sync_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# SQS DLQ 정책 생략: sqs:CreateQueue 권한이 없어 DLQ 자체를 안 만든다 (아래 참고).

# write_bike_risk_daily.py / write_station_daily.py / verify_serving_sync.py가
# pyiceberg로 warehouse_bucket의 Iceberg 데이터 파일을 직접 스캔한다 (#207) -
# 카탈로그(JDBC)는 위 Secrets Manager 자격증명으로 붙지만, 실제 Parquet 데이터
# 읽기는 S3 권한이 별도로 필요하다. warehouse_bucket 하위로만 범위를 한정한다.
data "aws_iam_policy_document" "serving_sync_s3_read_policy_doc" {
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      "arn:aws:s3:::${var.warehouse_bucket}",
      "arn:aws:s3:::${var.warehouse_bucket}/*",
    ]
  }
}

resource "aws_iam_policy" "serving_sync_s3_read_policy" {
  name        = "serving-sync-lambda-s3-read-policy"
  description = "Allows serving_sync Lambda to read Iceberg data files from the warehouse bucket"
  policy      = data.aws_iam_policy_document.serving_sync_s3_read_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "serving_sync_s3_read_attach" {
  role       = aws_iam_role.serving_sync_lambda_role.name
  policy_arn = aws_iam_policy.serving_sync_s3_read_policy.arn
}

# ---- 보안그룹: Lambda -> RDS ----
# 기존 RDS 보안그룹(var.rds_security_group_id)의 다른 인바운드 규칙은 건드리지
# 않고, 이 Lambda 전용 보안그룹에서의 5432 인바운드만 새로 추가한다.
resource "aws_security_group" "serving_sync_lambda_sg" {
  name        = "serving-sync-lambda-sg"
  # AWS 보안그룹/규칙 description은 ASCII만 허용한다 (Issue #188) - 한글 원문:
  # "serving_sync Lambda(write/verify) 아웃바운드 전용 보안그룹"
  description = "Outbound-only security group for serving_sync Lambda (write/verify)"
  vpc_id      = var.vpc_id

  egress {
    # 한글 원문: "전체 아웃바운드 허용 (RDS, Secrets Manager/S3는 VPC 엔드포인트로 나감)"
    description = "Allow all outbound (RDS, Secrets Manager/S3 egress via VPC endpoints)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "rds_allow_serving_sync_lambda" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.rds_security_group_id
  source_security_group_id = aws_security_group.serving_sync_lambda_sg.id
  # 한글 원문: "serving_sync Lambda(#172)의 RDS 접근 허용"
  description = "Allow RDS access from serving_sync Lambda (#172)"
}

# ---- S3 Gateway VPC Endpoint ----
# 생략: rtb-00e33332c4ec8246c/rtb-011d8b54a3d18e7a7(우리 private 서브넷)에 이미
# S3 Gateway 엔드포인트(vpce-0685bfa0c07335eb4, EMR Serverless 인프라가 생성)가
# 연결돼 있어서 이 리소스를 또 만들면 같은 라우트 테이블에 중복 라우트가 생겨 apply가
# 실패한다. 기존 엔드포인트를 그대로 재사용한다.

# ---- Lambda 함수 3개 (이미지 1개 공유, image_config.command만 다름) ----
# 공통 환경변수 - config.SETTINGS(config/__init__.py)가 그대로 읽는 이름들.
# 원래는 SERVING_DB_SECRET_ARN을 통해 app/_secrets.py가 콜드 스타트 시 Secrets
# Manager에서 읽어오게 할 계획이었으나, 이 계정에 secretsmanager:CreateSecret
# 권한이 없어(SCP가 아니라 순수 IAM gap) 자격증명을 직접 환경변수로 주입한다.
# SERVING_DB_SECRET_ARN을 비워두면 _secrets.py가 조용히 스킵하고 여기 값을 그대로 쓴다.
locals {
  serving_sync_common_env = {
    APP_ENV                       = "aws"
    RAW_BUCKET                    = var.raw_bucket
    WAREHOUSE_BUCKET              = var.warehouse_bucket
    ICEBERG_CATALOG_TYPE          = "jdbc"
    ICEBERG_CATALOG_NAME          = var.iceberg_catalog_name
    ICEBERG_WAREHOUSE_PATH        = "s3a://${var.warehouse_bucket}/warehouse"
    ICEBERG_JDBC_CATALOG_URI      = var.iceberg_jdbc_catalog_uri
    ICEBERG_JDBC_CATALOG_USER     = var.iceberg_jdbc_catalog_user
    ICEBERG_JDBC_CATALOG_PASSWORD = var.iceberg_jdbc_catalog_password
    SERVING_DB_HOST               = var.serving_db_host
    SERVING_DB_PORT               = var.serving_db_port
    SERVING_DB_NAME               = var.serving_db_name
    SERVING_DB_USER               = var.serving_db_user
    SERVING_DB_PASSWORD           = var.serving_db_password
  }
}

resource "aws_lambda_function" "write_bike_risk_daily" {
  function_name = "serving-sync-write-bike-risk-daily"
  role          = aws_iam_role.serving_sync_lambda_role.arn
  package_type  = "Image"
  image_uri     = local.serving_sync_image_uri
  timeout       = 300
  memory_size   = 512

  image_config {
    command = ["app.write_bike_risk_daily.handler"]
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.serving_sync_lambda_sg.id]
  }

  environment {
    variables = local.serving_sync_common_env
  }

  # RDS 커넥션 수를 제한한다 - 동시에 뜰 수 있는 인스턴스 수의 상한.
  reserved_concurrent_executions = var.serving_sync_reserved_concurrency
}

resource "aws_lambda_function" "write_station_daily" {
  function_name = "serving-sync-write-station-daily"
  role          = aws_iam_role.serving_sync_lambda_role.arn
  package_type  = "Image"
  image_uri     = local.serving_sync_image_uri
  timeout       = 300
  memory_size   = 512

  image_config {
    command = ["app.write_station_daily.handler"]
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.serving_sync_lambda_sg.id]
  }

  environment {
    variables = local.serving_sync_common_env
  }

  reserved_concurrent_executions = var.serving_sync_reserved_concurrency
}

resource "aws_lambda_function" "verify_serving_sync" {
  function_name = "serving-sync-verify"
  role          = aws_iam_role.serving_sync_lambda_role.arn
  package_type  = "Image"
  image_uri     = local.serving_sync_image_uri
  timeout       = 120
  memory_size   = 256

  image_config {
    command = ["app.verify_serving_sync.handler"]
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.serving_sync_lambda_sg.id]
  }

  environment {
    variables = local.serving_sync_common_env
  }

  # write_*보다 넉넉하게 - bike_risk_daily/station_daily 검증 둘 다 이 함수를
  # 공유해서 부르므로(#172, verify_serving_sync.py 참고) 동시 실행 여지가 더 있다.
  reserved_concurrent_executions = var.serving_sync_reserved_concurrency * 2
}

# ---- 장애 대응 및 알림 생략: sqs:CreateQueue/SNS:CreateTopic 권한이 없다(SCP가
# 아니라 순수 IAM gap). 권한이 열리면 DLQ/SNS/CloudWatch 알람을 다시 추가할 것.
