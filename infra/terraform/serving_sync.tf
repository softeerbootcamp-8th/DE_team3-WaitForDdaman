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

data "aws_iam_policy_document" "serving_sync_secrets_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.serving_db_secret_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.serving_sync_dlq.arn]
  }
}

resource "aws_iam_policy" "serving_sync_secrets_policy" {
  name        = "serving-sync-lambda-secrets-policy"
  description = "Allows reading the serving DB / iceberg catalog credentials secret and sending failed events to DLQ"
  policy      = data.aws_iam_policy_document.serving_sync_secrets_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "serving_sync_secrets_attach" {
  role       = aws_iam_role.serving_sync_lambda_role.name
  policy_arn = aws_iam_policy.serving_sync_secrets_policy.arn
}

# ---- 보안그룹: Lambda -> RDS ----
# 기존 RDS 보안그룹(var.rds_security_group_id)의 다른 인바운드 규칙은 건드리지
# 않고, 이 Lambda 전용 보안그룹에서의 5432 인바운드만 새로 추가한다.
resource "aws_security_group" "serving_sync_lambda_sg" {
  name = "serving-sync-lambda-sg"
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
# VPC 서브넷 안에서 NAT 없이 S3(Iceberg 데이터/카탈로그 warehouse)에 접근하기
# 위함. Gateway 타입은 요금이 없다(Interface 타입과 다름).
resource "aws_vpc_endpoint" "serving_sync_s3_gateway" {
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.route_table_ids
}

# ---- Lambda 함수 3개 (이미지 1개 공유, image_config.command만 다름) ----
# 공통 환경변수 - config.SETTINGS(config/__init__.py)가 그대로 읽는 이름들.
# ICEBERG_JDBC_CATALOG_USER/PASSWORD, SERVING_DB_HOST/PORT/NAME/USER/PASSWORD는
# 여기 안 넣는다 - app/_secrets.py가 콜드 스타트 시 SERVING_DB_SECRET_ARN으로
# Secrets Manager에서 읽어 os.environ에 채운다(평문으로 Terraform에 안 남김).
locals {
  serving_sync_common_env = {
    APP_ENV                  = "aws"
    RAW_BUCKET               = var.raw_bucket
    WAREHOUSE_BUCKET         = var.warehouse_bucket
    ICEBERG_CATALOG_TYPE     = "jdbc"
    ICEBERG_CATALOG_NAME     = var.iceberg_catalog_name
    ICEBERG_WAREHOUSE_PATH   = "s3a://${var.warehouse_bucket}/warehouse"
    ICEBERG_JDBC_CATALOG_URI = var.iceberg_jdbc_catalog_uri
    SERVING_DB_SECRET_ARN    = var.serving_db_secret_arn
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

  dead_letter_config {
    target_arn = aws_sqs_queue.serving_sync_dlq.arn
  }
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

  dead_letter_config {
    target_arn = aws_sqs_queue.serving_sync_dlq.arn
  }
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

  dead_letter_config {
    target_arn = aws_sqs_queue.serving_sync_dlq.arn
  }
}

# ---- 장애 대응 및 알림 (raw-fetch Lambda와 동일 패턴, main.tf 참고) ----
resource "aws_sqs_queue" "serving_sync_dlq" {
  name                      = "serving-sync-lambda-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sns_topic" "serving_sync_alerts" {
  name = "serving-sync-lambda-alerts"
}

resource "aws_cloudwatch_metric_alarm" "serving_sync_errors" {
  for_each = {
    write_bike_risk_daily = aws_lambda_function.write_bike_risk_daily.function_name
    write_station_daily   = aws_lambda_function.write_station_daily.function_name
    verify_serving_sync   = aws_lambda_function.verify_serving_sync.function_name
  }

  alarm_name          = "${each.value}_errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when ${each.value} Lambda experiences execution errors"
  alarm_actions       = [aws_sns_topic.serving_sync_alerts.arn]

  dimensions = {
    FunctionName = each.value
  }
}

resource "aws_cloudwatch_metric_alarm" "serving_sync_dlq_messages_visible" {
  alarm_name          = "serving_sync_dlq_messages_visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alarm when serving_sync DLQ receives failed execution events"
  alarm_actions       = [aws_sns_topic.serving_sync_alerts.arn]

  dimensions = {
    QueueName = aws_sqs_queue.serving_sync_dlq.name
  }
}
