# ------------------------------------------------------------------------------
# bikeman_event_generator RDS 적재 Lambda (#186)
#
# 목적: bikeman_event_generator DAG의 generate_collect_events / deploy_returned_bikes
# 2개 태스크가 Airflow 워커에서 직접 RDS(domain-db)에 접속한다(Airflow Connection
# bikeman_postgres) - serving_sync(#172)와 같은 이유로, 이 DB 자격증명을 워커에서
# 제거하기 위해 Lambda로 옮긴다.
#
# serving_sync와 별도 이미지를 쓴다 - 이 잡들은 psycopg2만 필요하고 pyiceberg/
# pyarrow는 전혀 안 쓴다(Iceberg를 안 건드림, RDS만 직접 조회/적재). 안 쓰는 무거운
# 의존성을 얹으면 콜드스타트/이미지 크기만 늘어난다.
#
# VPC/RDS는 이미 존재하는 자원을 참조한다(신규 생성 아님, serving_sync와 동일한
# domain-db) - var.vpc_id 등은 플레이스홀더이고 실제 값은 apply 시점에 -var로 채운다.
# ------------------------------------------------------------------------------

resource "aws_ecr_repository" "bikeman_event_generator" {
  name                 = "bikeman-event-generator-lambda"
  image_tag_mutability = "MUTABLE"
}

locals {
  bikeman_event_generator_image_uri = "${aws_ecr_repository.bikeman_event_generator.repository_url}:${var.bikeman_event_generator_image_tag}"
}

# ---- IAM ----
data "aws_iam_policy_document" "bikeman_event_generator_lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bikeman_event_generator_lambda_role" {
  name               = "bikeman-event-generator-lambda-exec-role"
  assume_role_policy = data.aws_iam_policy_document.bikeman_event_generator_lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "bikeman_event_generator_basic_execution" {
  role       = aws_iam_role.bikeman_event_generator_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# VPC 안에서 ENI를 만들고 정리하려면 이 관리형 정책이 필요하다 (serving_sync와 동일).
resource "aws_iam_role_policy_attachment" "bikeman_event_generator_vpc_access" {
  role       = aws_iam_role.bikeman_event_generator_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "bikeman_event_generator_secrets_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.bikeman_db_secret_arn]
  }
}

resource "aws_iam_policy" "bikeman_event_generator_secrets_policy" {
  name        = "bikeman-event-generator-lambda-secrets-policy"
  description = "Allows reading the bikeman_writer DB credentials secret"
  policy      = data.aws_iam_policy_document.bikeman_event_generator_secrets_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "bikeman_event_generator_secrets_attach" {
  role       = aws_iam_role.bikeman_event_generator_lambda_role.name
  policy_arn = aws_iam_policy.bikeman_event_generator_secrets_policy.arn
}

# ---- 보안그룹: Lambda -> RDS ----
resource "aws_security_group" "bikeman_event_generator_lambda_sg" {
  name        = "bikeman-event-generator-lambda-sg"
  description = "Outbound-only security group for bikeman_event_generator Lambda"
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound (RDS, Secrets Manager egress via VPC endpoint)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "rds_allow_bikeman_event_generator_lambda" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.rds_security_group_id
  source_security_group_id = aws_security_group.bikeman_event_generator_lambda_sg.id
  description               = "Allow RDS access from bikeman_event_generator Lambda (#186)"
}

# ---- Lambda 함수 2개 (이미지 1개 공유, image_config.command만 다름) ----
locals {
  bikeman_event_generator_common_env = {
    AWS_DEFAULT_REGION    = var.aws_region
    BIKEMAN_DB_SECRET_ARN = var.bikeman_db_secret_arn
  }
}

resource "aws_lambda_function" "generate_collect_events" {
  function_name = "bikeman-event-generator-generate-collect-events"
  role          = aws_iam_role.bikeman_event_generator_lambda_role.arn
  package_type  = "Image"
  image_uri     = local.bikeman_event_generator_image_uri
  timeout       = 120
  memory_size   = 256

  image_config {
    command = ["app.generate_collect_events.handler"]
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.bikeman_event_generator_lambda_sg.id]
  }

  environment {
    variables = local.bikeman_event_generator_common_env
  }

  reserved_concurrent_executions = var.bikeman_event_generator_reserved_concurrency

  dead_letter_config {
    target_arn = aws_sqs_queue.bikeman_event_generator_dlq.arn
  }
}

resource "aws_lambda_function" "deploy_returned_bikes" {
  function_name = "bikeman-event-generator-deploy-returned-bikes"
  role          = aws_iam_role.bikeman_event_generator_lambda_role.arn
  package_type  = "Image"
  image_uri     = local.bikeman_event_generator_image_uri
  timeout       = 120
  memory_size   = 256

  image_config {
    command = ["app.deploy_returned_bikes.handler"]
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.bikeman_event_generator_lambda_sg.id]
  }

  environment {
    variables = local.bikeman_event_generator_common_env
  }

  reserved_concurrent_executions = var.bikeman_event_generator_reserved_concurrency

  dead_letter_config {
    target_arn = aws_sqs_queue.bikeman_event_generator_dlq.arn
  }
}

# ---- 장애 대응 및 알림 (serving_sync와 동일 패턴) ----
resource "aws_sqs_queue" "bikeman_event_generator_dlq" {
  name                      = "bikeman-event-generator-lambda-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sns_topic" "bikeman_event_generator_alerts" {
  name = "bikeman-event-generator-lambda-alerts"
}

resource "aws_cloudwatch_metric_alarm" "bikeman_event_generator_errors" {
  for_each = {
    generate_collect_events = aws_lambda_function.generate_collect_events.function_name
    deploy_returned_bikes   = aws_lambda_function.deploy_returned_bikes.function_name
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
  alarm_actions       = [aws_sns_topic.bikeman_event_generator_alerts.arn]

  dimensions = {
    FunctionName = each.value
  }
}

resource "aws_cloudwatch_metric_alarm" "bikeman_event_generator_dlq_messages_visible" {
  alarm_name          = "bikeman_event_generator_dlq_messages_visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alarm when bikeman_event_generator DLQ receives failed execution events"
  alarm_actions       = [aws_sns_topic.bikeman_event_generator_alerts.arn]

  dimensions = {
    QueueName = aws_sqs_queue.bikeman_event_generator_dlq.name
  }
}
