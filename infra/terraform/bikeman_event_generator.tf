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

# SQS DLQ 정책 생략: sqs:CreateQueue 권한이 없어 DLQ 자체를 안 만든다 (아래 참고).

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
  description              = "Allow RDS access from bikeman_event_generator Lambda (#186)"
}

# domain-db(bikeman_writer 등)에 Lambda 말고는 붙을 경로가 없어, bikeman_writer
# 비밀번호를 점검/재설정하려는 psql 접속이 EC2에서도 타임아웃났다 (실측:
# 2026-08-25). iceberg_catalog RDS에 이미 적용한 것과 동일한 패턴
# (iceberg_catalog_allow_airflow_ec2, emr_spark.tf)으로 Airflow 워커(EC2) SG를
# 허용한다.
resource "aws_security_group_rule" "rds_allow_airflow_ec2" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.rds_security_group_id
  source_security_group_id = var.airflow_ec2_sg_id
  description              = "Allow domain-db RDS access from the Airflow worker EC2"
}

# ---- Lambda 함수 2개 (이미지 1개 공유, image_config.command만 다름) ----
# 원래는 BIKEMAN_DB_SECRET_ARN을 통해 Secrets Manager에서 읽어오게 할 계획이었으나,
# 이 계정에 secretsmanager:CreateSecret 권한이 없어(SCP가 아니라 순수 IAM gap)
# 자격증명을 직접 환경변수로 주입한다. app/_secrets.py는 BIKEMAN_DB_SECRET_ARN이
# 없으면 조용히 스킵하고 여기 값을 그대로 쓴다.
locals {
  bikeman_event_generator_common_env = {
    BIKEMAN_WRITER_DB_HOST     = var.bikeman_writer_db_host
    BIKEMAN_WRITER_DB_PORT     = var.bikeman_writer_db_port
    BIKEMAN_WRITER_DB_NAME     = var.bikeman_writer_db_name
    BIKEMAN_WRITER_DB_USER     = var.bikeman_writer_db_user
    BIKEMAN_WRITER_DB_PASSWORD = var.bikeman_writer_db_password
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
}

# ---- 장애 대응 및 알림 생략: sqs:CreateQueue/SNS:CreateTopic 권한이 없다(SCP가
# 아니라 순수 IAM gap). 권한이 열리면 DLQ/SNS/CloudWatch 알람을 다시 추가할 것.
