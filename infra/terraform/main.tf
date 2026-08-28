terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------------------
# Dead Letter Queue (SQS) - 생략: 이 계정에 sqs:CreateQueue 권한이 없다(SCP가 아니라
# 순수 IAM gap). DLQ/SNS 알림은 부가 기능이라 없어도 Lambda 자체는 정상 동작한다 -
# 권한이 열리면 이 블록과 아래 dead_letter_config/모니터링 섹션을 다시 추가할 것.
# ------------------------------------------------------------------------------
# IAM Role & Policies for Lambdas
# ------------------------------------------------------------------------------
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "raw_fetch_lambda_role" {
  name               = "raw-fetch-lambda-exec-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.raw_fetch_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "raw_fetch_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["arn:aws:s3:::${var.raw_bucket}/raw/*"]
  }
}

resource "aws_iam_policy" "raw_fetch_policy" {
  name        = "raw-fetch-lambda-policy"
  description = "Allows writing raw payloads to S3"
  policy      = data.aws_iam_policy_document.raw_fetch_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "lambda_raw_fetch_attach" {
  role       = aws_iam_role.raw_fetch_lambda_role.name
  policy_arn = aws_iam_policy.raw_fetch_policy.arn
}

# ------------------------------------------------------------------------------
# Lambda Zip Archives
# ------------------------------------------------------------------------------
data "archive_file" "station_master_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/fetch_station_master_raw/lambda_function.py"
  output_path = "${path.module}/build/fetch_station_master_raw.zip"
}

data "archive_file" "station_active_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/fetch_station_active_raw/lambda_function.py"
  output_path = "${path.module}/build/fetch_station_active_raw.zip"
}

# ------------------------------------------------------------------------------
# Lambda Functions
# ------------------------------------------------------------------------------
resource "aws_lambda_function" "fetch_station_master_raw" {
  function_name    = "fetch_station_master_raw"
  role             = aws_iam_role.raw_fetch_lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 180
  memory_size      = 256

  filename         = data.archive_file.station_master_zip.output_path
  source_code_hash = data.archive_file.station_master_zip.output_base64sha256

  environment {
    variables = {
      RAW_BUCKET         = var.raw_bucket
      SEOUL_API_KEY      = var.seoul_api_key
      SEOUL_API_BASE_URL = var.seoul_api_base_url
    }
  }
}

resource "aws_lambda_function" "fetch_station_active_raw" {
  function_name    = "fetch_station_active_raw"
  role             = aws_iam_role.raw_fetch_lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 180
  memory_size      = 256

  filename         = data.archive_file.station_active_zip.output_path
  source_code_hash = data.archive_file.station_active_zip.output_base64sha256

  environment {
    variables = {
      RAW_BUCKET         = var.raw_bucket
      SEOUL_API_KEY      = var.seoul_api_key
      SEOUL_API_BASE_URL = var.seoul_api_base_url
    }
  }
}

# ------------------------------------------------------------------------------
# EventBridge Schedule - 생략: 이 계정에 events:PutRule 권한이 없다(SCP가 아니라
# 순수 IAM gap). 대신 airflow/dags/raw_fetch_lambda_dag.py가 매일 00:10 KST에
# lambda:InvokeFunction으로 이 두 Lambda를 직접 호출한다(lambda_shared.tf의
# airflow_worker_lambda_invoke_policy에 권한 포함). 권한이 열리면 되살릴 것.
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Monitoring & Alerting (CloudWatch Alarm -> SNS) - 생략: sns:CreateTopic 권한이
# 없다. notify_slack Lambda(Issue #180)도 SNS가 있어야 트리거되므로 같이 뺐다 -
# 이미 만들어진 notify_slack Lambda는 이번 apply에서 destroy될 수 있다(무해함,
# 어차피 아무것도 호출해주지 않아 동작 안 하고 있었음). 권한이 열리면 되살릴 것.
# ------------------------------------------------------------------------------
