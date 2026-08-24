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
# Dead Letter Queue (SQS)
# ------------------------------------------------------------------------------
resource "aws_sqs_queue" "raw_fetch_dlq" {
  name                      = "raw-fetch-lambda-dlq"
  message_retention_seconds = 1209600 # 14 days
}

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

  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.raw_fetch_dlq.arn]
  }
}

resource "aws_iam_policy" "raw_fetch_policy" {
  name        = "raw-fetch-lambda-policy"
  description = "Allows writing raw payloads to S3 and sending failed events to DLQ"
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

  dead_letter_config {
    target_arn = aws_sqs_queue.raw_fetch_dlq.arn
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

  dead_letter_config {
    target_arn = aws_sqs_queue.raw_fetch_dlq.arn
  }
}

# ------------------------------------------------------------------------------
# EventBridge Schedule (Daily 00:10 KST = 15:10 UTC)
# ------------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "daily_raw_fetch_schedule" {
  name                = "daily-raw-fetch-at-0010-kst"
  description         = "Triggers raw fetch Lambdas daily at 00:10 KST (15:10 UTC)"
  schedule_expression = "cron(10 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "target_station_master" {
  rule      = aws_cloudwatch_event_rule.daily_raw_fetch_schedule.name
  target_id = "fetch_station_master_raw_target"
  arn       = aws_lambda_function.fetch_station_master_raw.arn
}

resource "aws_cloudwatch_event_target" "target_station_active" {
  rule      = aws_cloudwatch_event_rule.daily_raw_fetch_schedule.name
  target_id = "fetch_station_active_raw_target"
  arn       = aws_lambda_function.fetch_station_active_raw.arn
}

resource "aws_lambda_permission" "allow_eventbridge_station_master" {
  statement_id  = "AllowExecutionFromEventBridgeStationMaster"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fetch_station_master_raw.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_raw_fetch_schedule.arn
}

resource "aws_lambda_permission" "allow_eventbridge_station_active" {
  statement_id  = "AllowExecutionFromEventBridgeStationActive"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fetch_station_active_raw.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_raw_fetch_schedule.arn
}

# ------------------------------------------------------------------------------
# Monitoring & Alerting (CloudWatch Alarm -> SNS)
# ------------------------------------------------------------------------------
resource "aws_sns_topic" "raw_fetch_alerts" {
  name = "raw-fetch-lambda-alerts"
}

resource "aws_cloudwatch_metric_alarm" "station_master_errors" {
  alarm_name          = "fetch_station_master_raw_errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when fetch_station_master_raw Lambda experiences execution errors"
  alarm_actions       = [aws_sns_topic.raw_fetch_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.fetch_station_master_raw.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "station_active_errors" {
  alarm_name          = "fetch_station_active_raw_errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when fetch_station_active_raw Lambda experiences execution errors"
  alarm_actions       = [aws_sns_topic.raw_fetch_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.fetch_station_active_raw.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "dlq_messages_visible" {
  alarm_name          = "raw_fetch_dlq_messages_visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alarm when raw fetch DLQ receives failed execution events"
  alarm_actions       = [aws_sns_topic.raw_fetch_alerts.arn]

  dimensions = {
    QueueName = aws_sqs_queue.raw_fetch_dlq.name
  }
}

# ------------------------------------------------------------------------------
# Slack Notification (SNS -> Lambda -> Slack Incoming Webhook) - Issue #180
# ------------------------------------------------------------------------------
# slack_webhook_url이 비어있는 환경(로컬/dev)에서는 이 리소스들을 전부 스킵한다 -
# 웹훅이 없는데 리소스를 만들면 알림이 항상 실패하기만 하고, terraform apply
# 자체를 막을 이유는 없다.
locals {
  slack_notifications_enabled = var.slack_webhook_url != ""
}

data "archive_file" "notify_slack_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/notify_slack/lambda_function.py"
  output_path = "${path.module}/build/notify_slack.zip"
}

resource "aws_iam_role" "notify_slack_lambda_role" {
  count              = local.slack_notifications_enabled ? 1 : 0
  name               = "notify-slack-lambda-exec-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "notify_slack_basic_execution" {
  count      = local.slack_notifications_enabled ? 1 : 0
  role       = aws_iam_role.notify_slack_lambda_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "notify_slack" {
  count            = local.slack_notifications_enabled ? 1 : 0
  function_name    = "notify_slack"
  role             = aws_iam_role.notify_slack_lambda_role[0].arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 10
  memory_size      = 128

  filename         = data.archive_file.notify_slack_zip.output_path
  source_code_hash = data.archive_file.notify_slack_zip.output_base64sha256

  environment {
    variables = {
      SLACK_WEBHOOK_URL = var.slack_webhook_url
    }
  }
}

resource "aws_sns_topic_subscription" "raw_fetch_alerts_to_slack" {
  count     = local.slack_notifications_enabled ? 1 : 0
  topic_arn = aws_sns_topic.raw_fetch_alerts.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.notify_slack[0].arn
}

resource "aws_lambda_permission" "allow_sns_notify_slack" {
  count         = local.slack_notifications_enabled ? 1 : 0
  statement_id  = "AllowExecutionFromSNSRawFetchAlerts"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notify_slack[0].function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.raw_fetch_alerts.arn
}
