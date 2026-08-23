output "station_master_lambda_arn" {
  description = "ARN of fetch_station_master_raw Lambda"
  value       = aws_lambda_function.fetch_station_master_raw.arn
}

output "station_active_lambda_arn" {
  description = "ARN of fetch_station_active_raw Lambda"
  value       = aws_lambda_function.fetch_station_active_raw.arn
}

output "eventbridge_schedule_rule_arn" {
  description = "ARN of EventBridge daily schedule rule"
  value       = aws_cloudwatch_event_rule.daily_raw_fetch_schedule.arn
}

output "dlq_arn" {
  description = "ARN of Dead Letter Queue (SQS)"
  value       = aws_sqs_queue.raw_fetch_dlq.arn
}

output "alerts_sns_topic_arn" {
  description = "ARN of SNS Alert Topic"
  value       = aws_sns_topic.raw_fetch_alerts.arn
}

# ---- serving_sync Lambda (#172) ----
output "serving_sync_ecr_repository_url" {
  description = "URL of the serving_sync Lambda ECR repository - docker push 대상"
  value       = aws_ecr_repository.serving_sync.repository_url
}

output "write_bike_risk_daily_lambda_arn" {
  description = "ARN of serving-sync-write-bike-risk-daily Lambda"
  value       = aws_lambda_function.write_bike_risk_daily.arn
}

output "write_station_daily_lambda_arn" {
  description = "ARN of serving-sync-write-station-daily Lambda"
  value       = aws_lambda_function.write_station_daily.arn
}

output "verify_serving_sync_lambda_arn" {
  description = "ARN of serving-sync-verify Lambda"
  value       = aws_lambda_function.verify_serving_sync.arn
}

output "serving_sync_dlq_arn" {
  description = "ARN of serving_sync Dead Letter Queue (SQS)"
  value       = aws_sqs_queue.serving_sync_dlq.arn
}

output "notify_slack_lambda_arn" {
  description = "ARN of notify_slack Lambda (null when slack_webhook_url is not set)"
  # local.slack_notifications_enabled는 sensitive 변수(slack_webhook_url)에서 파생돼
  # 조건식에 쓰면 이 output 전체가 sensitive로 오염된다 (terraform validate 확인).
  # 대신 count로 실제로 생성된 리소스 개수를 봐서 오염을 피한다.
  value = length(aws_lambda_function.notify_slack) > 0 ? aws_lambda_function.notify_slack[0].arn : null
}

# ---- EMR Serverless prod Spark 인프라 (#183) ----
output "emr_spark_application_id" {
  description = "ID of the EMR Serverless Spark application"
  value       = aws_emrserverless_application.emr_spark.id
}

output "emr_spark_execution_role_arn" {
  description = "ARN of the EMR Serverless job execution role"
  value       = aws_iam_role.emr_spark_execution_role.arn
}

output "emr_spark_ecr_repository_url" {
  description = "URL of the emr-spark-prod ECR repository - docker push target"
  value       = aws_ecr_repository.emr_spark.repository_url
}

output "iceberg_catalog_rds_endpoint" {
  description = "Endpoint of the dedicated iceberg_catalog RDS instance"
  value       = aws_db_instance.iceberg_catalog.address
}

output "iceberg_catalog_secret_arn" {
  description = "ARN of the Secrets Manager secret holding ICEBERG_JDBC_CATALOG_USER/PASSWORD"
  value       = aws_secretsmanager_secret.iceberg_catalog.arn
}
