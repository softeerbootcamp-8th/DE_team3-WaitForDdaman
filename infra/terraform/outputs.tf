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
