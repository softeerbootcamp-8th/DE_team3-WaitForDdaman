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
