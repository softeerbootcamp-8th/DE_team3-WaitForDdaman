output "station_master_lambda_arn" {
  description = "ARN of fetch_station_master_raw Lambda"
  value       = aws_lambda_function.fetch_station_master_raw.arn
}

output "station_active_lambda_arn" {
  description = "ARN of fetch_station_active_raw Lambda"
  value       = aws_lambda_function.fetch_station_active_raw.arn
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
  description = "URL of the waitforddaman-emr-spark-prod ECR repository - docker push target"
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

# ---- bikeman_event_generator Lambda (#186) ----
output "bikeman_event_generator_ecr_repository_url" {
  description = "URL of the bikeman_event_generator Lambda ECR repository - docker push 대상"
  value       = aws_ecr_repository.bikeman_event_generator.repository_url
}

output "generate_collect_events_lambda_arn" {
  description = "ARN of bikeman-event-generator-generate-collect-events Lambda"
  value       = aws_lambda_function.generate_collect_events.arn
}

output "deploy_returned_bikes_lambda_arn" {
  description = "ARN of bikeman-event-generator-deploy-returned-bikes Lambda"
  value       = aws_lambda_function.deploy_returned_bikes.arn
}


