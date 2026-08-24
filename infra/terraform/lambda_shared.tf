# ------------------------------------------------------------------------------
# Lambda 그룹(serving_sync #172, bikeman_event_generator #186) 공용 인프라.
#
# 두 그룹 다 VPC 안(프라이빗 서브넷)에서 Secrets Manager를 읽어야 하는데, NAT
# Gateway가 없는 구조라(#172가 S3는 Gateway Endpoint로 우회했지만 Secrets Manager는
# 빠뜨림 - PR #190 검토 중 발견) 지금 상태로는 콜드 스타트 시 시크릿을 못 읽어온다.
# Airflow 워커가 이 Lambda들을 호출할 IAM 권한도 지금까지 없었다 - 이 파일에서
# 두 gap을 한 번에 고친다(리소스가 VPC/Airflow 워커 레벨로 두 그룹이 공유하는
# 성격이라 serving_sync.tf/bikeman_event_generator.tf 어느 한쪽에 넣기 애매함).
# ------------------------------------------------------------------------------

# ---- Secrets Manager Interface VPC Endpoint ----
resource "aws_security_group" "secretsmanager_vpc_endpoint_sg" {
  name        = "secretsmanager-vpce-sg"
  description = "Allows Lambda security groups to reach the Secrets Manager VPC endpoint"
  vpc_id      = var.vpc_id

  ingress {
    # EMR Serverless 워커도 이 엔드포인트를 공유한다(emr_spark.tf) - Secrets
    # Manager용 Interface VPC Endpoint를 중복 생성하지 않기 위함(실측: 2026-08-24,
    # initial_load_failure_report_file_emr 태스크가 CloudWatch Logs 전송 시
    # Connect timeout으로 실패한 것과 같은 원인 - NAT가 없는 VPC라 인터페이스
    # 엔드포인트 없이는 EMR 워커가 이 서비스들에 아예 도달할 라우트가 없음).
    description = "HTTPS from serving_sync/bikeman_event_generator Lambdas and EMR Serverless workers"
    from_port   = 443
    to_port     = 443
      protocol    = "tcp"
      security_groups = [
        aws_security_group.serving_sync_lambda_sg.id,
        aws_security_group.bikeman_event_generator_lambda_sg.id,
        aws_security_group.emr_serverless_worker.id,
    ]
  }
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.subnet_ids
  security_group_ids  = [aws_security_group.secretsmanager_vpc_endpoint_sg.id]
  private_dns_enabled = true
}

# ---- Airflow 워커 -> Lambda 호출 권한 ----
data "aws_iam_policy_document" "airflow_worker_lambda_invoke_doc" {
  statement {
    effect  = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.write_bike_risk_daily.arn,
      aws_lambda_function.write_station_daily.arn,
      aws_lambda_function.verify_serving_sync.arn,
      aws_lambda_function.generate_collect_events.arn,
      aws_lambda_function.deploy_returned_bikes.arn,
    ]
  }
}

resource "aws_iam_policy" "airflow_worker_lambda_invoke_policy" {
  name        = "airflow-worker-lambda-invoke-policy"
  description = "Allows the Airflow worker role to invoke serving_sync and bikeman_event_generator Lambdas"
  policy      = data.aws_iam_policy_document.airflow_worker_lambda_invoke_doc.json
}

resource "aws_iam_role_policy_attachment" "airflow_worker_lambda_invoke_attach" {
  role       = var.airflow_worker_role_name
  policy_arn = aws_iam_policy.airflow_worker_lambda_invoke_policy.arn
}
