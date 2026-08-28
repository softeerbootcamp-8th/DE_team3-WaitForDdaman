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
# 생략: serving_sync/bikeman_event_generator가 결국 Secrets Manager를 안 쓰기로
# 해서(이 계정에 secretsmanager:CreateSecret 권한이 없음 - variables.tf 주석 참고)
# 이 엔드포인트가 더 이상 필요 없다. emr_serverless_worker SG 참조도 이 terraform
# 상태가 관리하지 않는 리소스(emr_spark.tf는 이미 별도로 배포됨)라 그대로 두면
# apply가 깨진다.

# ---- Airflow 워커 -> Lambda 호출 권한 ----
# fetch_station_master_raw/fetch_station_active_raw도 포함한다 - events:PutRule 권한이
# 없어 EventBridge 스케줄이 막혀서, airflow/dags/raw_fetch_lambda_dag.py가 매일
# 00:10 KST에 이 두 Lambda를 대신 invoke한다.
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
      aws_lambda_function.fetch_station_master_raw.arn,
      aws_lambda_function.fetch_station_active_raw.arn,
    ]
  }
}

resource "aws_iam_policy" "airflow_worker_lambda_invoke_policy" {
  name        = "airflow-worker-lambda-invoke-policy"
  description = "Allows the Airflow worker role to invoke serving_sync, bikeman_event_generator, and raw-fetch Lambdas"
  policy      = data.aws_iam_policy_document.airflow_worker_lambda_invoke_doc.json
}

resource "aws_iam_role_policy_attachment" "airflow_worker_lambda_invoke_attach" {
  role       = var.airflow_worker_role_name
  policy_arn = aws_iam_policy.airflow_worker_lambda_invoke_policy.arn
}
