# bikeman_event_generator Lambda 전환 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `bikeman_event_generator` DAG의 두 태스크(`generate_collect_events`, `deploy_returned_bikes`)를 Airflow 워커에서 직접 RDS에 붙는 방식(PostgresHook)에서 Lambda 호출 방식으로 옮겨, 워커의 DB 자격증명 의존을 제거한다.

**Architecture:** `serving_sync`(#172)와 동일한 패턴 - 경량 Docker 이미지 1개를 Lambda 함수 2개가 공유(`image_config.command`만 다름), Secrets Manager에서 `bikeman_writer` 자격증명을 콜드 스타트 시 읽어 환경변수로 채운 뒤 기존 job 모듈의 `run()`을 그대로 호출. DAG는 `PythonOperator` 2개를 `LambdaInvokeFunctionOperator` 2개로 교체. 같은 VPC를 쓰는 다른 Lambda 그룹(serving_sync)에도 있던 인프라 gap 2개(Secrets Manager VPC Endpoint, Airflow→Lambda invoke 권한)를 공유 리소스로 같이 고친다.

**Tech Stack:** Terraform (AWS Lambda/IAM/VPC/SQS/SNS/CloudWatch), Docker, Python 3.11, psycopg2, boto3, Airflow `LambdaInvokeFunctionOperator`

**Spec:** `docs/bikeman_event_generator Lambda 전환 설계.md`

## Global Constraints

- Terraform 코드는 작성하되 `terraform apply`는 하지 않는다 (스펙 8절) - 실제 값을 아는 사람이 검토 후 실행
- VPC/RDS/시크릿 ARN 값이 없는 변수는 기본값 없이 둔다 (`var.bikeman_db_secret_arn`, `var.airflow_worker_role_name`)
- `bikeman_db.py`/`event_builder.py`/`event_ids.py`의 쿼리·이벤트 생성 로직은 변경하지 않는다 (스펙 9절)
- 새 env var 접두사는 기존 `ingestion/common/db_client.py`가 이미 쓰는 `BIKEMAN_DB_*`(airflow_reader, 읽기전용)와 절대 겹치지 않게 `BIKEMAN_WRITER_DB_*`를 쓴다 - 이름이 같으면 두 개의 다른 권한 롤이 같은 변수명으로 헷갈릴 수 있다 (설계 문서에는 없던 세부사항, 구현 중 발견)
- terraform 문법 검증은 로컬에 CLI가 없으므로 `docker run --rm -v "$(pwd)/infra/terraform:/workspace" -w /workspace hashicorp/terraform:latest validate`로 한다 (검증 후 생긴 `.terraform/`, `.terraform.lock.hcl` 변경은 커밋하지 말고 되돌린다)

---

## Task 1: Terraform 변수 추가

**Files:**
- Modify: `infra/terraform/variables.tf` (파일 끝에 추가)

**Interfaces:**
- Produces: `var.bikeman_db_secret_arn`, `var.bikeman_event_generator_image_tag`, `var.bikeman_event_generator_reserved_concurrency`, `var.airflow_worker_role_name` — Task 2/4에서 참조

- [ ] **Step 1: variables.tf 끝에 새 변수 4개 추가**

`infra/terraform/variables.tf` 파일 맨 끝(기존 `serving_sync_reserved_concurrency` 변수 다음)에 아래를 추가:

```hcl

# ------------------------------------------------------------------------------
# bikeman_event_generator Lambda (#186) - serving_sync(#172)와 같은 VPC/RDS를
# 재사용한다(같은 domain-db 인스턴스, docs/RDS 적재 및 세팅 설계.md 2절 참고).
# ------------------------------------------------------------------------------
variable "bikeman_db_secret_arn" {
  description = "bikeman_writer 역할(BIKEMAN_WRITER_DB_HOST/PORT/NAME/USER/PASSWORD)을 담은 기존 Secrets Manager 시크릿 ARN"
  type        = string
}

variable "bikeman_event_generator_image_tag" {
  description = "infra/lambdas/bikeman_event_generator 이미지의 ECR 태그"
  type        = string
  default     = "latest"
}

variable "bikeman_event_generator_reserved_concurrency" {
  description = "generate_collect_events / deploy_returned_bikes 함수당 예약 동시성 (RDS 커넥션 수 제한)"
  type        = number
  default     = 2
}

# ------------------------------------------------------------------------------
# Airflow 워커 -> Lambda 호출 권한 (기존 gap - serving_sync 3개 + bikeman_event_
# generator 2개 전부에 필요했는데 지금까지 빠져있었다)
# ------------------------------------------------------------------------------
variable "airflow_worker_role_name" {
  description = "Airflow 워커(EC2)가 쓰는 기존 IAM 롤 이름 - lambda:InvokeFunction 정책을 여기 붙인다. EC2 인스턴스 롤은 Terraform 밖에서 수동 생성됨(#109)"
  type        = string
}
```

- [ ] **Step 2: 문법만 우선 확인 (아직 이 변수를 쓰는 리소스가 없어 validate는 Task 4 이후에)**

Run: `python3 -c "import re; content = open('infra/terraform/variables.tf').read(); assert content.count('variable \"') == content.count('description')"`
Expected: 에러 없이 종료 (variable 블록 수와 description 수가 같음 - 괄호 안 닫힌 실수 등 방지용 러프 체크)

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/variables.tf
git commit -m "chore: bikeman_event_generator Lambda 관련 terraform 변수 추가 (#186)"
```

---

## Task 2: `infra/terraform/bikeman_event_generator.tf` 신규 작성

**Files:**
- Create: `infra/terraform/bikeman_event_generator.tf`

**Interfaces:**
- Consumes: `var.vpc_id`, `var.subnet_ids`, `var.rds_security_group_id`, `var.aws_region` (기존 변수), `var.bikeman_db_secret_arn`, `var.bikeman_event_generator_image_tag`, `var.bikeman_event_generator_reserved_concurrency` (Task 1)
- Produces: `aws_security_group.bikeman_event_generator_lambda_sg`, `aws_lambda_function.generate_collect_events`, `aws_lambda_function.deploy_returned_bikes`, `aws_sqs_queue.bikeman_event_generator_dlq` — Task 3(lambda_shared.tf)과 Task 5(outputs.tf)에서 참조

- [ ] **Step 1: 파일 작성**

`infra/terraform/bikeman_event_generator.tf`:

```hcl
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
```

- [ ] **Step 2: Commit**

```bash
git add infra/terraform/bikeman_event_generator.tf
git commit -m "feat: bikeman_event_generator Lambda terraform 리소스 추가 (#186)"
```

---

## Task 3: 공유 gap 수정 - Secrets Manager VPC Endpoint + Airflow invoke 권한

**Files:**
- Create: `infra/terraform/lambda_shared.tf`

**Interfaces:**
- Consumes: `aws_security_group.serving_sync_lambda_sg` (기존, `serving_sync.tf`), `aws_security_group.bikeman_event_generator_lambda_sg` (Task 2), `aws_lambda_function.write_bike_risk_daily`/`write_station_daily`/`verify_serving_sync` (기존), `aws_lambda_function.generate_collect_events`/`deploy_returned_bikes` (Task 2), `var.airflow_worker_role_name` (Task 1)

- [ ] **Step 1: 파일 작성**

`infra/terraform/lambda_shared.tf`:

```hcl
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
    description = "HTTPS from serving_sync and bikeman_event_generator Lambdas"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    security_groups = [
      aws_security_group.serving_sync_lambda_sg.id,
      aws_security_group.bikeman_event_generator_lambda_sg.id,
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
```

- [ ] **Step 2: Commit**

```bash
git add infra/terraform/lambda_shared.tf
git commit -m "fix: Secrets Manager VPC Endpoint + Airflow Lambda invoke 권한 누락 수정 (#186)"
```

---

## Task 4: outputs.tf 추가 + terraform validate

**Files:**
- Modify: `infra/terraform/outputs.tf` (파일 끝에 추가)

- [ ] **Step 1: outputs.tf 끝에 추가**

```hcl

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

output "bikeman_event_generator_dlq_arn" {
  description = "ARN of bikeman_event_generator Dead Letter Queue (SQS)"
  value       = aws_sqs_queue.bikeman_event_generator_dlq.arn
}

output "secretsmanager_vpc_endpoint_id" {
  description = "ID of the Secrets Manager Interface VPC Endpoint shared by serving_sync and bikeman_event_generator Lambdas"
  value       = aws_vpc_endpoint.secretsmanager.id
}
```

- [ ] **Step 2: terraform validate로 Task 1-4 전체 검증**

Run:
```bash
cd /Users/admin/Desktop/DE_team3-WaitForDdaman
docker run --rm -v "$(pwd)/infra/terraform:/workspace" -w /workspace hashicorp/terraform:latest init -backend=false
docker run --rm -v "$(pwd)/infra/terraform:/workspace" -w /workspace hashicorp/terraform:latest validate
```
Expected: 마지막 줄이 `Success! The configuration is valid.`

- [ ] **Step 3: validate가 만든 로컬 상태 되돌리기**

```bash
cd /Users/admin/Desktop/DE_team3-WaitForDdaman
git checkout -- infra/terraform/.terraform.lock.hcl
rm -rf infra/terraform/.terraform
git status -sb infra/terraform/
```
Expected: `outputs.tf`만 수정된 상태로 나오고 `.terraform.lock.hcl`/`.terraform/`는 안 보임

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/outputs.tf
git commit -m "chore: bikeman_event_generator terraform outputs 추가 (#186)"
```

---

## Task 5: job 파일 연결 획득부 교체 (PostgresHook → 환경변수+psycopg2)

**Files:**
- Modify: `pipeline/bikeman_event_generator/jobs/generate_collect_events.py`
- Modify: `pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py`

**Interfaces:**
- Produces: 두 파일의 `run(target_date: str) -> int`는 시그니처 그대로 유지 — Task 6의 Lambda 핸들러가 그대로 호출

- [ ] **Step 1: `generate_collect_events.py` 수정**

`pipeline/bikeman_event_generator/jobs/generate_collect_events.py` 전체를 아래로 교체:

```python
"""
serving.bike_risk_daily의 최신 snapshot_date(<= target_date)에서 risk_score 상위 N대에
대해 COLLECT 이벤트를 생성한다. N은 BIKEMAN_COLLECT_LIMIT로 조정할 수 있고 기본값은
500이다.

### Lambda 전환 (#186)
기존엔 Airflow Connection(bikeman_postgres)의 PostgresHook으로 연결했으나, Lambda는
Airflow 컨텍스트가 없어 이 방식을 못 쓴다. serving_db.py/db_client.py와 동일한
컨벤션(psycopg2 + 환경변수)으로 되돌린다 - 이 파일도 `python -m jobs.X`로 Airflow
없이 단독 실행 가능해야 한다는 저장소 전체 컨벤션에 다시 맞춘 것.

BIKEMAN_WRITER_DB_* 접두사를 쓴다 - ingestion/common/db_client.py가 이미 BIKEMAN_DB_*를
airflow_reader(읽기 전용) 역할로 쓰고 있어서, 이 잡이 쓰는 bikeman_writer(쓰기 가능)
자격증명과 이름이 겹치면 안 된다.

사용법 (Lambda에서 호출됨, 단독 실행 시):
    BIKEMAN_WRITER_DB_HOST=... BIKEMAN_WRITER_DB_NAME=... BIKEMAN_WRITER_DB_USER=... \
    BIKEMAN_WRITER_DB_PASSWORD=... python -c "import generate_collect_events; generate_collect_events.run('2026-07-01')"
"""
import logging
import os
import random

import psycopg2

import bikeman_db
import event_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _connect():
    return psycopg2.connect(
        host=os.environ["BIKEMAN_WRITER_DB_HOST"],
        port=os.environ.get("BIKEMAN_WRITER_DB_PORT", "5432"),
        dbname=os.environ["BIKEMAN_WRITER_DB_NAME"],
        user=os.environ["BIKEMAN_WRITER_DB_USER"],
        password=os.environ["BIKEMAN_WRITER_DB_PASSWORD"],
        connect_timeout=10,
    )


def run(target_date: str) -> int:
    collect_limit = int(os.getenv("BIKEMAN_COLLECT_LIMIT", str(bikeman_db.COLLECT_LIMIT_DEFAULT)))
    conn = _connect()
    try:
        targets = bikeman_db.fetch_collect_targets(conn, target_date, limit=collect_limit)
        events = [
            event_builder.build_collect_event(
                t["bike_id"], t["station_id"], target_date, random.choice(event_builder.WORKER_POOL)
            )
            for t in targets
        ]
        written = bikeman_db.insert_events(conn, events)
    finally:
        conn.close()

    logger.info(
        "%s: risk_score 상위 %d대 중 COLLECT 대상 %d건, 신규 삽입 %d건",
        target_date,
        collect_limit,
        len(targets),
        written,
    )
    return written
```

- [ ] **Step 2: `deploy_returned_bikes.py` 수정**

`pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py` 전체를 아래로 교체:

```python
"""
전날(target_date - 1일) COLLECT되고 아직 미배치인 자전거를 원래 station_id로 DEPLOY한다.
"미배치"는 별도 플래그가 아니라 "그 자전거의 가장 최근 이벤트가 COLLECT"라는 사실 자체로
정의된다 - 이미 DEPLOY됐다면 가장 최근 이벤트는 DEPLOY이므로 자동으로 제외된다
(bikeman_db.fetch_deploy_targets의 WITH latest ... 쿼리 참고).

### Lambda 전환 (#186)
generate_collect_events.py와 동일한 이유 - PostgresHook 대신 psycopg2 + 환경변수
(BIKEMAN_WRITER_DB_*)로 연결한다.

사용법 (Lambda에서 호출됨, 단독 실행 시):
    BIKEMAN_WRITER_DB_HOST=... BIKEMAN_WRITER_DB_NAME=... BIKEMAN_WRITER_DB_USER=... \
    BIKEMAN_WRITER_DB_PASSWORD=... python -c "import deploy_returned_bikes; deploy_returned_bikes.run('2026-07-01')"
"""
import logging
import os
import random

import psycopg2

import bikeman_db
import event_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _connect():
    return psycopg2.connect(
        host=os.environ["BIKEMAN_WRITER_DB_HOST"],
        port=os.environ.get("BIKEMAN_WRITER_DB_PORT", "5432"),
        dbname=os.environ["BIKEMAN_WRITER_DB_NAME"],
        user=os.environ["BIKEMAN_WRITER_DB_USER"],
        password=os.environ["BIKEMAN_WRITER_DB_PASSWORD"],
        connect_timeout=10,
    )


def run(target_date: str) -> int:
    conn = _connect()
    try:
        targets = bikeman_db.fetch_deploy_targets(conn, target_date)
        events = [
            event_builder.build_deploy_event(
                t["bike_id"], t["station_id"], target_date, random.choice(event_builder.WORKER_POOL)
            )
            for t in targets
        ]
        written = bikeman_db.insert_events(conn, events)
    finally:
        conn.close()

    logger.info("%s: DEPLOY 대상(전날 COLLECT & 미배치) %d건 중 %d건 신규 삽입", target_date, len(targets), written)
    return written
```

- [ ] **Step 3: 문법 검증**

Run: `python3 -m py_compile pipeline/bikeman_event_generator/jobs/generate_collect_events.py pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py`
Expected: 에러 없이 종료

- [ ] **Step 4: import 검증 (DB 연결 없이 모듈 로드만)**

Run:
```bash
cd /Users/admin/Desktop/DE_team3-WaitForDdaman/pipeline/bikeman_event_generator/jobs
python3 -c "import sys; sys.path.insert(0, '.'); import generate_collect_events, deploy_returned_bikes; print('OK')"
```
Expected: `OK` 출력 (psycopg2가 로컬 venv에 없으면 `ModuleNotFoundError: No module named 'psycopg2'` - 그 경우 `/Users/admin/Desktop/DE_team3-WaitForDdaman/.venv/bin/python3`로 실행)

- [ ] **Step 5: Commit**

```bash
git add pipeline/bikeman_event_generator/jobs/generate_collect_events.py pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py
git commit -m "fix: bikeman_event_generator PostgresHook 제거, 환경변수+psycopg2로 전환 (#186)"
```

---

## Task 6: Lambda 앱 패키지 작성 (`_secrets.py` + 핸들러 2개)

**Files:**
- Create: `infra/lambdas/bikeman_event_generator/app/__init__.py`
- Create: `infra/lambdas/bikeman_event_generator/app/_secrets.py`
- Create: `infra/lambdas/bikeman_event_generator/app/generate_collect_events.py`
- Create: `infra/lambdas/bikeman_event_generator/app/deploy_returned_bikes.py`

**Interfaces:**
- Consumes: `pipeline/bikeman_event_generator/jobs/generate_collect_events.run(target_date)`, `deploy_returned_bikes.run(target_date)` (Task 5) - Dockerfile(Task 7)이 이 모듈들을 이미지 루트에 복사해서 `import generate_collect_events`/`import deploy_returned_bikes`로 그대로 불러쓴다
- Produces: `app.generate_collect_events.handler(event, context)`, `app.deploy_returned_bikes.handler(event, context)` - Task 2의 terraform `image_config.command`가 참조하는 이름과 반드시 일치해야 한다

- [ ] **Step 1: 빈 `__init__.py` 생성**

`infra/lambdas/bikeman_event_generator/app/__init__.py`: 빈 파일 (serving_sync와 동일)

- [ ] **Step 2: `_secrets.py` 작성**

`infra/lambdas/bikeman_event_generator/app/_secrets.py`:

```python
"""
Secrets Manager에서 bikeman_writer DB 자격증명을 읽어 os.environ에 채운다 (#186).

generate_collect_events.py/deploy_returned_bikes.py는 BIKEMAN_WRITER_DB_HOST/PORT/
NAME/USER/PASSWORD를 os.environ에서 직접 읽도록 짜여 있다(#186 완료 조건: 두 파일
쿼리 로직 변경 없음). 이 파일은 그 관례를 그대로 두면서, Lambda 실행 환경에서만
그 값들의 출처를 Secrets Manager로 바꾼다 - serving_sync의 _secrets.py(#172)와
동일한 패턴.

콜드 스타트 시(핸들러 모듈이 import될 때) 1회만 호출한다 - 웜 인스턴스가 재사용될
때마다 Secrets Manager를 다시 부르지 않기 위함. BIKEMAN_DB_SECRET_ARN이 없으면
(로컬 실행 등) 이미 환경변수가 채워져 있다고 보고 조용히 넘어간다.
"""
import json
import os

import boto3

_SECRET_ENV_KEYS = (
    "BIKEMAN_WRITER_DB_HOST",
    "BIKEMAN_WRITER_DB_PORT",
    "BIKEMAN_WRITER_DB_NAME",
    "BIKEMAN_WRITER_DB_USER",
    "BIKEMAN_WRITER_DB_PASSWORD",
)


def load_bikeman_db_secret() -> None:
    secret_arn = os.environ.get("BIKEMAN_DB_SECRET_ARN")
    if not secret_arn:
        return

    client = boto3.client("secretsmanager")
    secret = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])
    for key in _SECRET_ENV_KEYS:
        if key in secret:
            os.environ[key] = str(secret[key])
```

- [ ] **Step 3: `generate_collect_events.py` 핸들러 작성**

`infra/lambdas/bikeman_event_generator/app/generate_collect_events.py`:

```python
"""generate_collect_events의 Lambda 진입점 (#186).

핸들러는 얇게 유지한다 - 실제 로직은 pipeline/bikeman_event_generator/jobs/
generate_collect_events.py의 run()에 그대로 있다(로컬 `python -c "..."` 경로와
완전히 같은 코드). 여기서는 (1) Secrets Manager에서 DB 자격증명을 채우고 (2) event의
snapshot_date를 run()의 인자로 그대로 넘긴다.

Terraform의 image_config.command로 "app.generate_collect_events.handler"를 가리키면
이 handler가 진입점이 된다 - 이미지는 deploy_returned_bikes와 공유하고 함수(Lambda
리소스)만 둘로 나뉜다.
"""
from typing import Any

from ._secrets import load_bikeman_db_secret

load_bikeman_db_secret()

from generate_collect_events import run  # noqa: E402 (자격증명을 채운 뒤에 import해야 함)


def handler(event: dict, context: Any) -> dict:
    event = event or {}
    written = run(event["snapshot_date"])
    return {"statusCode": 200, "written": written}
```

- [ ] **Step 4: `deploy_returned_bikes.py` 핸들러 작성**

`infra/lambdas/bikeman_event_generator/app/deploy_returned_bikes.py`:

```python
"""deploy_returned_bikes의 Lambda 진입점 - generate_collect_events.py와 동일 패턴 (#186)."""
from typing import Any

from ._secrets import load_bikeman_db_secret

load_bikeman_db_secret()

from deploy_returned_bikes import run  # noqa: E402 (자격증명을 채운 뒤에 import해야 함)


def handler(event: dict, context: Any) -> dict:
    event = event or {}
    written = run(event["snapshot_date"])
    return {"statusCode": 200, "written": written}
```

- [ ] **Step 5: 문법 검증**

Run: `python3 -m py_compile infra/lambdas/bikeman_event_generator/app/_secrets.py infra/lambdas/bikeman_event_generator/app/generate_collect_events.py infra/lambdas/bikeman_event_generator/app/deploy_returned_bikes.py`
Expected: 에러 없이 종료

- [ ] **Step 6: Commit**

```bash
git add infra/lambdas/bikeman_event_generator/app/
git commit -m "feat: bikeman_event_generator Lambda 핸들러 작성 (#186)"
```

---

## Task 7: Dockerfile + requirements.txt 작성, 빌드 검증

**Files:**
- Create: `infra/lambdas/bikeman_event_generator/Dockerfile`
- Create: `infra/lambdas/bikeman_event_generator/requirements.txt`

**Interfaces:**
- Consumes: `pipeline/bikeman_event_generator/jobs/{bikeman_db.py, event_builder.py, event_ids.py, generate_collect_events.py, deploy_returned_bikes.py}` (Task 5, 기존 파일), `infra/lambdas/bikeman_event_generator/app/` (Task 6)

- [ ] **Step 1: `requirements.txt` 작성**

`infra/lambdas/bikeman_event_generator/requirements.txt`:

```
# generate_collect_events.py/deploy_returned_bikes.py가 실제로 쓰는 것만 담는다 -
# pyiceberg/pyarrow/pandas는 이 잡들에 없다(#186, RDS만 직접 조회/적재).
psycopg2-binary>=2.9
boto3>=1.34
```

- [ ] **Step 2: `Dockerfile` 작성**

`infra/lambdas/bikeman_event_generator/Dockerfile`:

```dockerfile
# bikeman_event_generator RDS 적재 Lambda 이미지 (#186)
#
# 이미지 1개를 generate_collect_events / deploy_returned_bikes 2개 Lambda 함수가
# 공유한다 - Terraform의 image_config.command만 함수별로 다르게 줘서(예:
# "app.generate_collect_events.handler") 이 이미지 안의 다른 진입점을 고른다.
#
# 빌드 컨텍스트가 저장소 루트여야 한다(pipeline/bikeman_event_generator/jobs/를
# 함께 COPY하므로) - 이 Dockerfile이 있는 디렉터리가 아니라 루트에서 실행:
#   docker build -f infra/lambdas/bikeman_event_generator/Dockerfile -t bikeman-event-generator-lambda .

FROM public.ecr.aws/lambda/python:3.11

# bikeman_event_generator 잡 본체 - 로컬 실행과 완전히 같은 코드를 그대로 쓴다.
COPY pipeline/bikeman_event_generator/jobs/bikeman_db.py ${LAMBDA_TASK_ROOT}/bikeman_db.py
COPY pipeline/bikeman_event_generator/jobs/event_builder.py ${LAMBDA_TASK_ROOT}/event_builder.py
COPY pipeline/bikeman_event_generator/jobs/event_ids.py ${LAMBDA_TASK_ROOT}/event_ids.py
COPY pipeline/bikeman_event_generator/jobs/generate_collect_events.py ${LAMBDA_TASK_ROOT}/generate_collect_events.py
COPY pipeline/bikeman_event_generator/jobs/deploy_returned_bikes.py ${LAMBDA_TASK_ROOT}/deploy_returned_bikes.py

# Lambda 진입점(app.<모듈>.handler) - Secrets Manager에서 DB 자격증명을 채운 뒤
# 위 잡의 run()을 그대로 부르는 얇은 래퍼.
COPY infra/lambdas/bikeman_event_generator/app ${LAMBDA_TASK_ROOT}/app

COPY infra/lambdas/bikeman_event_generator/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Terraform의 image_config.command가 함수별로 덮어쓴다 - 이건 기본값일 뿐.
CMD ["app.generate_collect_events.handler"]
```

- [ ] **Step 3: `event_ids.py`가 실제로 저 경로에 있는지 확인**

Run: `ls pipeline/bikeman_event_generator/jobs/event_ids.py`
Expected: 파일 경로가 그대로 출력됨 (없으면 Dockerfile의 해당 COPY 줄을 실제 파일명에 맞게 고친다)

- [ ] **Step 4: 이미지 빌드로 실제 검증** (serving_sync #172가 실측 검증한 것과 동일한 방식 - numpy/pyarrow 버전 문제가 실제로 있었으므로 반드시 빌드까지 해본다)

Run:
```bash
cd /Users/admin/Desktop/DE_team3-WaitForDdaman
docker build -f infra/lambdas/bikeman_event_generator/Dockerfile -t bikeman-event-generator-lambda .
```
Expected: `Successfully tagged bikeman-event-generator-lambda:latest`로 끝남. 실패하면 에러 메시지를 읽고 requirements.txt 버전 상한 조정 (serving_sync/requirements.txt가 겪은 numpy/pyarrow wheel 문제 참고 - 이번엔 numpy/pyarrow가 의존성에 없어 재현 가능성은 낮음)

- [ ] **Step 5: RIE(Runtime Interface Emulator)로 콜드 스타트까지 재현** (실제 invoke까지는 RDS/Secrets Manager 접근이 없어 실패하지만, "여기까지는 정상 동작"을 확인하는 게 목적)

Run:
```bash
docker run --rm -p 9000:8080 -e BIKEMAN_DB_SECRET_ARN=arn:aws:secretsmanager:ap-northeast-2:000000000000:secret:dummy bikeman-event-generator-lambda &
sleep 3
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"snapshot_date": "2026-08-22"}'
```
Expected: `_secrets.py`가 boto3로 Secrets Manager를 호출하려다 실패하는 에러(자격증명/네트워크 오류)가 나와야 정상 - `ModuleNotFoundError`나 `ImportError`가 나오면 Dockerfile의 COPY 경로나 requirements.txt를 다시 확인. 확인 후 `docker stop $(docker ps -q --filter ancestor=bikeman-event-generator-lambda)`로 정리

- [ ] **Step 6: Commit**

```bash
git add infra/lambdas/bikeman_event_generator/Dockerfile infra/lambdas/bikeman_event_generator/requirements.txt
git commit -m "feat: bikeman_event_generator Lambda Dockerfile 작성 (#186)"
```

---

## Task 8: DAG를 LambdaInvokeFunctionOperator로 전환

**Files:**
- Modify: `airflow/dags/bikeman_event_generator_dag.py` (전체 재작성)

**Interfaces:**
- Consumes: `bikeman-event-generator-generate-collect-events`, `bikeman-event-generator-deploy-returned-bikes` (Task 2의 terraform `function_name`과 반드시 일치)

- [ ] **Step 1: 파일 전체 교체**

`airflow/dags/bikeman_event_generator_dag.py` 전체를 아래로 교체:

```python
"""
bikeman_event_generator - Gold 마트 동기화 결과를 근거로 bikeman(현장 작업자)의
수거·배치 행동을 시뮬레이션해 bikeman.fact_worker_event에 이벤트를 적재한다.
gold_to_serving_sync의 verify_bike_risk_daily_sync가 끝나면 TriggerDagRunOperator로
트리거된다.

serving.bike_risk_daily에서 action 컬럼이 제거된 뒤에는 최신 snapshot의 risk_score
상위 500대를 generate_collect_events의 COLLECT 대상으로 삼는다. DEPLOY 이벤트는
기존 bikeman.fact_worker_event의 전날 COLLECT 이력 기준으로 계속 생성된다.

generate_collect_events/deploy_returned_bikes는 애초에 서로 다른 event_type/자전거
집합을 다루는 독립 작업으로 보고 병렬 실행했다. Task 9 E2E 백필 검증 중 deploy_
returned_bikes가 매번 대상 0건을 반환하는 문제를 발견했는데, 근본 원인은 이번
백필보다 먼저 적재돼 있던 2026-09-01 COLLECT 배치(Task 5)였다 - fetch_deploy_targets
가 "가장 최근 이벤트"를 occurred_at(비즈니스 날짜) 기준으로 판별하다 보니, 실제
삽입 시각과 무관하게 날짜값이 미래인 09-01 COLLECT가 07-18~08-17 전 구간에서
계속 "최신"으로 잡혀 매일의 "어제 COLLECT" 조회를 가려버렸다(자세한 검증은
E2E_VERIFICATION.md 참고). 그와 별개로, 두 태스크를 병렬로 두면 "수거" 스냅샷이
여러 날 재사용되는 상황에서 generate_collect_events가 오늘자 COLLECT를 deploy_
returned_bikes의 "어제 COLLECT" 조회보다 먼저 커밋해 같은 방식으로 대상을 놓칠
잠재적 여지도 있어, 재발 방지 차원에서 deploy_returned_bikes >> generate_collect_events
로 순서를 강제한다.

### Lambda 전환 (#186)
generate_collect_events/deploy_returned_bikes 둘 다 PostgresHook(Airflow Connection
bikeman_postgres)으로 워커에서 직접 RDS에 접속했었다. gold_to_serving_sync(#172)와
같은 이유(워커의 DB 자격증명 제거)로 Lambda로 옮긴다 - bikeman/serving 스키마가
같은 domain-db 인스턴스라(docs/RDS 적재 및 세팅 설계.md 2절), #172만 하고 이 DAG를
남겨두면 워커는 여전히 DB에 붙는 경로가 남아 그 변경의 실질 이득이 없다.
"""
import json
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.sdk import dag

from dag_common import notify_slack_on_failure

# Terraform(infra/terraform/bikeman_event_generator.tf)의 aws_lambda_function.
# function_name과 반드시 같아야 한다 (#186).
GENERATE_COLLECT_EVENTS_LAMBDA = "bikeman-event-generator-generate-collect-events"
DEPLOY_RETURNED_BIKES_LAMBDA = "bikeman-event-generator-deploy-returned-bikes"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_slack_on_failure,
}


def _lambda_payload() -> str:
    """LambdaInvokeFunctionOperator의 payload(JSON 문자열) - gold_to_serving_sync_dag.py의
    _lambda_payload()와 동일한 규칙. dag_run.conf의 snapshot_date를 자기 ds보다
    우선한다(트리거 상류 DAG가 처리한 날짜와 어긋나지 않게)."""
    return json.dumps({"snapshot_date": "{{ dag_run.conf.get('snapshot_date') or ds }}"})


@dag(
    dag_id="bikeman_event_generator",
    schedule=None,  # gold_to_serving_sync의 TriggerDagRunOperator로만 실행
    start_date=pendulum.datetime(2026, 8, 18, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["serving", "trigger_only"],
    doc_md=__doc__,
)
def bikeman_event_generator():
    generate_collect_events_task = LambdaInvokeFunctionOperator(
        task_id="generate_collect_events",
        function_name=GENERATE_COLLECT_EVENTS_LAMBDA,
        invocation_type="RequestResponse",
        payload=_lambda_payload(),
        execution_timeout=timedelta(minutes=10),
    )
    deploy_returned_bikes_task = LambdaInvokeFunctionOperator(
        task_id="deploy_returned_bikes",
        function_name=DEPLOY_RETURNED_BIKES_LAMBDA,
        invocation_type="RequestResponse",
        payload=_lambda_payload(),
        execution_timeout=timedelta(minutes=10),
    )

    # Task 9 E2E 백필 중 발견: 같은 "수거" 대상 자전거 목록이 매일 재사용되는 상황(gold
    # 스냅샷이 하나뿐이거나 백필처럼 연속 실행할 때)에서 두 태스크를 병렬로 두면
    # deploy_returned_bikes가 "어제 COLLECT"를 찾는 조회(fetch_deploy_targets, latest
    # event 기준)가 같은 실행의 generate_collect_events가 "오늘" COLLECT를 커밋한
    # 뒤에 실행될 경우 그 자전거의 최신 이벤트가 이미 오늘 COLLECT로 바뀌어버려
    # 대상을 0건으로 놓친다. deploy_returned_bikes를 먼저 끝내 오늘자 COLLECT가
    # 커밋되기 전에 어제자 조회를 마치도록 순서를 강제한다.
    deploy_returned_bikes_task >> generate_collect_events_task


bikeman_event_generator()
```

- [ ] **Step 2: 문법 검증**

Run: `python3 -m py_compile airflow/dags/bikeman_event_generator_dag.py`
Expected: 에러 없이 종료

- [ ] **Step 3: Commit**

```bash
git add airflow/dags/bikeman_event_generator_dag.py
git commit -m "feat: bikeman_event_generator DAG를 LambdaInvokeFunctionOperator로 전환 (#186)"
```

---

## Task 9: DagBag 파싱 검증 (실제 Airflow 환경)

**Files:** 없음 (검증 전용 태스크)

- [ ] **Step 1: docker-compose 로컬 환경이 떠 있는지 확인**

Run: `docker compose -f docker-compose.local.yml ps`
Expected: `airflow-scheduler`가 `Up`으로 나옴 (안 떠 있으면 `docker compose -f docker-compose.local.yml up -d` 먼저 실행)

- [ ] **Step 2: DagBag으로 실제 파싱 확인**

Run:
```bash
docker exec airflow-scheduler python -c "
from airflow.dag_processing.dagbag import DagBag
bag = DagBag(dag_folder='/opt/airflow/dags')
errors = {k: v for k, v in bag.import_errors.items() if 'bikeman_event_generator' in k}
print('import_errors:', errors)
dag = bag.get_dag('bikeman_event_generator')
print('dag found:', dag is not None)
if dag:
    print('tasks:', [t.task_id for t in dag.tasks])
"
```
Expected: `import_errors: {}`, `dag found: True`, `tasks: ['generate_collect_events', 'deploy_returned_bikes']` (다른 DAG의 `dag_common`/`dag_assets` 관련 에러가 같이 나올 수 있는데, 이 DagBag 수동 호출 방식 자체의 sys.path 차이 때문이라 무시해도 됨 - `bikeman_event_generator` 관련 에러가 없는지만 확인)

- [ ] **Step 3: 여기서 발견되는 문제가 있으면 해당 Task로 돌아가 수정**

`import_errors`에 `bikeman_event_generator`가 걸리면 보통 `dag_common` import 실패이거나 `LambdaInvokeFunctionOperator`의 `airflow.providers.amazon` 패키지 미설치 - 후자면 `airflow/requirements.txt`에 `apache-airflow-providers-amazon`이 이미 있는지 확인(gold_to_serving_sync_dag.py가 이미 쓰고 있으므로 이미 있을 것).

---

## Self-Review 체크리스트 (완료 후 직접 확인)

- [ ] 스펙 1~9절 전부 태스크로 커버됐는지: 1(연결 방식)→Task5, 2(시크릿 구조)→Task1/6, 3(이미지)→Task6/7, 4(연결 획득부)→Task5, 5(gap 2개)→Task3, 6(DAG)→Task8, 7(테스트)→Task5 Step3-4, 8(apply 안 함)→전 태스크에서 `-var` 없이 `validate`까지만, 9(비범위)→`bikeman_db.py` 등 변경 없음 확인됨
- [ ] `BIKEMAN_WRITER_DB_*`(Task5/6) 이름이 모든 파일(job 2개, `_secrets.py`, terraform 없음-시크릿 내용은 AWS 쪽)에서 일관되게 쓰였는지
- [ ] Lambda `function_name`(Task2 terraform) == DAG 상수(Task8) == 실제 문자열 일치 확인: `bikeman-event-generator-generate-collect-events`/`bikeman-event-generator-deploy-returned-bikes`