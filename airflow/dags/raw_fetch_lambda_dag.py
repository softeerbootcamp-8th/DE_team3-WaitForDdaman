# airflow/dags/raw_fetch_lambda_dag.py
"""
raw_fetch_lambda - 대여소정보(station_master)/실시간 대여정보(station_active) raw
스냅샷을 매일 00:10 KST에 S3 raw 영역으로 가져온다.

### 왜 Airflow가 이 스케줄을 맡는가
원래는 Terraform의 EventBridge 규칙(aws_cloudwatch_event_rule.daily_raw_fetch_schedule)이
이 두 Lambda(fetch_station_master_raw/fetch_station_active_raw)를 직접 트리거했다.
이 AWS 계정에 events:PutRule 권한이 없어(조직 SCP가 아니라 순수 IAM 권한 gap) 그 방식이
막혀서, 이미 매일 도는 Airflow 스케줄러가 대신 두 Lambda를 invoke하도록 옮겼다.
권한이 열리면 EventBridge로 되돌릴 수 있다 - 그 전까지는 이 DAG가 유일한 트리거 경로다.

### payload를 비워두는 이유
두 Lambda 다 event.get("snapshot_date")가 없으면 호출 시점의 KST 당일 날짜로 스스로
스냅샷을 찍는다(lambda_function.py 참고) - EventBridge도 input 없이 트리거했으므로
그 동작을 그대로 재현한다. 다른 DAG들처럼 `ds`(Airflow 데이터 인터벌 날짜)를
snapshot_date로 넘기면 안 된다 - 이 스케줄은 "실행 시각의 당일 스냅샷"이 목적이라
`ds`를 쓰면 하루 밀린 날짜가 찍힌다(대여소정보/실시간 대여정보는 과거 소급 조회가
안 되는 API라 한 번 밀리면 그 날짜는 영구 손실).

### 왜 두 Lambda가 서로 의존하지 않는가
station_master/station_active는 서로 다른 API고 실패해도 서로에게 영향이 없다 -
bronze_daily_batch_all_sources_dag.py와 같은 이유로 병렬 실행한다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.sdk import dag

from dag_common import DEFAULT_ARGS

# infra/terraform/main.tf의 aws_lambda_function.function_name과 반드시 같아야 한다.
FETCH_STATION_MASTER_RAW_LAMBDA = "fetch_station_master_raw"
FETCH_STATION_ACTIVE_RAW_LAMBDA = "fetch_station_active_raw"


@dag(
    dag_id="raw_fetch_lambda",
    schedule="10 0 * * *",  # 매일 00:10 KST - 기존 EventBridge 스케줄과 동일
    start_date=pendulum.datetime(2026, 8, 25, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "raw_fetch"],
    doc_md=__doc__,
)
def raw_fetch_lambda():
    LambdaInvokeFunctionOperator(
        task_id="fetch_station_master_raw",
        function_name=FETCH_STATION_MASTER_RAW_LAMBDA,
        invocation_type="RequestResponse",
        execution_timeout=timedelta(minutes=5),
    )
    LambdaInvokeFunctionOperator(
        task_id="fetch_station_active_raw",
        function_name=FETCH_STATION_ACTIVE_RAW_LAMBDA,
        invocation_type="RequestResponse",
        execution_timeout=timedelta(minutes=5),
    )


raw_fetch_lambda()
