"""Iceberg 신규 테이블 생성 수동 Bootstrap DAG (Issue #216).

신규 LocalStack/AWS 환경에서는 Iceberg warehouse와 JDBC 카탈로그가 비어 있어 Initial
Load/Daily Batch가 `load_table()` 단계에서 실패한다. Initial Load/Daily Batch를 처음
실행하기 전에 이 DAG를 한 번 수동으로 실행해야 한다(운영 실행 순서):

    인프라 배포 -> 이 DAG(bootstrap_iceberg_tables) -> Initial Load -> Daily Batch

`create_bronze_tables` 하나만 실행한다. 필수 Bronze 테이블이 이미 있으면 스킵하고,
없으면 새로 생성하므로 반복 실행해도 중복 테이블/데이터가 생기지 않으며 기존 데이터도
훼손하지 않는다.

Airflow UI에서 Trigger DAG (설정 입력 불필요) 로 실행한다.
"""
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
import pendulum

from dag_common import DEFAULT_ARGS, bash_job

from dag_common import BRONZE_POOL


@dag(
    dag_id="bootstrap_iceberg_tables",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "manual"],
    doc_md=__doc__,
)
def bootstrap_iceberg_tables():
    BashOperator(
        task_id="create_bronze_tables",
        bash_command=bash_job("bootstrap_iceberg_tables"),
        pool=BRONZE_POOL,
    )


bootstrap_iceberg_tables()
