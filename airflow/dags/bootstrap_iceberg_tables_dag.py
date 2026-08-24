"""Iceberg 신규 테이블 + JDBC 카탈로그 등록 수동 Bootstrap DAG (Issue #216).

신규 LocalStack/AWS 환경에서는 Iceberg warehouse와 JDBC 카탈로그가 비어 있어 Initial
Load/Daily Batch가 `load_table()` 단계에서 실패한다. Initial Load/Daily Batch를 처음
실행하기 전에 이 DAG를 한 번 수동으로 실행해야 한다(운영 실행 순서):

    인프라 배포 -> 이 DAG(bootstrap_iceberg_tables) -> Initial Load -> Daily Batch

두 태스크는 서로 다른 관심사를 각각 멱등하게 처리하고, 순서대로 실행한다
(jobs/bootstrap_iceberg_tables.py 모듈 docstring 참고):

    1. register_existing_hadoop_tables
       jobs/register_tables_in_jdbc_catalog.py - 기존 Hadoop Catalog(S3 warehouse)에
       이미 있는 테이블의 metadata.json 위치를 JDBC 카탈로그에 포인터로만 등록한다
       (데이터/메타데이터 파일 재작성 없음). 등록할 대상이 없는 신규 환경에서도
       0개 등록으로 정상 종료한다.
    2. bootstrap_new_tables
       jobs/bootstrap_iceberg_tables.py - 1번으로도 채워지지 않는(어디에도 아직 없는)
       필수 Bronze 테이블을 새로 만든다. 특히 bronze.station_active는 별도 Initial
       Load 경로가 없어 이 태스크가 유일한 테이블 생성 경로다.

두 태스크 모두 "이미 있으면 스킵"하는 멱등 잡이라 반복 실행해도 중복 테이블/데이터가
생기지 않고, 기존 데이터도 훼손하지 않는다. bootstrap_new_tables는 1번 태스크의
성공/스킵 여부와 무관하게 항상 실행되도록 trigger_rule="all_done"을 둔다 - 신규
환경(등록할 Hadoop metadata 자체가 없는 환경)에서도 테이블 생성이 막히면 안 된다.

Airflow UI에서 Trigger DAG (설정 입력 불필요) 로 실행한다.
"""
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
import pendulum

from dag_common import DEFAULT_ARGS, bash_job

# register_tables_in_jdbc_catalog는 S3 전체를 스캔하고 Spark 세션을 띄운다 - 다른
# Bronze 잡들과 동일하게 워커 메모리 가드 풀에 넣어 동시 실행 수를 제한한다.
from dag_common import BRONZE_POOL


@dag(
    dag_id="bootstrap_iceberg_tables",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "manual", "bootstrap"],
    doc_md=__doc__,
)
def bootstrap_iceberg_tables():
    register_existing_hadoop_tables = BashOperator(
        task_id="register_existing_hadoop_tables",
        bash_command=bash_job("register_tables_in_jdbc_catalog"),
        pool=BRONZE_POOL,
    )

    bootstrap_new_tables = BashOperator(
        task_id="bootstrap_new_tables",
        bash_command=bash_job("bootstrap_iceberg_tables"),
        trigger_rule="all_done",
    )

    register_existing_hadoop_tables >> bootstrap_new_tables


bootstrap_iceberg_tables()
