"""Bronze rental_history/failure_report 공백 자동 복구 DAG (#195).

00:30에 전날 기준 D-2까지의 공백을 확인하고, 날짜별 mapped task로 각 원천을 독립
처리한다. 날짜 task는 워터마크를 직접 갱신하지 않으며, 모든 completion marker가
연속으로 성공한 뒤에만 원천별 워터마크를 전진시킨다.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

from dag_common import DEFAULT_ARGS, SEOUL_API_POOL, bash_job, load_env_file

TARGET_DATE = "{{ data_interval_end.in_timezone('Asia/Seoul').subtract(days=2).strftime('%Y-%m-%d') }}"


def _parse_dates(raw_json: str) -> list[str]:
    values = json.loads(raw_json or "[]")
    if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
        raise ValueError(f"gap 목록 형식이 올바르지 않음: {values!r}")
    return values


@dag(
    dag_id="bronze_historical_reconciliation",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "reconciliation", "rental_history", "failure_report"],
    doc_md=__doc__,
)
def bronze_historical_reconciliation():
    common_env = {
        "RECONCILIATION_TARGET_DATE": TARGET_DATE,
        "DAG_RUN_ID": "{{ run_id }}",
    }

    check_rental = BashOperator(
        task_id="check_rental_history_gap",
        bash_command=bash_job(
            "check_bronze_gap",
            "DATASET='rental_history' "
            f"RECONCILIATION_TARGET_DATE='{TARGET_DATE}' ",
        ),
        env=common_env,
        append_env=True,
        do_xcom_push=True,
    )
    check_failure = BashOperator(
        task_id="check_failure_report_gap",
        bash_command=bash_job(
            "check_bronze_gap",
            "DATASET='failure_report' "
            f"RECONCILIATION_TARGET_DATE='{TARGET_DATE}' ",
        ),
        env=common_env,
        append_env=True,
        do_xcom_push=True,
    )

    @task(task_id="parse_rental_history_gap")
    def parse_rental_history_gap(raw_json: str) -> list[str]:
        return _parse_dates(raw_json)

    @task(task_id="parse_failure_report_gap")
    def parse_failure_report_gap(raw_json: str) -> list[str]:
        return _parse_dates(raw_json)

    rental_dates = parse_rental_history_gap(check_rental.output)
    failure_dates = parse_failure_report_gap(check_failure.output)

    @task(task_id="assign_rental_history_api_keys")
    def assign_rental_history_api_keys(dates: list[str]) -> list[dict]:
        return [
            {"target_date": target_date, "api_key_slot": (index % 3) + 1}
            for index, target_date in enumerate(dates)
        ]

    @task(task_id="assign_failure_report_api_key")
    def assign_failure_report_api_key(dates: list[str]) -> list[dict]:
        return [{"target_date": target_date, "api_key_slot": 4} for target_date in dates]

    rental_requests = assign_rental_history_api_keys(rental_dates)
    failure_requests = assign_failure_report_api_key(failure_dates)

    @task(
        task_id="catchup_rental_history_date",
        pool=SEOUL_API_POOL,
        max_active_tis_per_dag=3,
        execution_timeout=timedelta(minutes=45),
    )
    def catchup_rental_history_date(target_date: str, api_key_slot: int) -> str:
        load_env_file()
        key = os.getenv(f"SEOUL_API_KEY{api_key_slot}")
        if not key:
            raise RuntimeError(f"SEOUL_API_KEY{api_key_slot}가 설정되지 않았습니다")
        env = os.environ.copy()
        env.update(
            {
                "SEOUL_API_KEY": key,
                "BACKFILL_TARGET_DATE": target_date,
                "COLLECTION_CUTOFF_AT": f"{target_date}T23:59:59+09:00",
                "SNAPSHOT_TYPE": "FINAL",
                "RENTAL_HISTORY_T0_ENABLED": "false",
                "RENTAL_HISTORY_FALLBACK_ENABLED": "false",
                "DAG_RUN_ID": os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "unknown"),
                "BACKFILL_STARTED_AT": pendulum.now("UTC").to_iso8601_string(),
                "PYTHONPATH": "/opt/airflow/ingestion:/opt/airflow/pylib",
            }
        )
        cwd = "/opt/airflow/ingestion"
        commands = [
            [sys.executable, "-m", "jobs.collect_rental_history_raw"],
            [sys.executable, "-m", "jobs.select_rental_history_snapshot"],
            [sys.executable, "-m", "jobs.promote_rental_history_raw"],
            [sys.executable, "-m", "jobs.write_rental_history_completion_marker"],
        ]
        results = [subprocess.run(command, cwd=cwd, env=env, check=False) for command in commands]
        if results[-1].returncode != 0:
            raise RuntimeError(f"rental_history {target_date} 처리 실패")
        return target_date

    @task(
        task_id="catchup_failure_report_date",
        pool=SEOUL_API_POOL,
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(minutes=30),
    )
    def catchup_failure_report_date(target_date: str, api_key_slot: int) -> str:
        load_env_file()
        key = os.getenv(f"SEOUL_API_KEY{api_key_slot}")
        if not key:
            raise RuntimeError(f"SEOUL_API_KEY{api_key_slot}가 설정되지 않았습니다")
        env = os.environ.copy()
        env.update(
            {
                "SEOUL_API_KEY": key,
                "TARGET_DATE": target_date,
                "DAG_RUN_ID": os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "unknown"),
                "PYTHONPATH": "/opt/airflow/ingestion:/opt/airflow/pylib",
            }
        )
        result = subprocess.run(
            [sys.executable, "-m", "jobs.catchup_failure_report_date"],
            cwd="/opt/airflow/ingestion",
            env=env,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"failure_report {target_date} 처리 실패")
        return target_date

    rental_catchup = catchup_rental_history_date.expand_kwargs(rental_requests)
    failure_catchup = catchup_failure_report_date.expand_kwargs(failure_requests)

    advance_rental = BashOperator(
        task_id="advance_rental_history_watermark",
        bash_command=bash_job(
            "advance_completion_watermark",
            "DATASET='rental_history' "
            f"RECONCILIATION_TARGET_DATE='{TARGET_DATE}' "
            "RECONCILIATION_FAIL_ON_INCOMPLETE='true' ",
        ),
        env=common_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    advance_failure = BashOperator(
        task_id="advance_failure_report_watermark",
        bash_command=bash_job(
            "advance_completion_watermark",
            "DATASET='failure_report' "
            f"RECONCILIATION_TARGET_DATE='{TARGET_DATE}' "
            "RECONCILIATION_FAIL_ON_INCOMPLETE='true' ",
        ),
        env=common_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    rental_catchup >> advance_rental
    failure_catchup >> advance_failure
    # gap이 0개인 zero-length map에서도 completion marker를 다시 확인해 워터마크를
    # 전진시킬 수 있어야 한다(이전 실행에서 marker 기록 후 WM 갱신만 실패한 경우).
    rental_requests >> advance_rental
    failure_requests >> advance_failure


bronze_historical_reconciliation()
