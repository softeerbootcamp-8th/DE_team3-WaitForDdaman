"""Bronze rental_history/failure_report 공백 자동 복구 DAG (#195).

00:30에 전날 기준 D-2까지의 공백을 확인하고, 날짜별 mapped task로 각 원천을 독립
처리한다. 날짜 task는 워터마크를 직접 갱신하지 않으며, 모든 completion marker가
연속으로 성공한 뒤에만 원천별 워터마크를 전진시킨다.

rental_history는 prepare(수집+선택)/promote(승격+completion marker) 두 mapped task로
나뉜다. prepare는 SEOUL_API_POOL(키 1~3)에서 날짜별로 최대 3개까지 병렬 실행되지만,
promote는 BRONZE_RENTAL_HISTORY_COMMIT_POOL(slot=1)에서 날짜 순서 없이 1개씩만 돌아
같은 bronze.rental_history 테이블에 대한 PyIceberg commit이 동시에 충돌하지 않는다.
promote는 prepare 성공 여부와 무관하게 항상 실행되어(trigger_rule=all_done) 실패한
날짜도 completion marker에 FAILED로 남기고, downstream 워터마크는 그 지점에서 멈춘다.
promote는 prepare의 반환값(XCom)이 아니라 assign 단계의 원본 rental_requests를 기준으로
expand된다 - prepare mapped TI가 실패해 XCom을 못 남긴 날짜라도 promote 매핑에서
누락되지 않게 하기 위함이며, prepare -> promote는 값 의존 없는 태스크 의존성으로만
연결된다.
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

from dag_common import (
    BRONZE_RENTAL_HISTORY_COMMIT_POOL,
    DEFAULT_ARGS,
    SEOUL_API_POOL,
    bash_job,
    load_env_file,
)

INGESTION_DIR = os.getenv("INGESTION_DIR", "/opt/airflow/ingestion")
TARGET_DATE = "{{ data_interval_end.in_timezone('Asia/Seoul').subtract(days=2).strftime('%Y-%m-%d') }}"


def _parse_dates(raw_json: str) -> list[str]:
    values = json.loads(raw_json or "[]")
    if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
        raise ValueError(f"gap 목록 형식이 올바르지 않음: {values!r}")
    return values


def _current_run_id() -> str:
    try:
        from airflow.operators.python import get_current_context

        return get_current_context()["run_id"]
    except Exception:
        return os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


@dag(
    dag_id="bronze_historical_reconciliation",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
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

    def _rental_history_env(target_date: str, api_key_slot: int | None) -> dict:
        load_env_file()
        env = os.environ.copy()
        env.update(
            {
                "BACKFILL_TARGET_DATE": target_date,
                "COLLECTION_CUTOFF_AT": f"{target_date}T23:59:59+09:00",
                "SNAPSHOT_TYPE": "FINAL",
                "RENTAL_HISTORY_T0_ENABLED": "false",
                "RENTAL_HISTORY_FALLBACK_ENABLED": "false",
                "DAG_RUN_ID": _current_run_id(),
                "PYTHONPATH": f"{INGESTION_DIR}:/opt/airflow/pylib:{os.getenv('PYTHONPATH', '')}",
            }
        )
        if api_key_slot is not None:
            key = os.getenv(f"SEOUL_API_KEY{api_key_slot}")
            if not key:
                raise RuntimeError(f"SEOUL_API_KEY{api_key_slot}가 설정되지 않았습니다")
            env["SEOUL_API_KEY"] = key
        return env

    @task(
        task_id="prepare_rental_history_date",
        pool=SEOUL_API_POOL,
        max_active_tis_per_dag=3,
        execution_timeout=timedelta(minutes=45),
    )
    def prepare_rental_history_date(target_date: str, api_key_slot: int) -> dict:
        """날짜별 API 수집 + Raw snapshot 선택까지만 담당한다.

        Bronze Iceberg commit(promote)은 다루지 않으므로 이 task는 최대 3개 날짜가
        SEOUL_API_POOL 안에서 동시에 돌아도 안전하다 - 서로 다른 API 키를 쓰고, S3 Raw
        영역에도 날짜별로 분리된 key에만 쓰기 때문이다.
        """
        started_at = pendulum.now("UTC").to_iso8601_string()
        env = _rental_history_env(target_date, api_key_slot)
        env["BACKFILL_STARTED_AT"] = started_at
        cwd = INGESTION_DIR
        commands = [
            [sys.executable, "-m", "jobs.collect_rental_history_raw"],
            [sys.executable, "-m", "jobs.select_rental_history_snapshot"],
        ]
        for command in commands:
            res = subprocess.run(command, cwd=cwd, env=env, check=False)
            if res.returncode != 0:
                raise RuntimeError(f"rental_history {target_date} 준비(수집/선택) 실패: {' '.join(command)}")
        return {"target_date": target_date, "started_at": started_at}

    @task(
        task_id="promote_rental_history_date",
        pool=BRONZE_RENTAL_HISTORY_COMMIT_POOL,
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(minutes=45),
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def promote_rental_history_date(request: dict) -> str:
        """prepare 완료 여부와 무관하게 항상 실행해 promote + completion marker를 남긴다.

        rental_requests(assign 단계에서 만든 날짜 목록)를 그대로 expand 기준으로 쓴다 -
        prepare mapped TI의 반환값(XCom)에 expand하면, prepare 인스턴스가 실패해 XCom을
        못 남긴 날짜는 promote 쪽 매핑 자체가 만들어지지 못해 completion marker를 영영
        남길 수 없다. rental_requests 기준으로 매핑하고 prepare -> promote를 태스크
        의존성으로만 연결하면(값 의존 없음) prepare가 실패해도 같은 날짜의 promote가
        반드시 실행된다.

        prepare가 실패해 selection.json이 없더라도 promote_rental_history_raw/
        write_rental_history_completion_marker는 실제 S3 상태(manifest/promotion)를
        다시 읽어 COMPLETE/FAILED를 판정하므로, 여기서 먼저 raise해 marker 기록을
        건너뛰면 안 된다(원본 마커 없이 조용히 넘어가면 downstream 워터마크가 그 날짜의
        상태를 영원히 알 수 없게 된다). BRONZE_RENTAL_HISTORY_COMMIT_POOL(slot=1)이
        같은 bronze.rental_history 테이블에 대한 PyIceberg commit을 날짜 간에
        직렬화해서 snapshot 충돌을 없앤다.
        """
        target_date = request["target_date"]
        env = _rental_history_env(target_date, api_key_slot=None)
        cwd = INGESTION_DIR

        promote_res = subprocess.run(
            [sys.executable, "-m", "jobs.promote_rental_history_raw"],
            cwd=cwd,
            env=env,
            check=False,
        )
        marker_res = subprocess.run(
            [sys.executable, "-m", "jobs.write_rental_history_completion_marker"],
            cwd=cwd,
            env=env,
            check=False,
        )
        if promote_res.returncode != 0 or marker_res.returncode != 0:
            raise RuntimeError(
                f"rental_history {target_date} promote 처리 실패 "
                f"(promote={promote_res.returncode}, marker={marker_res.returncode})"
            )
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
                "DAG_RUN_ID": _current_run_id(),
                "PYTHONPATH": f"{INGESTION_DIR}:/opt/airflow/pylib:{os.getenv('PYTHONPATH', '')}",
            }
        )
        result = subprocess.run(
            [sys.executable, "-m", "jobs.catchup_failure_report_date"],
            cwd=INGESTION_DIR,
            env=env,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"failure_report {target_date} 처리 실패")
        return target_date

    rental_prepared = prepare_rental_history_date.expand_kwargs(rental_requests)
    rental_promoted = promote_rental_history_date.expand(request=rental_requests)
    rental_prepared >> rental_promoted
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

    rental_promoted >> advance_rental
    failure_catchup >> advance_failure
    # gap이 0개인 zero-length map에서도 completion marker를 다시 확인해 워터마크를
    # 전진시킬 수 있어야 한다(이전 실행에서 marker 기록 후 WM 갱신만 실패한 경우).
    rental_requests >> advance_rental
    failure_requests >> advance_failure


bronze_historical_reconciliation()
