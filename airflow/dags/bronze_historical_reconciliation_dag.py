"""Bronze rental_history/failure_report 공백 자동 복구 DAG (#195).

00:30에 전날 기준 D-2까지의 공백을 확인하고, 날짜별 mapped task로 각 원천을 독립
처리한다. 날짜 task는 워터마크를 직접 갱신하지 않으며, 모든 completion marker가
연속으로 성공한 뒤에만 원천별 워터마크를 전진시킨다.

rental_history는 prepare(Raw 수집)/promote(Bronze 승격 + completion marker) 두 단계로
나뉜다. Catchup은 일 배치와 달리 후보 선택을 하지 않는다 - 이미 지나간 확정 날짜라
관측본이 하나뿐이고 그 key도 target_date 23:59:59 KST로 결정적이라 고를 것이 없다.

prepare는 RENTAL_HISTORY_API_POOL(키 1~3, slot=3)에서 날짜별로 최대 3개까지 동시에
수집한다. 수집이 실패하면 그 날짜의 prepare task가 실패하고 배치 구성에서 빠지므로,
승격 쪽에서 manifest를 다시 검증하지 않는다 - 수집 성공의 진실은 prepare 한 곳에만 둔다.
실패한 날짜는 completion marker가 없어 advance_completion_watermark가 그 지점에서 멈춘다.

promote는 수집에 성공한 날짜를 6일씩 묶어 BRONZE_RENTAL_HISTORY_COMMIT_POOL(slot=1)에서
한 번의 Iceberg overwrite_partitions commit으로 반영하고, 커밋 성공 뒤에만 날짜별
promotion/completion marker를 남긴다(marker-last).

failure_report는 FAILURE_REPORT_API_POOL(키 4, slot=1)에서 날짜별로 수집과 Bronze 적재,
completion marker 기록을 한 태스크에서 끝낸다 - 날짜당 볼륨이 작아 커밋을 묶을 이득이
없고, 단계를 쪼개면 중간 산출물만 늘어난다.

API 풀을 원천별로 나눈 이유는 dag_common.py의 RENTAL_HISTORY_API_POOL 주석 참고.
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
    FAILURE_REPORT_API_POOL,
    RENTAL_HISTORY_API_POOL,
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
        pool=RENTAL_HISTORY_API_POOL,
        max_active_tis_per_dag=3,
        execution_timeout=timedelta(minutes=45),
    )
    def prepare_rental_history_date(target_date: str, api_key_slot: int) -> str:
        """날짜별 API 수집만 담당한다. 후보 선택(selection)은 하지 않는다.

        Catchup은 관측본이 하나뿐이고 그 key도 target_date 23:59:59 KST로 결정적이라
        고를 것이 없다 - 일 배치의 select_rental_history_snapshot을 쓰지 않는 이유다.

        수집이 실패하면 이 task가 실패하고, 실패한 날짜는 승격으로 넘어가지 않는다.
        그래서 승격 쪽에서 manifest를 다시 검증하지 않는다 - 수집 성공 여부의 진실은
        여기 한 곳에만 둔다.

        Bronze Iceberg commit은 다루지 않으므로 최대 3개 날짜가 RENTAL_HISTORY_API_POOL
        안에서 동시에 돌아도 안전하다 - 서로 다른 API 키를 쓰고, S3 Raw 영역에도 날짜별로
        분리된 key에만 쓰기 때문이다.
        """
        env = _rental_history_env(target_date, api_key_slot)
        env["BACKFILL_STARTED_AT"] = pendulum.now("UTC").to_iso8601_string()
        res = subprocess.run(
            [sys.executable, "-m", "jobs.collect_rental_history_raw"],
            cwd=INGESTION_DIR, env=env, check=False,
        )
        if res.returncode != 0:
            raise RuntimeError(f"rental_history {target_date} Raw 수집 실패")
        return target_date

    @task(task_id="build_rental_history_promote_batches")
    def build_rental_history_promote_batches(prepared_dates: list[str]) -> list[dict]:
        """수집에 성공한 날짜만 결정적 순서로 묶는다.

        prepare의 반환값(성공한 날짜)을 기준으로 삼는다 - 수집이 실패한 날짜는 여기
        오지 않으므로 승격 대상에서 자연히 빠지고, 그 날짜는 completion marker가 없어
        워터마크가 그 지점에서 멈춘다.

        순서는 그대로 유지한다(정렬하지 않는다) - 재실행 시 같은 그룹이 재현돼야
        배치 단위 재시도가 예측 가능해진다.
        """
        size = max(1, int(os.getenv("RENTAL_HISTORY_PROMOTE_BATCH_SIZE", "6")))
        dates = [d for d in prepared_dates if d]
        return [{"dates": dates[i : i + size]} for i in range(0, len(dates), size)]

    @task(
        task_id="promote_rental_history_batch",
        pool=BRONZE_RENTAL_HISTORY_COMMIT_POOL,
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(minutes=60),
    )
    def promote_rental_history_batch(dates: list[str]) -> list[str]:
        """prepare가 수집을 마친 날짜를 묶어 Bronze에 단일 commit하고 날짜별 marker를 남긴다.

        manifest를 다시 검증하지 않는다 - 수집 성공은 prepare task가 이미 보장했고,
        실패한 날짜는 배치 구성에서 빠져 여기 오지 않는다. 그 날짜는 completion marker가
        없으므로 advance_completion_watermark가 그 지점에서 멈춘다.

        BRONZE_RENTAL_HISTORY_COMMIT_POOL(slot=1)이 같은 bronze.rental_history 테이블에
        대한 PyIceberg commit을 배치 간에 직렬화해 snapshot 충돌을 없앤다.
        """
        env = _rental_history_env(dates[0], api_key_slot=None)
        env["BACKFILL_TARGET_DATES"] = ",".join(dates)

        promote_res = subprocess.run(
            [sys.executable, "-m", "jobs.promote_rental_history_catchup_batch"],
            cwd=INGESTION_DIR, env=env, check=False,
        )
        # marker는 날짜별로 남긴다 - 배치 커밋이 끝난 뒤 각 날짜의 promotion 문서를
        # 되읽어 COMPLETE/FAILED를 판정하는 기존 잡을 그대로 쓴다.
        marker_failures = []
        for target_date in dates:
            marker_env = _rental_history_env(target_date, api_key_slot=None)
            marker_res = subprocess.run(
                [sys.executable, "-m", "jobs.write_rental_history_completion_marker"],
                cwd=INGESTION_DIR, env=marker_env, check=False,
            )
            if marker_res.returncode != 0:
                marker_failures.append(target_date)

        if promote_res.returncode != 0:
            raise RuntimeError(f"rental_history 배치 승격 실패: {dates}")
        if marker_failures:
            raise RuntimeError(f"rental_history completion marker 기록 실패: {marker_failures}")
        return dates

    @task(
        task_id="catchup_failure_report_date",
        pool=FAILURE_REPORT_API_POOL,
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(minutes=30),
    )
    def catchup_failure_report_date(target_date: str, api_key_slot: int) -> str:
        """하루치 수집과 Bronze 적재, completion marker 기록을 한 태스크에서 끝낸다.

        rental_history와 달리 나누지 않는다 - 고장신고는 날짜당 볼륨이 작아 커밋을 묶을
        이득이 없고, 수집과 적재를 쪼개면 중간 산출물(Raw manifest)만 늘어난다.
        """
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
    rental_batches = build_rental_history_promote_batches(rental_prepared)
    rental_promoted = promote_rental_history_batch.expand_kwargs(rental_batches)

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
