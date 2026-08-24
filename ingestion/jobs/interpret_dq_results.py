"""
DQ 해석 에이전트 실행 (#217, 1차 버전)

dq.check_result_history에서 최근 N일 히스토리를 조회하고, 이번 실행 결과와 함께
LLM에 넘겨 이상 여부를 해석한다. Slack 알림은 다음 단계 - 이번엔 파일 저장
(_meta/dq/interpretation/...) + 콘솔 출력 스텁만 구현한다.

FAIL/ERROR가 하나도 없으면(전부 PASS/MONITOR) LLM을 호출하지 않고 스킵한다 - 매
배치마다 무조건 호출하면 이상이 없는 날에도 비용이 든다. ERROR는 config에 선언된
컬럼이 테이블에 없는 등 스키마 불일치로 지표 계산 자체가 실패한 경우다
(common/dq_assertions.py) - 값이 하나도 안 나온 상태라 FAIL과는 다르지만, 사람이
반드시 봐야 하는 상황이라 같은 조건으로 묶었다. MONITOR 체크(예: 결측률처럼 하드
threshold 없이 추이만 보는 지표)의 급변은 이 조건으로 못 잡는다는 트레이드오프가
있다 - 그건 다음 단계에서 필요해지면 다룬다(#217 파일럿 범위 밖).

사용법:
    EXECUTION_DATE=2026-08-24 DQ_SOURCE_NAME=rental_history python -m jobs.interpret_dq_results
"""
import json
import logging
import os

import config
from common.dq_interpreter import DEFAULT_LOOKBACK_DAYS, DEFAULT_MODEL, fetch_history, interpret
from common.iceberg_catalog import build_iceberg_catalog
from common.s3_utils import ensure_bucket, get_json, put_json
from jobs.run_dq_assertions_rental_history import pending_result_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def interpretation_key(source_name: str, execution_date: str) -> str:
    return f"_meta/dq/interpretation/{source_name}/{execution_date}.json"


def run(source_name: str | None = None, execution_date_str: str | None = None) -> dict:
    source_name = source_name or os.environ.get("DQ_SOURCE_NAME", "rental_history")
    execution_date_str = execution_date_str or os.environ["EXECUTION_DATE"]
    lookback_days = int(os.environ.get("DQ_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS))
    model = os.environ.get("DQ_INTERPRETER_MODEL", DEFAULT_MODEL)

    bucket = config.SETTINGS.raw_bucket
    pending = get_json(bucket, pending_result_key(source_name, execution_date_str))
    if not pending:
        raise RuntimeError(f"pending DQ 결과 없음: {source_name}/{execution_date_str}")
    current_run = pending["results"]

    if not any(r["pass_fail"] in ("FAIL", "ERROR") for r in current_run):
        logger.info(
            "%s/%s: FAIL/ERROR 없음(전부 PASS/MONITOR) - 해석 에이전트 호출 스킵",
            source_name, execution_date_str,
        )
        return None

    catalog = build_iceberg_catalog()
    history = fetch_history(catalog, source_name, execution_date_str, lookback_days)

    interpretation = interpret(
        source_name=source_name,
        execution_date=execution_date_str,
        current_run=current_run,
        history=history,
        lookback_days=lookback_days,
        model=model,
    )

    ensure_bucket(bucket)
    put_json(bucket, interpretation_key(source_name, execution_date_str), interpretation)

    # Slack 연동 스텁 - 실제 알림은 다음 단계에서 구현. 지금은 콘솔 출력 + 파일 저장까지만.
    logger.info(
        "[DQ 해석 결과 - %s / %s]\n%s",
        source_name, execution_date_str,
        json.dumps(interpretation, ensure_ascii=False, indent=2),
    )

    return interpretation


if __name__ == "__main__":
    run()
