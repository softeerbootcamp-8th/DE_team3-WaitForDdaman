"""
전체 데이터셋 워터마크 정체(stale) 감지 - Issue #180

각 daily_batch/Silver/Gold 잡은 "완전히 성공했을 때만" 워터마크를 전진시킨다
(common.watermark.write_watermark 참고). 그래서 워터마크가 여러 날 정체됐다는 건
그 잡이 계속 실패하고 있거나 스케줄 자체가 멈췄다는 신호다.

Lambda 실행 에러/DLQ 적재(infra/lambdas/notify_slack)는 "무엇이 실행되다 실패했는지"를
바로 알려주지만, Airflow DAG 자체가 트리거되지 않거나(스케줄러 장애 등) 매번 조용히
스킵되는 경우는 잡히지 않는다. 이 체크는 결과 데이터(워터마크)를 기준으로 "파이프라인
전체가 멈췄는지"를 감지하는 별도 안전망이다.

Airflow에서는 PythonOperator로 run()을 호출한다 - 정체가 감지되면 예외를 던져 태스크를
실패시키고, dag_common.notify_slack_on_failure(on_failure_callback)가 Slack 알림을
보내게 한다.

사용법:
    python -m jobs.check_watermark_staleness
    MAX_STALE_DAYS=5 python -m jobs.check_watermark_staleness
"""
import logging
import os
from datetime import date

from common.watermark import read_watermark
from config.watermark_keys import DATASET_WATERMARK_KEYS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 매일 "어제까지" 처리하는 잡들의 정상 상태는 watermark == today - 1이다.
# 스케줄 실패/재시도/휴일 등 하루이틀 지연은 정상 변동으로 보고, 그보다 오래
# 정체됐을 때만 알린다.
DEFAULT_MAX_STALE_DAYS = 3

# station_master/station_active는 날짜 파라미터가 없는 "항상 전체 스냅샷" API라
# 증분 워터마크 개념이 없다 (jobs/set_watermark.py 참고) - 이 체크 대상에서 제외.
#
# config/watermark_keys.py의 단일 소스를 그대로 쓴다 - jobs/set_watermark.py도 동일한
# 딕셔너리를 참조하므로, 새 데이터셋이 추가돼도 두 잡이 항상 동기화된다.
WATERMARK_DATASETS = DATASET_WATERMARK_KEYS


class WatermarkStalenessError(Exception):
    """하나 이상의 워터마크가 기준일보다 오래 정체됐을 때 발생한다."""


def stale_datasets(as_of: date, max_stale_days: int) -> list[dict]:
    """기준일(as_of) 대비 max_stale_days보다 더 정체된 데이터셋 목록을 반환한다."""
    stale = []
    for dataset, watermark_key in WATERMARK_DATASETS.items():
        watermark = read_watermark(watermark_key=watermark_key)
        days_stale = (as_of - watermark).days
        if days_stale > max_stale_days:
            stale.append({
                "dataset": dataset,
                "watermark_key": watermark_key,
                "last_processed_date": watermark.isoformat(),
                "days_stale": days_stale,
            })
    return stale


def run() -> None:
    max_stale_days = int(os.getenv("MAX_STALE_DAYS", str(DEFAULT_MAX_STALE_DAYS)))
    as_of = date.today()

    stale = stale_datasets(as_of, max_stale_days)
    if not stale:
        logger.info(
            "워터마크 정상 - 전체 %d개 데이터셋 모두 %d일 이내 (기준일=%s)",
            len(WATERMARK_DATASETS), max_stale_days, as_of,
        )
        return

    details = "; ".join(
        f"{s['dataset']}(워터마크={s['last_processed_date']}, {s['days_stale']}일 정체)"
        for s in stale
    )
    message = f"워터마크 정체 감지 ({len(stale)}개 데이터셋, 기준 {max_stale_days}일 초과): {details}"
    logger.error(message)
    raise WatermarkStalenessError(message)


if __name__ == "__main__":
    run()
