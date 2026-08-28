"""
silver_rental_history 백필 청크 계획 계산 (#232)

initial_load_dag.py의 load_silver_rental_history 태스크가
기존에는 bash for-loop로 transform_silver_rental_history.py를 반복 호출하며 매
반복마다 잡이 스스로 워터마크를 읽어 다음 구간을 정했다 - 이 방식은 한 반복이
실패하면 루프 전체가 멈추고(#232), 반복들이 서로 의존해 병렬화할 수 없었다.

이 잡은 그 대신 Bronze/Silver 워터마크를 한 번만 읽어서 처리해야 할 전체 구간을
CHUNK_DAYS 크기로 미리 다 잘라 JSON으로 출력한다. DAG가 이 목록을 Airflow dynamic
task mapping(.expand())으로 펼치면, 각 청크가 서로 독립적인 태스크 인스턴스가 되어
하나가 실패해도 나머지가 계속 진행되고 개별 재시도가 가능해진다
(list_input_files -> parse_*_files -> .expand() 패턴과 동일한 구조).

### 단순 목록 -> 계획 객체
예전에는 범위 목록만 출력했다. 그러면 118개 청크 중 하나만 실패해도 다음 DAG Run이
이전 Silver 워터마크부터 같은 목록을 다시 만들어, 이미 성공한 117개를 전부 재계산했다.
이제는 두 목록을 나눠 출력한다.

  - all_ranges     : 이번 실행이 책임지는 결정론적 전체 구간. finalizer가 연속 완료
                     구간을 계산할 때 이 목록을 순서대로 훑는다.
  - pending_ranges : 같은 contract version의 COMPLETE marker가 없는 구간. Dynamic Task
                     Mapping은 이것만 펼쳐, 이미 끝난 청크는 Task Instance조차 만들지
                     않는다(S3 marker 확인 외의 비용을 쓰지 않는다).

bronze_watermark_at_start를 계획에 박아두는 이유는 finalizer가 DAG 실행 중 더 전진했을
수도 있는 Bronze 워터마크가 아니라 "이번 실행이 시작할 때 고정한 상한"만 보게 하기
위함이다.

청크 경계는 CHUNK_DAYS가 결정한다. 재실행 사이에 CHUNK_DAYS를 바꾸면 marker의
start/end가 달라져 기존 marker가 매칭되지 않고 새 경계로 전부 다시 처리된다 - 잘못
재사용하는 것보다 안전한 쪽이다.

각 청크는 transform_silver_rental_history.py를 BACKFILL_RANGE_START/END로 호출한다
(그 잡의 run() 참고) - 워터마크는 이 잡도, 각 청크도 쓰지 않는다. 모든 청크가 끝난
뒤 DAG의 마무리 태스크(advance_silver_rental_history_watermark)가 연속 완료 구간까지만
한 번에 전진시킨다.

사용법:
    CHUNK_DAYS=31 TOTAL_DAYS_CAP=3650 python -m jobs.compute_silver_rental_history_backfill_ranges
"""
import json
import logging
import os
from datetime import date, timedelta

import config
from common.s3_utils import ensure_bucket
from common.silver_rental_history_completion import (
    SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
    is_range_complete,
)
from common.watermark import read_watermark
from config.watermark_keys import DATASET_WATERMARK_KEYS
from jobs.rental_history_snapshot_policy import parse_max_days

logger = logging.getLogger(__name__)

SILVER_WATERMARK_KEY = DATASET_WATERMARK_KEYS["silver_rental_history"]
DEFAULT_CHUNK_DAYS = 31

# 기본은 상한 없음. 예전 기본값 3650(10년)은 초기 적재를 조용히 잘랐다 - 2015-01-01
# 시작 계획이 2024-12-28에서 끊겨 118청크가 전부 성공하고 DAG도 success로 끝났는데
# Bronze(2026-06-30)까지 1년 반이 남았고, 워터마크를 직접 보기 전엔 알 수 없었다.
# 청크가 marker로 독립 재시작되므로(#232 이후) 계획이 길어도 실패 범위가 번지지 않는다.
# 한 실행을 일부러 짧게 끊고 싶으면 TOTAL_DAYS_CAP에 숫자를 넣는다.
DEFAULT_TOTAL_DAYS_CAP = None


def build_ranges(
    silver_watermark: date, bronze_watermark: date, chunk_days: int, total_days_cap: int | None
) -> list[dict]:
    """Silver 워터마크 다음 날부터 Bronze 상한까지를 chunk_days 크기로 자른다.

    total_days_cap이 None이면 상한을 걸지 않고 Bronze 워터마크까지 전부 계획에 넣는다.
    """
    start = silver_watermark + timedelta(days=1)
    end = bronze_watermark
    if total_days_cap is not None:
        end = min(end, start + timedelta(days=total_days_cap - 1))

    ranges = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        ranges.append({"start": cursor.strftime("%Y-%m-%d"), "end": chunk_end.strftime("%Y-%m-%d")})
        cursor = chunk_end + timedelta(days=1)
    return ranges


def select_pending_ranges(bucket: str, ranges: list[dict], contract_version: int) -> list[dict]:
    """COMPLETE marker가 없는 구간만 남긴다 (marker 판정은 완료 모듈 하나가 담당)."""
    pending = []
    for chunk in ranges:
        if is_range_complete(bucket, chunk["start"], chunk["end"], contract_version):
            logger.info("완료 marker 재사용: %s~%s", chunk["start"], chunk["end"])
            continue
        pending.append(chunk)
    return pending


def build_plan(
    silver_watermark: date,
    bronze_watermark: date,
    chunk_days: int,
    total_days_cap: int,
    bucket: str,
    contract_version: int = SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
) -> dict:
    all_ranges = build_ranges(silver_watermark, bronze_watermark, chunk_days, total_days_cap)
    pending_ranges = select_pending_ranges(bucket, all_ranges, contract_version)
    return {
        "silver_watermark_before": silver_watermark.strftime("%Y-%m-%d"),
        "bronze_watermark_at_start": bronze_watermark.strftime("%Y-%m-%d"),
        "contract_version": contract_version,
        "all_ranges": all_ranges,
        "pending_ranges": pending_ranges,
    }


def run(chunk_days: int, total_days_cap: int) -> dict:
    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)  # marker prefix를 읽으려면 버킷이 있어야 한다 (신규 환경 안전장치)

    bronze_watermark = read_watermark()
    silver_watermark = read_watermark(watermark_key=SILVER_WATERMARK_KEY)
    plan = build_plan(silver_watermark, bronze_watermark, chunk_days, total_days_cap, bucket)

    logger.info(
        "백필 계획: 전체 %d청크, 미완료 %d청크 (Silver 워터마크=%s, Bronze 상한=%s, contract=%d)",
        len(plan["all_ranges"]), len(plan["pending_ranges"]),
        plan["silver_watermark_before"], plan["bronze_watermark_at_start"], plan["contract_version"],
    )
    # BashOperator가 stdout 마지막 줄을 XCom으로 밀어 올린다 - 계획 JSON은 반드시 한 줄이어야 한다.
    print(json.dumps(plan))
    return plan


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    # TOTAL_DAYS_CAP은 빈 값이면 "상한 없음"이다 - MAX_DAYS_PER_RUN과 같은 규칙을 쓰려고
    # parse_max_days를 그대로 재사용한다(DAG params 기본값도 빈 문자열이라 이 경로를 탄다).
    run(
        chunk_days=int(os.getenv("CHUNK_DAYS") or DEFAULT_CHUNK_DAYS),
        total_days_cap=parse_max_days(os.getenv("TOTAL_DAYS_CAP")) or DEFAULT_TOTAL_DAYS_CAP,
    )
