"""
silver_rental_history 백필 청크 목록 계산 (#232)

bronze_initial_load_all_sources_dag.py의 load_silver_rental_history 태스크가
기존에는 bash for-loop로 transform_silver_rental_history.py를 반복 호출하며 매
반복마다 잡이 스스로 워터마크를 읽어 다음 구간을 정했다 - 이 방식은 한 반복이
실패하면 루프 전체가 멈추고(#232), 반복들이 서로 의존해 병렬화할 수 없었다.

이 잡은 그 대신 Bronze/Silver 워터마크를 한 번만 읽어서 처리해야 할 전체 구간을
CHUNK_DAYS 크기로 미리 다 잘라 JSON으로 출력한다. DAG가 이 목록을 Airflow dynamic
task mapping(.expand())으로 펼치면, 각 청크가 서로 독립적인 태스크 인스턴스가 되어
하나가 실패해도 나머지가 계속 진행되고 개별 재시도가 가능해진다
(list_input_files -> parse_*_files -> .expand() 패턴과 동일한 구조).

각 청크는 transform_silver_rental_history.py를 BACKFILL_RANGE_START/END로 호출한다
(그 잡의 run() 참고) - 워터마크는 이 잡도, 각 청크도 쓰지 않는다. 모든 청크가 끝난
뒤 DAG의 마무리 태스크가 set_watermark.py로 한 번만 전진시킨다.

사용법:
    CHUNK_DAYS=31 TOTAL_DAYS_CAP=3650 python -m jobs.compute_silver_rental_history_backfill_ranges
"""
import json
import os
from datetime import date, timedelta

from common.watermark import read_watermark
from config.watermark_keys import DATASET_WATERMARK_KEYS

SILVER_WATERMARK_KEY = DATASET_WATERMARK_KEYS["silver_rental_history"]
DEFAULT_CHUNK_DAYS = 31
DEFAULT_TOTAL_DAYS_CAP = 3650


def build_ranges(
    silver_watermark: date, bronze_watermark: date, chunk_days: int, total_days_cap: int
) -> list[dict]:
    start = silver_watermark + timedelta(days=1)
    capped_end = start + timedelta(days=total_days_cap - 1)
    end = min(bronze_watermark, capped_end)

    ranges = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        ranges.append({"start": cursor.strftime("%Y-%m-%d"), "end": chunk_end.strftime("%Y-%m-%d")})
        cursor = chunk_end + timedelta(days=1)
    return ranges


def run(chunk_days: int, total_days_cap: int) -> None:
    bronze_watermark = read_watermark()
    silver_watermark = read_watermark(watermark_key=SILVER_WATERMARK_KEY)
    ranges = build_ranges(silver_watermark, bronze_watermark, chunk_days, total_days_cap)
    print(json.dumps(ranges))


if __name__ == "__main__":
    run(
        chunk_days=int(os.getenv("CHUNK_DAYS") or DEFAULT_CHUNK_DAYS),
        total_days_cap=int(os.getenv("TOTAL_DAYS_CAP") or DEFAULT_TOTAL_DAYS_CAP),
    )
