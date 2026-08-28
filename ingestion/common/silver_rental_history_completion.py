"""Silver 대여이력 초기 적재의 청크 완료 marker.

Bronze Historical Reconciliation의 날짜별 completion marker
(`jobs/write_rental_history_completion_marker.py`)와 같은 발상이지만 완료 단위가 다르다 -
Bronze는 하루가 완료 단위이고, Silver 초기 적재는 planner가 결정론적으로 자른
`range_start~range_end` 청크가 완료 단위다. 그래서 prefix도 marker key 구조도 따로 둔다.

marker가 뜻하는 것은 "이 청크 범위가 이번 contract의 결과로 완전히 교체됐다"이다.
그 의미를 성립시키는 건 iceberg_io.replace_range()로, 이번 입력에 데이터가 있는 날짜가
아니라 선언된 구간 전체를 교체한다. 두 가지가 짝을 이뤄야 marker를 근거로 청크를
건너뛸 수 있다.

marker 매칭 기준은 `range_start + range_end + contract_version` 세 값뿐이다. dag_run_id는
감사 정보로만 남긴다 - 매칭 조건에 넣으면 새 DAG Run이 이전 Run의 성공 청크를 재사용할
수 없어 marker의 목적 자체가 사라진다.
"""
from __future__ import annotations

from typing import Optional

from common.s3_utils import get_json, put_json

DATASET = "silver_rental_history"
COMPLETION_PREFIX = f"_meta/completion/{DATASET}"
STATUS_COMPLETE = "COMPLETE"

# planner / transform / finalizer가 공유하는 결과 계약 버전. 다음 의미가 바뀌면 올린다.
#   - transform_silver_rental_history._DEDUP_SQL의 행 선택 규칙
#   - 날짜/숫자 캐스팅 규칙(_KNOWN_DATETIME_FORMATS, _cast_sql)
#   - quarantine 판정식(_QUARANTINE_VIOLATION)
#   - Silver 결과 스키마나 범위 교체 의미
# 버전이 다른 marker는 완료로 재사용하지 않는다 - 이전 contract로 만든 결과와 새 contract
# 결과가 한 테이블에 조용히 섞이는 것을 막는 게 이 상수의 유일한 목적이다.
SILVER_RENTAL_HISTORY_CONTRACT_VERSION = 1


def completion_key(
    range_start: str,
    range_end: str,
    contract_version: int = SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
) -> str:
    """marker의 S3 key. contract_version을 경로에 넣어 버전별로 완전히 분리된 네임스페이스를 쓴다."""
    return (
        f"{COMPLETION_PREFIX}"
        f"/contract_version={contract_version}"
        f"/range_start={range_start}"
        f"/range_end={range_end}"
        f"/completion.json"
    )


def build_completion_marker(
    *,
    range_start: str,
    range_end: str,
    bronze_watermark_at_start: str,
    bronze_row_count: int,
    silver_row_count: int,
    quarantine_row_count: int,
    dag_run_id: str,
    processed_at: str,
    contract_version: int = SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
) -> dict:
    """COMPLETE marker 문서를 만든다 (저장은 하지 않는다 - marker-last 순서를 호출자가 통제)."""
    return {
        "dataset": DATASET,
        "range_start": range_start,
        "range_end": range_end,
        "contract_version": contract_version,
        "status": STATUS_COMPLETE,
        "bronze_watermark_at_start": bronze_watermark_at_start,
        "bronze_row_count": bronze_row_count,
        "silver_row_count": silver_row_count,
        "quarantine_row_count": quarantine_row_count,
        "dag_run_id": dag_run_id,
        "processed_at": processed_at,
    }


def is_complete_marker(
    marker,
    range_start: str,
    range_end: str,
    contract_version: int = SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
) -> bool:
    """읽어온 문서가 이 범위/버전의 유효한 COMPLETE marker인지 판정한다.

    key에 이미 세 값이 들어 있지만 문서 내용도 다시 확인한다 - 손상된 문서나 다른 범위의
    문서가 잘못된 key에 올라간 경우를 완료로 착각하면 그 구간이 영영 처리되지 않는다.
    확신할 수 없으면 pending으로 떨어뜨리는 쪽이 안전하다(재처리는 멱등이다).
    """
    if not isinstance(marker, dict):
        return False
    return (
        marker.get("dataset") == DATASET
        and marker.get("status") == STATUS_COMPLETE
        and marker.get("range_start") == range_start
        and marker.get("range_end") == range_end
        and marker.get("contract_version") == contract_version
    )


def read_completion_marker(
    bucket: str,
    range_start: str,
    range_end: str,
    contract_version: int = SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
) -> Optional[dict]:
    """marker 문서를 읽는다. 없으면 None (get_json이 NoSuchKey를 None으로 돌려준다)."""
    return get_json(bucket, completion_key(range_start, range_end, contract_version))


def is_range_complete(
    bucket: str,
    range_start: str,
    range_end: str,
    contract_version: int = SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
) -> bool:
    """이 범위가 같은 contract version으로 이미 완료됐는지 확인한다."""
    marker = read_completion_marker(bucket, range_start, range_end, contract_version)
    return is_complete_marker(marker, range_start, range_end, contract_version)


def write_completion_marker(bucket: str, marker: dict) -> str:
    """marker를 S3에 남기고 key를 돌려준다. 두 Iceberg 테이블 쓰기가 모두 끝난 뒤에만 호출한다."""
    key = completion_key(marker["range_start"], marker["range_end"], marker["contract_version"])
    put_json(bucket, key, marker)
    return key
