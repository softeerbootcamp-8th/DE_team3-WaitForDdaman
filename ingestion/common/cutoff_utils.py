"""Airflow 및 잡 공통 논리 기준시각(collection_cutoff_at) 파싱 유틸리티."""

from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def parse_collection_cutoff(value: str | None) -> datetime:
    """Airflow가 전달한 논리적 cutoff를 timezone-aware KST datetime으로 정규화한다.

    값이 없으면 현재 KST 시각을 반환한다 (로컬 단독 실행 지원).
    마이크로초는 버려 일관된 초 단위 비교 및 키 형식을 보장한다.
    """
    if not value:
        return datetime.now(KST).replace(microsecond=0)
    cutoff = datetime.fromisoformat(value)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("collection_cutoff_at must include timezone")
    return cutoff.astimezone(KST).replace(microsecond=0)
