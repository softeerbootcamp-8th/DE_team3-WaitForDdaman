"""
스키마 정의 - 따맨/bikeman 수거/배치 이벤트

다른 원천(대여이력, 고장신고)과의 결정적 차이: 이 데이터는 외부 공공 API/파일이 아니라
우리 자신이 설계한 OLTP(Postgres bikeman.fact_worker_event)에서 SQL로 직접 온다.
그래서:
  - is_X_file()처럼 "이게 이 데이터셋이 맞는지" 판별할 필요가 없다
    (파일 혼입 걱정이 없는, 우리가 정의한 단일 테이블을 쿼리하는 것이므로).
  - 그래도 컬럼 검증(validate_and_report)은 유지한다. bikeman 서비스가 스키마를
    변경(컬럼 추가 등)했을 때 조용히 놓치지 않기 위함
    (source_data 문서의 "contract-first, schema_version, 추가만 허용" 설계 원칙과 동일).
"""
import logging

logger = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    """필수 컬럼이 없는 경우. 배치를 중단시켜야 하는 심각한 스키마 변경."""


REQUIRED_COLUMNS = [
    "event_id",
    "event_type",
    "bike_id",
    "worker_id",
    "occurred_at",
    "received_at",
]

# station_id는 NULL이 정상값(노상 수거)이므로 필수 컬럼에는 넣되, null 자체는 위반이 아님.
OPTIONAL_COLUMNS = ["station_id"]

ALLOWED_EVENT_TYPES = {"COLLECT", "DEPLOY"}

# Postgres 컬럼명 -> Bronze 테이블 컬럼명. 지금은 1:1이지만, bikeman 쪽 컬럼명이
# 바뀌어도 이 매핑 하나만 고치면 되도록 다른 스키마 모듈과 동일한 패턴을 유지한다.
COLUMN_MAPPING = {
    "event_id": "event_id",
    "event_type": "event_type",
    "bike_id": "bike_id",
    "station_id": "station_id",
    "worker_id": "worker_id",
    "occurred_at": "occurred_at",
    "received_at": "received_at",
}


def validate_and_report(actual_columns: list[str]) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in actual_columns]
    if missing:
        raise SchemaValidationError(f"필수 컬럼 누락: {missing} (실제 컬럼: {actual_columns})")

    unknown = [c for c in actual_columns if c not in COLUMN_MAPPING]
    if unknown:
        # 실패시키지 않는다 - bikeman 팀이 컬럼을 추가한 것일 수 있음(추가만 허용 정책).
        # 원본 보존은 daily_batch_bikeman_event.py의 raw JSON 적재(put_json)에서 이미 처리됨.
        logger.warning("알 수 없는 신규 컬럼 감지(스키마 확장 가능성): %s", unknown)


def build_select_exprs(actual_columns: list[str]):
    """Spark DataFrame에서 COLUMN_MAPPING에 정의된 컬럼만, 정해진 이름으로 select."""
    from pyspark.sql import functions as F

    return [
        F.col(src).alias(dst)
        for src, dst in COLUMN_MAPPING.items()
        if src in actual_columns
    ]


def validate_event_types(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    event_type이 허용된 값(COLLECT/DEPLOY)이 아닌 행을 걸러낸다.
    (앱 버전 혼재로 신규/오타 enum 값이 섞여 들어올 수 있다는 시나리오 대응 -
     source_data 문서의 "미등록 event_type -> 즉시 quarantine" 요구사항과 동일)

    반환: (정상 행 목록, quarantine 대상 행 목록)
    """
    valid_rows, quarantine_rows = [], []
    for r in rows:
        if r.get("event_type") in ALLOWED_EVENT_TYPES:
            valid_rows.append(r)
        else:
            quarantine_rows.append(r)
    return valid_rows, quarantine_rows
