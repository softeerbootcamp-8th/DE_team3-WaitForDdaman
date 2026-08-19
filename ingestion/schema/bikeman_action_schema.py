"""
스키마 정의 - Silver 계층 bikeman_action (따맨/bikeman 행동 이력)

Bronze(bikeman_event) -> Silver(bikeman_action) 정제 시 적용하는 이상치 규칙.

Bronze 레이어(schema/bikeman_event_schema.py)는 "필수 컬럼 존재 여부"와
"event_type이 COLLECT/DEPLOY인지"만 검증했다. 시간 정합성(occurred_at vs
received_at 역전), 서비스 시작일 이전 발생 여부는 검증하지 않았다 - Silver에서
방어선을 하나 더 둔다 (defense-in-depth: Bronze 스키마가 나중에 느슨해져도
Silver가 최종 방어선 역할).

최종 Silver 컬럼은 bike_id/event_type/occurred_at/station_id/ingested_at 5개로
결정했으므로(worker_id/received_at/event_id 의도적 제외), received_at은
이 모듈의 이상치 판정에만 쓰고 최종 select에는 포함하지 않는다.

station_id는 Gold의 gold.fact_station_inventory가 DEPLOY 이벤트로 자전거의
현재 위치를 정확히 파악하는 데 필요해서(2026-08-17) 포함했다. null이 정상값이다
(노상 수거처럼 특정 대여소와 무관한 이벤트는 station_id가 없다).
"""
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)

ALLOWED_EVENT_TYPES = {"COLLECT", "DEPLOY"}

# bikeman 서비스 시작일 - 이보다 이전 occurred_at은 물리적으로 존재할 수 없음
SERVICE_START_DATE = date(2026, 6, 30)

# 최종 Silver 테이블 컬럼 (worker_id/received_at/event_id 의도적 제외)
FINAL_COLUMNS = ["bike_id", "event_type", "occurred_at", "station_id", "ingested_at"]


def classify_rows(rows: list[dict], now: datetime) -> tuple[list[dict], list[dict]]:
    """
    Bronze에서 읽은 row(occurred_at/received_at 포함)를 검사해서
    (정상 행 목록, quarantine 대상 행 목록)으로 나눈다.
    quarantine 행에는 사유(quarantine_reason)를 붙여서 반환한다.

    검사 항목:
      - bike_id / event_type / occurred_at null 여부
      - event_type이 COLLECT/DEPLOY 외의 값인지 (Bronze 통과했어도 재검증)
      - occurred_at이 서비스 시작일(2026-06-30) 이전인지 - 물리적으로 불가능
      - occurred_at이 미래 시각인지 - 클라이언트 시계 오차 의심
      - occurred_at > received_at인지 - 시간 역전, 기획 문서의
        "occurred_at > received_at = 0 기대" 요구사항에 대응
    """
    valid_rows, quarantine_rows = [], []

    for r in rows:
        reasons = []
        bike_id = r.get("bike_id")
        event_type = r.get("event_type")
        occurred_at = r.get("occurred_at")
        received_at = r.get("received_at")

        if not bike_id:
            reasons.append("null_bike_id")

        if not event_type:
            reasons.append("null_event_type")
        elif event_type not in ALLOWED_EVENT_TYPES:
            reasons.append(f"invalid_event_type:{event_type}")

        if occurred_at is None:
            reasons.append("null_occurred_at")
        else:
            if occurred_at.date() < SERVICE_START_DATE:
                reasons.append("occurred_before_service_start")
            if occurred_at > now:
                reasons.append("occurred_in_future")
            if received_at is not None and occurred_at > received_at:
                reasons.append("occurred_after_received")

        if reasons:
            quarantine_rows.append({**r, "quarantine_reason": ",".join(reasons)})
        else:
            valid_rows.append(r)

    return valid_rows, quarantine_rows


def dedup_rows(rows: list[dict]) -> list[dict]:
    """
    (bike_id, event_type, occurred_at) 조합 기준 중복 제거.
    Bronze가 날짜 파티션을 통째로 덮어쓰는 방식(overwritePartitions)이라 정상
    상황에서는 중복이 없어야 하지만, 재처리 lookback 구간이 겹치는 극단적 케이스를
    방어적으로 한 번 더 막는다. 순서상 나중 항목이 아니라 처음 본 항목을 유지한다
    (occurred_at ASC로 미리 정렬돼 들어온다는 전제 - 동일 키면 어차피 내용 같음).
    """
    seen = set()
    deduped = []
    for r in rows:
        key = (r.get("bike_id"), r.get("event_type"), r.get("occurred_at"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped
