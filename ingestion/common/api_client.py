"""
서울 열린데이터광장 - 공공자전거 Open API 클라이언트

실제 확인된 서비스 스펙 (2026-08-11 팀 확인):

    서비스명                  | 데이터셋      | 응답 root key | URL 추가 파라미터
    -------------------------|--------------|---------------|---------------------------
    tbCycleRentData          | 대여이력      | rentData      | {날짜 YYYY-MM-DD}/{시간 0~23}
    tbCycleFailureReport     | 고장신고      | failureReport | {날짜 YYYYMMDD}
    tbCycleStationInfo       | 대여소정보    | (미확인)       | 없음 (스냅샷 전체 조회)

URL 패턴: http://openapi.seoul.go.kr:8088/{인증키}/{TYPE}/{서비스명}/{START_INDEX}/{END_INDEX}/{...추가파라미터}

⚠️ tbCycleRentData는 하루를 한 번에 조회할 수 없고 "시간(0~23)" 단위로 쪼개서 호출해야 한다.
   실측 예시(2022-10-01 01시)에서 list_total_count=4355 → 시간당 페이지네이션도 필요.

- 페이징: 1회 최대 1000건으로 추정 (ERROR-336=요청 초과, 우리 쪽 요청 구성 문제이므로 재시도 무의미)
- rate limit / 네트워크 오류 등 일시적 문제는 tenacity로 재시도 + 지수 백오프
"""
import logging
from datetime import date
from typing import Iterator, Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

import config

logger = logging.getLogger(__name__)

SUCCESS_CODE = "INFO-000"
# 문서화된 오류 중 "재시도해도 소용없는" 것들 (요청 파라미터 자체가 잘못됨)
NON_RETRYABLE_CODES = {"ERROR-300", "ERROR-336"}

# 서비스별 응답 JSON의 root key (실제 확인된 값). 모르는 서비스는 None으로 두고
# _extract_service_node가 RESULT/row를 가진 노드를 방어적으로 탐색한다.
SERVICE_ROOT_KEYS = {
    "tbCycleRentData": "rentData",
    "tbCycleFailureReport": "failureReport",
    "tbCycleStationInfo": None,  # 응답 샘플 미확인 - 방어적 탐색으로 처리
}

# API 페이징 메타데이터 - 실제 데이터 컬럼이 아니므로 스키마 검증 전에 제거해야 함
PAGINATION_META_FIELDS = {"START_INDEX", "END_INDEX", "RNUM"}


class SeoulApiError(Exception):
    """재시도 불가능한 API 응답 오류 (필수값 누락, 페이지 크기 초과 등)."""


class SeoulApiTransientError(Exception):
    """재시도 가능한 일시적 오류 (rate limit, 네트워크 타임아웃 등)."""


def strip_pagination_meta(row: dict) -> dict:
    """응답 row에서 START_INDEX/END_INDEX/RNUM 같은 페이징 메타 필드를 제거한다."""
    return {k: v for k, v in row.items() if k not in PAGINATION_META_FIELDS}


def _extract_service_node(body: dict, service: str) -> dict:
    """알려진 root key가 있으면 바로 사용하고, 없으면 RESULT/row를 가진 노드를 탐색한다."""
    known_key = SERVICE_ROOT_KEYS.get(service)
    if known_key and known_key in body:
        return body[known_key]
    if "RESULT" in body or "row" in body:
        return body
    for value in body.values():
        if isinstance(value, dict) and ("RESULT" in value or "row" in value):
            return value
    return body


@retry(
    retry=retry_if_exception_type(SeoulApiTransientError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _fetch_page(service: str, start_idx: int, end_idx: int, extra_path_segments: Optional[list] = None) -> dict:
    """단일 페이지 호출. 오류 유형에 따라 재시도 여부를 분기한다."""
    settings = config.SETTINGS
    segments = [settings.seoul_api_key, settings.seoul_api_type, service, str(start_idx), str(end_idx)]
    segments.extend(str(s) for s in (extra_path_segments or []))
    url = f"{settings.seoul_api_base_url}/" + "/".join(segments) + "/"

    try:
        resp = requests.get(url, timeout=30)
    except requests.RequestException as e:
        raise SeoulApiTransientError(str(e)) from e

    if resp.status_code == 429 or resp.status_code >= 500:
        raise SeoulApiTransientError(f"HTTP {resp.status_code}")
    resp.raise_for_status()

    try:
        body = resp.json()
    except ValueError as e:
        # 응답이 완전히 비어있거나 JSON이 아닐 때(HTML 에러페이지, 인증키 오류 등)
        # 원문 일부를 에러 메시지에 남겨야 다음에 원인을 바로 알 수 있다.
        snippet = resp.text[:300] if resp.text else "(빈 응답)"

        # 실제 확인된 사례: SEOUL_API_KEY가 여전히 "sample"(서울시가 인식하는 데모 키)이면
        # XML로 "ERROR-335: 샘플데이터는 최대 5건만 조회 가능" 응답을 준다.
        # 재시도해도 소용없는 설정 문제이므로 즉시, 명확하게 실패시킨다.
        if "샘플데이터" in snippet or "ERROR-335" in snippet:
            raise SeoulApiError(
                "SEOUL_API_KEY가 아직 'sample'(서울시 데모 키)입니다. "
                "data.seoul.go.kr에서 발급받은 실제 인증키로 .env의 SEOUL_API_KEY를 교체하세요. "
                f"(서버 응답: {snippet!r})"
            ) from e

        # 그 외 비-JSON 응답은 일시적 문제일 수 있으므로 재시도
        raise SeoulApiTransientError(
            f"JSON 파싱 실패 (HTTP {resp.status_code}), 응답 원문: {snippet!r}"
        ) from e

    node = _extract_service_node(body, service)
    code = node.get("RESULT", {}).get("CODE", "")

    if code == SUCCESS_CODE or not code:
        return node
    if code in NON_RETRYABLE_CODES:
        raise SeoulApiError(f"{code}: {node.get('RESULT', {}).get('MESSAGE')}")
    # 문서화되지 않은 코드는 일시 오류로 간주하고 재시도
    raise SeoulApiTransientError(f"{code}: {node.get('RESULT', {}).get('MESSAGE')}")


def _paginate(service: str, extra_path_segments: list) -> Iterator[dict]:
    """공통 페이징 루프: 마지막 페이지(rows < page_size)까지 순회하며 row를 그대로 yield."""
    page_size = config.SETTINGS.api_page_size
    start_idx = 1
    while True:
        end_idx = start_idx + page_size - 1
        node = _fetch_page(service, start_idx, end_idx, extra_path_segments)
        rows = node.get("row", [])

        if not rows:
            break
        for row in rows:
            yield row

        if len(rows) < page_size:
            break  # 마지막 페이지
        start_idx += page_size


def fetch_rent_history_by_hour(target_date: date, hour: int) -> Iterator[dict]:
    """
    tbCycleRentData: 특정 날짜의 특정 시간(0~23)치 대여이력을 전부 가져온다.
    (이 서비스는 하루 전체를 한 번에 조회할 수 없고 시간 단위로 쪼개야 함)
    """
    date_str = target_date.strftime("%Y-%m-%d")
    logger.info("대여이력 API 호출: %s %d시", date_str, hour)
    yield from _paginate("tbCycleRentData", [date_str, hour])


def fetch_rent_history_by_date(target_date: date) -> Iterator[dict]:
    """하루 24시간을 순서대로 순회하며 tbCycleRentData 전체를 가져온다."""
    for hour in range(24):
        yield from fetch_rent_history_by_hour(target_date, hour)


def fetch_failure_reports_by_date(target_date: date) -> Iterator[dict]:
    """tbCycleFailureReport: 특정 날짜(YYYYMMDD)의 고장신고 내역을 전부 가져온다."""
    date_str = target_date.strftime("%Y%m%d")
    logger.info("고장신고 API 호출: %s", date_str)
    yield from _paginate("tbCycleFailureReport", [date_str])


def fetch_station_info() -> Iterator[dict]:
    """
    tbCycleStationInfo: 날짜 파라미터 없이 현재 전체 대여소 스냅샷을 페이징하며 가져온다.
    ⚠️ 이 서비스의 실제 응답 root key/필드명은 아직 확인된 샘플이 없다 (SERVICE_ROOT_KEYS에
    None으로 등록되어 있어 _extract_service_node가 RESULT/row를 가진 노드를 방어적으로 탐색함).
    """
    logger.info("대여소정보 API 호출 (전체 스냅샷)")
    yield from _paginate("tbCycleStationInfo", [])