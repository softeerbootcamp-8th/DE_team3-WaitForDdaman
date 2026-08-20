from datetime import date
from unittest.mock import patch

import requests

from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_failure_reports_by_date,
    fetch_rent_history_by_date,
    fetch_station_active,
    strip_pagination_meta,
)


class _FakeResp:
    def __init__(self, status_code, json_body=None, raw_text=None):
        self.status_code = status_code
        self._json = json_body
        # raw_text가 주어지면 실제 서울 API가 인증키 오류 등으로 비어있는/HTML 응답을
        # 줄 때를 재현한다 (json_body=None, raw_text="" 또는 임의 문자열).
        self.text = raw_text if raw_text is not None else ""

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        if self._json is None:
            # requests가 실제로 하는 동작: 빈/비-JSON 본문이면 ValueError 계열 예외 발생
            import json as _json_module

            return _json_module.loads(self.text)
        return self._json


# 사용자가 실제로 확인한 tbCycleRentData 응답 샘플 (2022-10-01 01시, 일부)
REAL_RENT_DATA_SAMPLE = {
    "rentData": {
        "list_total_count": "4355",
        "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상 처리되었습니다."},
        "row": [
            {
                "BIKE_ID": "SPB-33174",
                "RENT_DT": "2022-10-01 01:00:21",
                "RENT_ID": "01118",
                "RENT_NM": "증미역 3번출구뒤(등촌두산위브센티움오피스텔)",
                "RENT_HOLD": "0",
                "RTN_DT": "2022-10-01 01:00:31",
                "RTN_ID": "01118",
                "RTN_NM": "증미역 3번출구뒤(등촌두산위브센티움오피스텔)",
                "RTN_HOLD": "0",
                "USE_MIN": "0",
                "USE_DST": "0.00",
                "USR_CLS_CD": "USR_001",
                "SEX_CD": "M",
                "BIRTH_YEAR": "1998",
                "RENT_STATION_ID": "ST-516",
                "RETURN_STATION_ID": "ST-516",
                "BIKE_SE_CD": "일반자전거",
                "START_INDEX": 0,
                "END_INDEX": 0,
                "RNUM": "1",
            }
        ],
    }
}

# 사용자가 실제로 확인한 tbCycleFailureReport 응답 샘플
REAL_FAILURE_REPORT_SAMPLE = {
    "failureReport": {
        "list_total_count": "20936",
        "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상 처리되었습니다."},
        "row": [
            {
                "bikeNo": "SPB-60754",
                "regDttm": "2022-10-01 00:05:51",
                "mlangComCdName": "페달",
                "START_INDEX": 0,
                "END_INDEX": 0,
                "RNUM": "1",
            }
        ],
    }
}


def test_rent_history_uses_rentData_root_key_and_hourly_urls():
    """tbCycleRentData는 root key가 'rentData'이고, 시간(0~23) 단위로 24번 호출되어야 한다."""
    called_urls = []

    def fake_get(url, timeout):
        called_urls.append(url)
        # 매 시간 호출마다 1건씩 반환, 다음 페이지는 없음(1건 < page_size)
        return _FakeResp(200, REAL_RENT_DATA_SAMPLE)

    with patch("requests.get", side_effect=fake_get):
        rows = list(fetch_rent_history_by_date(date(2022, 10, 1)))

    assert len(called_urls) == 24  # 0~23시 전부 호출
    assert "2022-10-01" in called_urls[0]
    assert called_urls[0].rstrip("/").endswith("/0")  # 0시 호출
    assert called_urls[-1].rstrip("/").endswith("/23")  # 23시 호출
    assert len(rows) == 24  # 시간당 1건 * 24시간
    assert rows[0]["BIKE_ID"] == "SPB-33174"


def test_failure_report_uses_failureReport_root_key_and_yyyymmdd():
    called_urls = []

    def fake_get(url, timeout):
        called_urls.append(url)
        return _FakeResp(200, REAL_FAILURE_REPORT_SAMPLE)

    with patch("requests.get", side_effect=fake_get):
        rows = list(fetch_failure_reports_by_date(date(2022, 10, 1)))

    assert len(called_urls) == 1  # 1건 < page_size 이므로 페이지네이션 1회로 종료
    assert "20221001" in called_urls[0]
    assert rows[0]["bikeNo"] == "SPB-60754"


def test_strip_pagination_meta_removes_only_meta_fields():
    row = {"BIKE_ID": "SPB-1", "RENT_DT": "2026-01-01 00:00:00", "START_INDEX": 0, "END_INDEX": 0, "RNUM": "1"}
    cleaned = strip_pagination_meta(row)
    assert cleaned == {"BIKE_ID": "SPB-1", "RENT_DT": "2026-01-01 00:00:00"}


def test_non_retryable_error_336_fails_immediately():
    err_body = {"rentData": {"RESULT": {"CODE": "ERROR-336", "MESSAGE": "1000건 초과"}, "row": []}}

    with patch("requests.get", return_value=_FakeResp(200, err_body)):
        try:
            list(fetch_rent_history_by_date(date(2022, 10, 1)))
            assert False, "SeoulApiError가 발생해야 함"
        except SeoulApiError:
            pass


def test_transient_5xx_recovers_after_retry():
    calls = {"n": 0}

    def flaky_get(url, timeout):
        calls["n"] += 1
        if calls["n"] < 3:
            return _FakeResp(503)
        return _FakeResp(200, {"rentData": {"RESULT": {"CODE": "INFO-000"}, "row": []}})

    with patch("requests.get", side_effect=flaky_get), patch("tenacity.nap.time.sleep", return_value=None):
        rows = list(fetch_rent_history_by_date(date(2022, 10, 1)))

    # 0시 호출: 503 두 번 겪고 세 번째 시도에 성공(빈 결과) = 3회
    # 1~23시 호출: 매 시간 첫 시도에 성공(빈 결과) = 23회
    assert calls["n"] == 3 + 23
    assert rows == []


def test_empty_response_body_raises_transient_error_with_readable_message():
    """
    실제 사례(2026-08-11): 워터마크 미설정으로 2015년치를 조회했을 때 응답 바디가
    완전히 비어있어 JSONDecodeError로 크래시했다. 이제는 재시도 가능한 에러로
    잡히고, 에러 메시지에 원인 파악용 응답 원문 스니펫이 남아야 한다.
    """
    with patch("requests.get", return_value=_FakeResp(200, json_body=None, raw_text="")), patch(
        "tenacity.nap.time.sleep", return_value=None
    ):
        try:
            list(fetch_rent_history_by_date(date(2015, 1, 2)))
            assert False, "SeoulApiTransientError가 발생해야 함"
        except SeoulApiTransientError as e:
            assert "빈 응답" in str(e) or "JSON 파싱 실패" in str(e)


def test_html_error_page_response_included_in_error_message():
    """인증키 오류 등으로 HTML 에러 페이지가 오는 경우도 재현 - 원문이 에러 메시지에 남아야 한다."""
    html_body = "<html><body>인증키가 유효하지 않습니다.</body></html>"
    with patch("requests.get", return_value=_FakeResp(200, json_body=None, raw_text=html_body)), patch(
        "tenacity.nap.time.sleep", return_value=None
    ):
        try:
            list(fetch_rent_history_by_date(date(2026, 7, 1)))
            assert False, "SeoulApiTransientError가 발생해야 함"
        except SeoulApiTransientError as e:
            assert "인증키" in str(e)


# 사용자가 실제로 확인한 bikeList(rentBikeStatus) 응답 샘플 (2026-08-16 실측,
# 전수 페이징 2,735건 중 1건 발췌 - 필드 7개가 매 행 빠짐없이 존재함을 확인함)
REAL_BIKE_LIST_SAMPLE = {
    "rentBikeStatus": {
        "list_total_count": 1,
        "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상 처리되었습니다."},
        "row": [
            {
                "rackTotCnt": "15",
                "stationName": "102. 망원역 1번출구 앞",
                "parkingBikeTotCnt": "5",
                "shared": "33",
                "stationLatitude": "37.55564880",
                "stationLongitude": "126.91062927",
                "stationId": "ST-4",
            }
        ],
    }
}


def test_station_active_uses_rentBikeStatus_root_key():
    """bikeList는 root key가 'rentBikeStatus'이고 페이징 메타 필드가 없다 (실측 2026-08-16)."""
    with patch("requests.get", return_value=_FakeResp(200, REAL_BIKE_LIST_SAMPLE)):
        rows = list(fetch_station_active())

    assert len(rows) == 1
    assert rows[0]["stationId"] == "ST-4"
    assert rows[0]["shared"] == "33"