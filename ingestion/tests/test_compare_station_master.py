import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pytest
from moto import mock_aws

from common import config as config_module


def _make_test_xlsx(path: Path, data_rows: list[list[str]]) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    for _ in range(1, 6):
        ws.append(["헤더"] * 10)  # 1~5행 병합헤더 흉내 (skiprows=5 대상)
    for row in data_rows:
        ws.append(row)
    wb.save(path)


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(env="aws", raw_bucket="test-raw-bucket")
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket("test-raw-bucket")
        yield


def test_file_parsing_extracts_station_ids_by_position(tmp_path):
    from jobs.compare_station_master import _read_file_station_ids

    xlsx_path = tmp_path / "station.xlsx"
    _make_test_xlsx(
        xlsx_path,
        [
            ["102", "강남역", "강남구", "주소", "37.1", "127.4", "2017", "10", "", "LCD"],
            ["103", "역삼역", "강남구", "주소", "37.1", "127.4", "2018", "", "15", "QR"],
        ],
    )
    ids = _read_file_station_ids(xlsx_path)
    assert ids == {"102", "103"}


def test_file_parsing_fails_safely_on_column_count_mismatch(tmp_path):
    """파일 구조(컬럼 수)가 바뀌면 잘못된 위치 매핑 대신 명확하게 실패해야 한다."""
    from jobs.compare_station_master import _read_file_station_ids

    xlsx_path = tmp_path / "broken.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    for _ in range(1, 6):
        ws.append(["헤더"] * 8)  # 10개가 아니라 8개 컬럼
    ws.append(["102", "강남역", "강남구", "주소", "37.1", "127.4", "2017", "10"])
    wb.save(xlsx_path)

    with pytest.raises(ValueError):
        _read_file_station_ids(xlsx_path)


def test_api_station_id_field_auto_detected():
    from jobs.compare_station_master import _extract_station_ids, _fetch_api_rows

    rows = [{"RENT_NO": "102", "RENT_NM": "강남역"}, {"RENT_NO": "103", "RENT_NM": "역삼역"}]
    with patch("jobs.compare_station_master.fetch_station_info", return_value=iter(rows)):
        ids = _extract_station_ids(_fetch_api_rows())
    assert ids == {"102", "103"}


def test_zero_padded_api_ids_normalize_to_match_file_ids():
    """
    실제 사례(2026-08-11): API는 대여소번호를 5자리 zero-padding으로 준다('00102')
    반면 파일은 padding 없이 준다('102'). 정규화 없이는 공통 0개로 잘못 나왔다.
    """
    from jobs.compare_station_master import _extract_station_ids, _fetch_api_rows, _normalize_station_id

    assert _normalize_station_id("00102") == "102"
    assert _normalize_station_id("102") == "102"
    assert _normalize_station_id("01022") == "1022"

    rows = [{"RENT_NO": "00102"}, {"RENT_NO": "01022"}]
    with patch("jobs.compare_station_master.fetch_station_info", return_value=iter(rows)):
        ids = _extract_station_ids(_fetch_api_rows())
    assert ids == {"102", "1022"}  # zero-padding 제거된 형태로 나와야 파일과 비교 가능


def test_api_station_id_field_not_found_raises_with_actual_keys():
    from jobs.compare_station_master import StationIdFieldNotFoundError, _extract_station_ids, _fetch_api_rows

    rows = [{"unknownField": "x"}]
    with patch("jobs.compare_station_master.fetch_station_info", return_value=iter(rows)):
        with pytest.raises(StationIdFieldNotFoundError, match="unknownField"):
            _extract_station_ids(_fetch_api_rows())


def test_new_stations_csv_contains_only_api_exclusive_stations_with_details(tmp_path, s3_env):
    """
    실제 사례: 448개 신설 후보가 나왔을 때 ID만으로는 뭔지 알 수 없어서
    이름/주소 등 상세정보가 포함된 CSV를 다운로드 가능하게 남겨야 한다.
    공통(파일에도 있는) 대여소는 신설 후보가 아니므로 CSV에 나오면 안 된다.
    """
    from jobs.compare_station_master import run

    xlsx_path = tmp_path / "공공자전거 대여소 정보(테스트).xlsx"
    _make_test_xlsx(
        xlsx_path,
        [
            ["102", "강남역", "강남구", "주소", "37.1", "127.4", "2017", "10", "", "LCD"],
        ],
    )

    api_rows = [
        {"RENT_NO": "00102", "RENT_NM": "강남역", "STA_ADD1": "강남구 어딘가"},  # 공통
        {"RENT_NO": "00999", "RENT_NM": "신설역A", "STA_ADD1": "신설동 111"},  # 신설
    ]

    with patch("jobs.compare_station_master.fetch_station_info", return_value=iter(api_rows)):
        run(str(xlsx_path))

    from common.s3_utils import get_s3_client

    s3 = get_s3_client()
    objs = s3.list_objects_v2(Bucket="test-raw-bucket", Prefix="raw/station_master/_compare/")
    csv_key = [o["Key"] for o in objs["Contents"] if o["Key"].endswith(".csv")][0]
    csv_content = s3.get_object(Bucket="test-raw-bucket", Key=csv_key)["Body"].read().decode("utf-8")

    assert "신설역A" in csv_content
    assert "강남역" not in csv_content


def test_end_to_end_upload_and_compare_report(tmp_path, s3_env):
    """
    실제 사례를 흉내낸 엔드투엔드: 파일엔 102/103/104(padding 없음),
    API엔 00103/00104/00105(5자리 zero-padding)가 있을 때, 정규화 후
    103/104가 공통으로, 102는 파일에만(폐쇄 후보), 105는 API에만(신설 후보)으로
    정확히 분류되어야 한다. (정규화 없으면 문자열이 달라 전부 안 겹치는 것으로 잘못 나옴)
    """
    from jobs.compare_station_master import run

    xlsx_path = tmp_path / "공공자전거 대여소 정보(테스트).xlsx"
    _make_test_xlsx(
        xlsx_path,
        [
            ["102", "강남역", "강남구", "주소", "37.1", "127.4", "2017", "10", "", "LCD"],
            ["103", "역삼역", "강남구", "주소", "37.1", "127.4", "2018", "", "15", "QR"],
            ["104", "삼성역", "강남구", "주소", "37.1", "127.4", "2019", "5", "5", "혼합"],
        ],
    )

    api_rows = [
        {"RENT_NO": "00103", "RENT_NM": "역삼역"},
        {"RENT_NO": "00104", "RENT_NM": "삼성역"},
        {"RENT_NO": "00105", "RENT_NM": "신설역"},
    ]

    with patch("jobs.compare_station_master.fetch_station_info", return_value=iter(api_rows)):
        run(str(xlsx_path))

    from common.s3_utils import get_s3_client

    s3 = get_s3_client()

    landing_objs = s3.list_objects_v2(Bucket="test-raw-bucket", Prefix="raw/station_master/_landing/")
    assert len(landing_objs.get("Contents", [])) == 1

    # API 원본 응답도 raw zone에 남아서 실제로 뭘 받았는지 확인 가능해야 한다
    api_payload_objs = s3.list_objects_v2(Bucket="test-raw-bucket", Prefix="raw/station_master/api/")
    assert len(api_payload_objs.get("Contents", [])) == 1
    api_payload = json.loads(
        s3.get_object(Bucket="test-raw-bucket", Key=api_payload_objs["Contents"][0]["Key"])["Body"].read()
    )
    assert api_payload["row_count"] == 3
    assert api_payload["rows"] == api_rows

    compare_objs = s3.list_objects_v2(Bucket="test-raw-bucket", Prefix="raw/station_master/_compare/")
    report_key = [o["Key"] for o in compare_objs["Contents"] if o["Key"].endswith("report.json")][0]
    report = json.loads(s3.get_object(Bucket="test-raw-bucket", Key=report_key)["Body"].read())

    assert report["file_count"] == 3
    assert report["api_count"] == 3
    assert report["common_count"] == 2
    assert report["only_in_file_sample"] == ["102"]
    assert report["only_in_api_sample"] == ["105"]