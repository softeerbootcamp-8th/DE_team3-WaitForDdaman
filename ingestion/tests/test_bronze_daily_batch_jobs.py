"""
Bronze 일 배치 5개 잡 Spark 제거 및 PyArrow + PyIceberg 전환 테스트 (Issue #142)
"""
from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from moto import mock_aws

import config as config_module
from common.api_client import fetch_rent_history_by_date_parallel
from common.s3_utils import get_s3_client, put_json
from jobs.daily_batch_bikeman_event import _process_one_day as process_bikeman_event
from jobs.daily_batch_failure_report import _process_one_day as process_failure_report
from jobs.daily_batch_rental_history import _process_one_day as process_rental_history
from jobs.daily_batch_station_active import _process_snapshot as process_station_active
from jobs.daily_batch_station_master import _process_snapshot as process_station_master

BUCKET = "test-bronze-batch-bucket"


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket=BUCKET,
        warehouse_bucket=BUCKET,
        s3_region="ap-northeast-2",
        raw_source="s3",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    with mock_aws():
        s3 = get_s3_client()
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-2"},
        )
        yield s3


def test_station_master_pure_arrow_write(s3_env):
    sample_rows = [
        {
            "STA_LOC": "마포구",
            "RENT_ID": "ST-10",
            "RENT_NO": "00108",
            "RENT_NM": "서교동",
            "RENT_ID_NM": "108. 서교동",
            "HOLD_NUM": "10",
            "STA_ADD1": "서울시",
            "STA_ADD2": "마포구",
            "STA_LAT": "37.5",
            "STA_LONG": "126.9",
        }
    ]
    put_json(
        BUCKET,
        "raw/station_master/api/snapshot_date=2026-08-22/payload.json",
        {"snapshot_date": "2026-08-22", "row_count": 1, "rows": sample_rows},
    )

    with patch("jobs.daily_batch_station_master.overwrite_partition") as mock_overwrite:
        count = process_station_master("2026-08-22")

    assert count == 1
    mock_overwrite.assert_called_once()
    table_name, arrow_table, col, val = mock_overwrite.call_args[0]
    assert table_name == "bronze.station_master"
    assert col == "snapshot_date"
    assert val == "2026-08-22"
    # Verify station_no normalization ('00108' -> '108')
    assert arrow_table["station_no"].to_pylist() == ["108"]


def test_station_active_pure_arrow_write(s3_env):
    sample_rows = [
        {
            "stationId": "ST-4",
            "stationName": "망원역",
            "rackTotCnt": "15",
            "parkingBikeTotCnt": "5",
            "shared": "33",
            "stationLatitude": "37.55",
            "stationLongitude": "126.91",
        }
    ]
    put_json(
        BUCKET,
        "raw/station_active/api/snapshot_date=2026-08-22/payload.json",
        {"snapshot_date": "2026-08-22", "row_count": 1, "rows": sample_rows},
    )

    with patch("jobs.daily_batch_station_active.overwrite_partition") as mock_overwrite:
        count = process_station_active("2026-08-22")

    assert count == 1
    mock_overwrite.assert_called_once()
    table_name, arrow_table, col, val = mock_overwrite.call_args[0]
    assert table_name == "bronze.station_active"
    assert col == "snapshot_date"
    assert val == "2026-08-22"


def test_rental_history_parallel_fetch_and_arrow_write(s3_env):
    sample_rows = [
        {
            "BIKE_ID": "SPB-100",
            "RENT_DT": "2026-08-22 01:00:00",
            "RENT_ID": "108",
            "RENT_NM": "서교동",
            "RENT_HOLD": "1",
            "RTN_DT": "2026-08-22 01:20:00",
            "RTN_ID": "109",
            "RTN_NM": "합정역",
            "RTN_HOLD": "1",
            "USE_MIN": "20",
            "USE_DST": "2500",
        }
    ]

    with patch("jobs.daily_batch_rental_history.fetch_rent_history_by_date_parallel", return_value=sample_rows), \
         patch("jobs.daily_batch_rental_history.overwrite_partition") as mock_overwrite:
        count = process_rental_history(date(2026, 8, 22))

    assert count == 1
    mock_overwrite.assert_called_once()
    table_name, arrow_table, col, val = mock_overwrite.call_args[0]
    assert table_name == "bronze.rental_history"
    assert col == "rent_date_partition"
    assert val == "2026-08-22"
    assert arrow_table["bike_id"].to_pylist() == ["SPB-100"]


def test_failure_report_pure_arrow_write(s3_env):
    sample_rows = [
        {
            "bikeNo": "SPB-100",
            "regDttm": "2026-08-22 03:00:00",
            "mlangComCdName": "체인",
        }
    ]

    with patch("jobs.daily_batch_failure_report.fetch_failure_reports_by_date", return_value=sample_rows), \
         patch("jobs.daily_batch_failure_report.overwrite_partition") as mock_overwrite:
        count = process_failure_report(date(2026, 8, 22))

    assert count == 1
    mock_overwrite.assert_called_once()
    table_name, arrow_table, col, val = mock_overwrite.call_args[0]
    assert table_name == "bronze.failure_report"
    assert col == "reg_date_partition"
    assert val == "2026-08-22"


def test_bikeman_event_valid_and_quarantine_split(s3_env):
    sample_rows = [
        {
            "event_id": "EV-1",
            "event_type": "COLLECT",
            "bike_id": "SPB-001",
            "station_id": "ST-10",
            "worker_id": "W-1",
            "occurred_at": "2026-08-22 10:00:00",
            "received_at": "2026-08-22 10:05:00",
        },
        {
            "event_id": "EV-2",
            "event_type": "UNKNOWN_TYPE",  # invalid -> quarantine
            "bike_id": "SPB-002",
            "station_id": "ST-11",
            "worker_id": "W-2",
            "occurred_at": "2026-08-22 11:00:00",
            "received_at": "2026-08-22 11:05:00",
        },
    ]

    with patch("jobs.daily_batch_bikeman_event.fetch_events_by_date", return_value=sample_rows), \
         patch("jobs.daily_batch_bikeman_event.overwrite_partition") as mock_overwrite, \
         patch("jobs.daily_batch_bikeman_event.append") as mock_append:
        count = process_bikeman_event(date(2026, 8, 22))

    assert count == 1  # 1 valid row
    mock_overwrite.assert_called_once()
    assert mock_overwrite.call_args[0][0] == "bronze.bikeman_event"

    mock_append.assert_called_once()  # 1 quarantined row
    assert mock_append.call_args[0][0] == "bronze.bikeman_event_quarantine"
