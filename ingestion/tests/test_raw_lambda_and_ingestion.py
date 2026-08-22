"""
Lambda raw 수집 및 Bronze 일 배치 S3/API 소스 분기 테스트 (Issue #141)
"""
import json
import os
import sys
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from moto import mock_aws

import config as config_module
from common.s3_utils import get_s3_client, put_json
from infra.lambdas.fetch_station_master_raw.lambda_function import (
    fetch_all_station_master,
    lambda_handler as master_lambda_handler,
)
from infra.lambdas.fetch_station_active_raw.lambda_function import (
    fetch_all_station_active,
    lambda_handler as active_lambda_handler,
)

BUCKET = "test-raw-lambda-bucket"


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket=BUCKET,
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


# ------------------------------------------------------------------------------
# Lambda unit tests
# ------------------------------------------------------------------------------
def test_fetch_station_master_lambda_success(s3_env, monkeypatch):
    monkeypatch.setenv("RAW_BUCKET", BUCKET)
    monkeypatch.setenv("SEOUL_API_KEY", "test-api-key")

    mock_rows = [
        {"STATION_NO": "108", "STATION_ID": "ST-10", "STATION_NAME": "서교동", "HOLD_NUM": "10"},
        {"STATION_NO": "109", "STATION_ID": "ST-11", "STATION_NAME": "합정역", "HOLD_NUM": "15"},
    ]

    with patch("infra.lambdas.fetch_station_master_raw.lambda_function.fetch_all_station_master", return_value=mock_rows):
        res = master_lambda_handler({"snapshot_date": "2026-08-22"}, None)

    assert res["statusCode"] == 200
    assert res["row_count"] == 2
    assert res["snapshot_date"] == "2026-08-22"

    # Verify S3 object
    obj = s3_env.get_object(Bucket=BUCKET, Key="raw/station_master/api/snapshot_date=2026-08-22/payload.json")
    saved_payload = json.loads(obj["Body"].read().decode("utf-8"))
    assert saved_payload["snapshot_date"] == "2026-08-22"
    assert saved_payload["row_count"] == 2
    assert saved_payload["rows"] == mock_rows


def test_fetch_station_active_lambda_success(s3_env, monkeypatch):
    monkeypatch.setenv("RAW_BUCKET", BUCKET)
    monkeypatch.setenv("SEOUL_API_KEY", "test-api-key")

    mock_rows = [
        {"stationId": "ST-4", "stationName": "마포구청역", "parkingBikeTotCnt": "5", "rackTotCnt": "10"},
    ]

    with patch("infra.lambdas.fetch_station_active_raw.lambda_function.fetch_all_station_active", return_value=mock_rows):
        res = active_lambda_handler({"snapshot_date": "2026-08-22"}, None)

    assert res["statusCode"] == 200
    assert res["row_count"] == 1
    assert res["snapshot_date"] == "2026-08-22"

    obj = s3_env.get_object(Bucket=BUCKET, Key="raw/station_active/api/snapshot_date=2026-08-22/payload.json")
    saved_payload = json.loads(obj["Body"].read().decode("utf-8"))
    assert saved_payload["row_count"] == 1
    assert saved_payload["rows"] == mock_rows


# ------------------------------------------------------------------------------
# Ingestion S3 raw source branch tests
# ------------------------------------------------------------------------------
def test_station_master_process_snapshot_missing_raw_fails_fast(s3_env, monkeypatch):
    from jobs.daily_batch_station_master import _process_snapshot

    with pytest.raises(FileNotFoundError, match="S3 raw payload가 존재하지 않습니다"):
        _process_snapshot("2026-08-22")


def test_station_active_process_snapshot_missing_raw_fails_fast(s3_env, monkeypatch):
    from jobs.daily_batch_station_active import _process_snapshot

    with pytest.raises(FileNotFoundError, match="S3 raw payload가 존재하지 않습니다"):
        _process_snapshot("2026-08-22")


def test_station_master_process_snapshot_reads_s3_payload(s3_env, monkeypatch):
    from jobs.daily_batch_station_master import _process_snapshot

    sample_rows = [
        {
            "STA_LOC": "마포구",
            "RENT_ID": "ST-10",
            "RENT_NO": "108",
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
        {"snapshot_date": "2026-08-22", "row_count": len(sample_rows), "rows": sample_rows},
    )

    with patch("jobs.daily_batch_station_master.overwrite_partition") as mock_overwrite:
        row_count = _process_snapshot("2026-08-22")

    assert row_count == 1
    mock_overwrite.assert_called_once()


def test_station_active_process_snapshot_reads_s3_payload(s3_env, monkeypatch):
    from jobs.daily_batch_station_active import _process_snapshot

    sample_rows = [
        {
            "stationId": "ST-4",
            "stationName": "102. 망원역 1번출구 앞",
            "rackTotCnt": "15",
            "parkingBikeTotCnt": "5",
            "shared": "33",
            "stationLatitude": "37.55564880",
            "stationLongitude": "126.91062927",
        }
    ]
    put_json(
        BUCKET,
        "raw/station_active/api/snapshot_date=2026-08-22/payload.json",
        {"snapshot_date": "2026-08-22", "row_count": len(sample_rows), "rows": sample_rows},
    )

    with patch("jobs.daily_batch_station_active.overwrite_partition") as mock_overwrite:
        row_count = _process_snapshot("2026-08-22")

    assert row_count == 1
    mock_overwrite.assert_called_once()
