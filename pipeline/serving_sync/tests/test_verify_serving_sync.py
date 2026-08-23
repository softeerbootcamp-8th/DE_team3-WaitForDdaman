"""verify_serving_sync의 순수 로직(파티션 카운트 집계, 카탈로그 접두사 제거) 테스트 (#172)."""
from datetime import date

import pandas as pd
import pytest

from verify_serving_sync import (
    ServingSyncVerificationError,
    _partition_row_count,
    _strip_catalog_prefix,
)


def partitions_df(rows: list[tuple]) -> pd.DataFrame:
    """rows: [(snapshot_date, record_count), ...] - 실제 table.inspect.partitions()의
    'partition'(struct) / 'record_count' 컬럼 모양을 흉내낸다."""
    return pd.DataFrame(
        {
            "partition": [{"snapshot_date": d} for d, _ in rows],
            "record_count": [c for _, c in rows],
        }
    )


def test_partition_row_count_matches_target_date():
    df = partitions_df([(date(2026, 8, 18), 37079)])
    assert _partition_row_count(df, date(2026, 8, 18)) == 37079


def test_partition_row_count_zero_when_date_not_present():
    df = partitions_df([(date(2026, 8, 17), 100)])
    assert _partition_row_count(df, date(2026, 8, 18)) == 0


def test_partition_row_count_zero_on_empty_table():
    df = partitions_df([])
    assert _partition_row_count(df, date(2026, 8, 18)) == 0


def test_strip_catalog_prefix_removes_matching_prefix():
    assert _strip_catalog_prefix("bike_catalog.gold.mart_bike_risk_daily", "bike_catalog") == "gold.mart_bike_risk_daily"


def test_strip_catalog_prefix_leaves_unprefixed_identifier_unchanged():
    assert _strip_catalog_prefix("gold.mart_bike_risk_daily", "bike_catalog") == "gold.mart_bike_risk_daily"


def test_verification_error_is_exception():
    assert issubclass(ServingSyncVerificationError, Exception)
