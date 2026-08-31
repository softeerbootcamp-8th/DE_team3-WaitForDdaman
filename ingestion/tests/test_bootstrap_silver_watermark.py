"""bootstrap_silver_watermark의 대상 데이터셋 매핑 테스트 (#288)."""

import pytest

from config.watermark_keys import SILVER_FAILURE_REPORT, SILVER_RENTAL_HISTORY
from operations.bootstrap_silver_watermark import DATASETS


def test_failure_report_is_a_bootstrap_target():
    """확정 구간 증분(#288)으로 바뀐 뒤 Silver failure_report도 하한이 필요해졌다.

    부트스트랩이 없으면 read_watermark가 backfill_start_date(기본 2015-01-01)로
    폴백해서, 데이터가 2021-02부터인데 6년치 빈 구간을 처리하려 든다.
    """
    assert DATASETS["failure_report"] == (
        "bronze.failure_report",
        "reg_date_partition",
        SILVER_FAILURE_REPORT,
    )


def test_rental_history_mapping_is_unchanged():
    assert DATASETS["rental_history"] == (
        "bronze.rental_history",
        "rent_date_partition",
        SILVER_RENTAL_HISTORY,
    )


@pytest.mark.parametrize("dataset", ["bikeman_event", "station_master", "station_active"])
def test_non_incremental_sources_are_not_bootstrap_targets(dataset):
    """워터마크 기반 증분이 아닌 원천은 대상이 아니다 - station류는 워터마크 자체가 없다."""
    assert dataset not in DATASETS
