"""silver_rental_history 백필 청크 목록 계산 테스트 (#232)."""
from datetime import date
from unittest import mock

import jobs.compute_silver_rental_history_backfill_ranges as compute_ranges


def test_splits_into_chunk_days_windows():
    ranges = compute_ranges.build_ranges(
        silver_watermark=date(2017, 2, 26),
        bronze_watermark=date(2017, 3, 29),
        chunk_days=10,
        total_days_cap=3650,
    )
    assert ranges == [
        {"start": "2017-02-27", "end": "2017-03-08"},
        {"start": "2017-03-09", "end": "2017-03-18"},
        {"start": "2017-03-19", "end": "2017-03-28"},
        {"start": "2017-03-29", "end": "2017-03-29"},
    ]


def test_returns_empty_when_silver_caught_up():
    ranges = compute_ranges.build_ranges(
        silver_watermark=date(2017, 3, 29),
        bronze_watermark=date(2017, 3, 29),
        chunk_days=31,
        total_days_cap=3650,
    )
    assert ranges == []


def test_total_days_cap_limits_total_span():
    ranges = compute_ranges.build_ranges(
        silver_watermark=date(2017, 1, 1),
        bronze_watermark=date(2020, 1, 1),
        chunk_days=31,
        total_days_cap=31,
    )
    assert ranges == [{"start": "2017-01-02", "end": "2017-02-01"}]


def test_run_prints_json_array(capsys):
    with mock.patch.object(compute_ranges, "read_watermark", side_effect=[date(2017, 3, 29), date(2017, 2, 26)]):
        compute_ranges.run(chunk_days=31, total_days_cap=3650)
    out = capsys.readouterr().out
    assert '"start": "2017-02-27"' in out
    assert '"end": "2017-03-29"' in out
