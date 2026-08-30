"""selection.json 계약 테스트.

NOTE: test_watermark.py와 같은 이유로 config.SETTINGS.env를 "aws"로 교체해 moto의
가상 AWS를 쓴다 (moto는 커스텀 endpoint_url을 가로채지 못함).
"""
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from moto import mock_aws

import config as config_module

KST = ZoneInfo("Asia/Seoul")
BUCKET = "test-selection-bucket"
CUTOFF = "2026-08-22T06:00:00+09:00"


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket=BUCKET,
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket(BUCKET)
        yield


@pytest.fixture
def base_env(monkeypatch):
    monkeypatch.setenv("COLLECTION_CUTOFF_AT", CUTOFF)
    monkeypatch.setenv("MAX_DAYS_PER_RUN", "3")
    monkeypatch.delenv("RENTAL_HISTORY_FALLBACK_ENABLED", raising=False)
    monkeypatch.delenv("RENTAL_HISTORY_T0_ENABLED", raising=False)
    monkeypatch.delenv("RENTAL_HISTORY_PRELIMINARY_MAX_AGE_MINUTES", raising=False)


def _prefix(target_date: str, observed_at: str, snapshot_type: str) -> str:
    observed_key = datetime.fromisoformat(observed_at).astimezone(KST).strftime(
        "%Y%m%dT%H%M%S%z"
    )
    return (
        f"raw/rental_history/api/target_date={target_date}/"
        f"observed_at={observed_key}/snapshot_type={snapshot_type}/"
    )


def _put_snapshot(
    target_date: str,
    observed_at: str,
    snapshot_type: str,
    hours: list[int],
    *,
    with_payload: bool = True,
    **overrides,
) -> dict:
    from common.s3_utils import put_json

    prefix = _prefix(target_date, observed_at, snapshot_type)
    payload_key = f"{prefix}payload.json"
    manifest = {
        "dataset": "rental_history",
        "target_date": target_date,
        "observed_at": datetime.fromisoformat(observed_at).astimezone(KST).isoformat(),
        "snapshot_type": snapshot_type,
        "status": "COMPLETE",
        "requested_hours": list(hours),
        "completed_hours": list(hours),
        "page_count": len(hours),
        "row_count": 1000,
        "schema_valid": True,
        "payload_key": payload_key,
        "error": None,
    }
    manifest.update(overrides)
    if with_payload:
        put_json(BUCKET, payload_key, [{"BIKE_ID": "SPB-1"}])
    put_json(BUCKET, f"{prefix}manifest.json", manifest)
    return manifest


def _set_watermark(watermark: date) -> None:
    from common.watermark import write_watermark

    write_watermark(watermark)


def test_selects_final_and_writes_normal_selection(s3_env, base_env, capsys):
    from common.s3_utils import get_json
    from bronze import select_rental_history_snapshot as selector

    _set_watermark(date(2026, 8, 20))
    final = _put_snapshot("2026-08-21", CUTOFF, "FINAL", list(range(24)))
    _put_snapshot(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", list(range(24))
    )

    document = selector.run()

    assert document["mode"] == "NORMAL"
    assert document["dataset"] == "rental_history"
    assert document["run_date"] == "2026-08-22"
    assert document["promotion_id"] == "20260822T060000+0900"
    assert document["collection_cutoff_at"] == CUTOFF
    assert document["fallback_enabled"] is False
    assert document["t0_enabled"] is False
    assert document["required_confirmed_dates"] == ["2026-08-21"]
    assert document["current_date_required"] is False
    assert [s["payload_key"] for s in document["selected_snapshots"]] == [
        final["payload_key"]
    ]

    stored = get_json(BUCKET, document["selection_key"])
    assert stored["selected_snapshots"] == document["selected_snapshots"]

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["promotion_id"] == "20260822T060000+0900"
    assert summary["mode"] == "NORMAL"
    assert summary["selection_key"] == document["selection_key"]


def test_falls_back_to_latest_valid_preliminary(s3_env, base_env, monkeypatch):
    from bronze import select_rental_history_snapshot as selector

    monkeypatch.setenv("RENTAL_HISTORY_FALLBACK_ENABLED", "true")
    _set_watermark(date(2026, 8, 20))
    _put_snapshot(
        "2026-08-21",
        CUTOFF,
        "FINAL",
        list(range(24)),
        status="INCOMPLETE",
        completed_hours=[0, 1],
    )
    _put_snapshot(
        "2026-08-21", "2026-08-22T04:30:00+09:00", "PRELIMINARY", list(range(24))
    )
    newest = _put_snapshot(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", list(range(24))
    )

    document = selector.run()

    assert document["mode"] == "DEGRADED"
    (selected,) = document["selected_snapshots"]
    assert selected["snapshot_type"] == "PRELIMINARY"
    assert selected["payload_key"] == newest["payload_key"]
    assert selected["fallback_reason"] == "FINAL_INCOMPLETE"


def test_fallback_disabled_fails_instead_of_hiding_final_failure(s3_env, base_env):
    from bronze import select_rental_history_snapshot as selector

    _set_watermark(date(2026, 8, 20))
    _put_snapshot(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", list(range(24))
    )

    with pytest.raises(selector.SnapshotSelectionError, match="2026-08-21"):
        selector.run()


@pytest.mark.parametrize(
    "overrides, extra",
    [
        ({"status": "INCOMPLETE", "completed_hours": [0, 1]}, {}),
        ({"status": "COMPLETE_EMPTY", "row_count": 0}, {}),
        ({"schema_valid": False}, {}),
        ({}, {"with_payload": False}),
        ({"requested_hours": list(range(23))}, {}),
    ],
)
def test_unusable_preliminary_is_never_selected(
    s3_env, base_env, monkeypatch, overrides, extra
):
    from bronze import select_rental_history_snapshot as selector

    monkeypatch.setenv("RENTAL_HISTORY_FALLBACK_ENABLED", "true")
    _set_watermark(date(2026, 8, 20))
    _put_snapshot(
        "2026-08-21",
        "2026-08-22T05:00:00+09:00",
        "PRELIMINARY",
        list(range(24)),
        **extra,
        **overrides,
    )

    with pytest.raises(selector.SnapshotSelectionError, match="2026-08-21"):
        selector.run()


@pytest.mark.parametrize(
    "observed_at",
    ["2026-08-22T03:59:00+09:00", "2026-08-21T05:00:00+09:00"],
)
def test_stale_or_cross_day_preliminary_is_rejected(
    s3_env, base_env, monkeypatch, observed_at
):
    from bronze import select_rental_history_snapshot as selector

    monkeypatch.setenv("RENTAL_HISTORY_FALLBACK_ENABLED", "true")
    _set_watermark(date(2026, 8, 20))
    _put_snapshot("2026-08-21", observed_at, "PRELIMINARY", list(range(24)))

    with pytest.raises(selector.SnapshotSelectionError, match="2026-08-21"):
        selector.run()


def test_preliminary_current_day_uses_its_own_observed_hours_when_t0_enabled(
    s3_env, base_env, monkeypatch
):
    from bronze import select_rental_history_snapshot as selector

    monkeypatch.setenv("RENTAL_HISTORY_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("RENTAL_HISTORY_T0_ENABLED", "true")
    _set_watermark(date(2026, 8, 20))
    _put_snapshot("2026-08-21", CUTOFF, "FINAL", list(range(24)))
    current = _put_snapshot(
        "2026-08-22", "2026-08-22T05:00:00+09:00", "PRELIMINARY", [0, 1, 2, 3, 4]
    )

    document = selector.run()

    assert document["current_date_required"] is True
    assert document["mode"] == "DEGRADED"
    assert [s["target_date"] for s in document["selected_snapshots"]] == [
        "2026-08-21",
        "2026-08-22",
    ]
    assert document["selected_snapshots"][1]["payload_key"] == current["payload_key"]


def test_optional_current_day_snapshot_is_not_promoted_when_t0_disabled(
    s3_env, base_env
):
    from bronze import select_rental_history_snapshot as selector

    _set_watermark(date(2026, 8, 20))
    _put_snapshot("2026-08-21", CUTOFF, "FINAL", list(range(24)))
    _put_snapshot("2026-08-22", CUTOFF, "FINAL", [0, 1, 2, 3, 4, 5])

    document = selector.run()

    assert [s["target_date"] for s in document["selected_snapshots"]] == ["2026-08-21"]
    assert document["current_date_required"] is False


def test_selection_is_noop_when_watermark_already_current(s3_env, base_env):
    from bronze import select_rental_history_snapshot as selector

    _set_watermark(date(2026, 8, 21))

    document = selector.run()

    assert document["selected_snapshots"] == []
    assert document["required_confirmed_dates"] == []
    assert document["mode"] == "NORMAL"


def test_final_from_another_run_is_not_reused(s3_env, base_env):
    from bronze import select_rental_history_snapshot as selector

    _set_watermark(date(2026, 8, 20))
    _put_snapshot(
        "2026-08-21", "2026-08-22T07:00:00+09:00", "FINAL", list(range(24))
    )

    with pytest.raises(selector.SnapshotSelectionError, match="2026-08-21"):
        selector.run()
