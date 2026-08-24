"""대여이력 FINAL/PRELIMINARY 승격 정책의 순수 함수 테스트.

S3나 Spark 없이 window 계산, manifest 검증, 후보 선택, mode 판정만 고정한다.
"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from jobs import rental_history_snapshot_policy as policy

KST = ZoneInfo("Asia/Seoul")

VALID_HOURS_FULL_DAY = list(range(24))


def _cutoff(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(KST)


def _manifest(
    target_date: str,
    observed_at: str,
    snapshot_type: str,
    hours: list[int],
    /,
    **overrides,
):
    payload_key = (
        f"raw/rental_history/api/target_date={target_date}/"
        f"observed_at={_cutoff(observed_at).strftime('%Y%m%dT%H%M%S%z')}/"
        f"snapshot_type={snapshot_type}/payload.json"
    )
    manifest = {
        "dataset": "rental_history",
        "target_date": target_date,
        "observed_at": _cutoff(observed_at).isoformat(),
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
    return manifest


def _candidate(manifest: dict, payload_exists: bool = True) -> policy.Candidate:
    return policy.Candidate(
        manifest_key=manifest["payload_key"].replace("payload.json", "manifest.json"),
        manifest=manifest,
        payload_exists=payload_exists,
    )


# ---------------------------------------------------------------- parse_bool


def test_parse_bool_accepts_only_true_and_false():
    assert policy.parse_bool("true") is True
    assert policy.parse_bool("false") is False
    assert policy.parse_bool(None) is False
    assert policy.parse_bool("") is False
    assert policy.parse_bool("", default=True) is True
    with pytest.raises(ValueError, match="boolean"):
        policy.parse_bool("yes")


# ------------------------------------------------------------ final windows


def test_build_final_windows_caps_oldest_contiguous_backlog_at_three_days():
    windows = policy.build_final_windows(
        cutoff=_cutoff("2026-08-22T06:00:00+09:00"),
        confirmed_through=date(2026, 8, 15),
        max_days=3,
        t0_enabled=False,
    )

    confirmed = [w for w in windows if w.role == policy.ROLE_CONFIRMED]
    assert [w.target_date for w in confirmed] == [
        date(2026, 8, 16),
        date(2026, 8, 17),
        date(2026, 8, 18),
    ]
    assert all(w.hours == VALID_HOURS_FULL_DAY for w in confirmed)
    assert all(w.required for w in confirmed)


def test_build_date_backfill_windows_is_one_required_full_day_without_watermark():
    windows = policy.build_date_backfill_windows(date(2026, 8, 22))

    assert windows == [
        policy.CollectionWindow(
            target_date=date(2026, 8, 22),
            hours=VALID_HOURS_FULL_DAY,
            required=True,
            role=policy.ROLE_CONFIRMED,
        )
    ]


def test_build_final_windows_without_cap_covers_backlog_up_to_yesterday():
    windows = policy.build_final_windows(
        cutoff=_cutoff("2026-08-22T06:00:00+09:00"),
        confirmed_through=date(2026, 8, 19),
        max_days=None,
        t0_enabled=False,
    )

    confirmed = [w for w in windows if w.role == policy.ROLE_CONFIRMED]
    assert [w.target_date for w in confirmed] == [date(2026, 8, 20), date(2026, 8, 21)]


def test_build_final_windows_has_no_confirmed_window_when_watermark_is_current():
    windows = policy.build_final_windows(
        cutoff=_cutoff("2026-08-22T06:00:00+09:00"),
        confirmed_through=date(2026, 8, 21),
        max_days=3,
        t0_enabled=False,
    )

    assert [w for w in windows if w.role == policy.ROLE_CONFIRMED] == []


def test_current_window_is_optional_when_t0_disabled():
    disabled = policy.build_final_windows(
        cutoff=_cutoff("2026-08-22T06:00:00+09:00"),
        confirmed_through=date(2026, 8, 21),
        max_days=3,
        t0_enabled=False,
    )
    enabled = policy.build_final_windows(
        cutoff=_cutoff("2026-08-22T06:00:00+09:00"),
        confirmed_through=date(2026, 8, 21),
        max_days=3,
        t0_enabled=True,
    )

    (optional_current,) = [w for w in disabled if w.role == policy.ROLE_CURRENT]
    (required_current,) = [w for w in enabled if w.role == policy.ROLE_CURRENT]

    assert optional_current.target_date == date(2026, 8, 22)
    assert optional_current.hours == [0, 1, 2, 3, 4, 5]
    assert optional_current.required is False
    assert required_current.required is True


def test_build_final_windows_omits_current_window_at_midnight_cutoff():
    windows = policy.build_final_windows(
        cutoff=_cutoff("2026-08-22T00:00:00+09:00"),
        confirmed_through=date(2026, 8, 20),
        max_days=3,
        t0_enabled=True,
    )

    assert [w.role for w in windows] == [policy.ROLE_CONFIRMED]
    assert [w.target_date for w in windows] == [date(2026, 8, 21)]


# ----------------------------------------------------------- expected hours


def test_preliminary_current_range_uses_its_own_observed_hour():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    observed_at = _cutoff("2026-08-22T05:00:00+09:00")

    assert policy.expected_hours(date(2026, 8, 22), cutoff.date(), cutoff) == [
        0, 1, 2, 3, 4, 5,
    ]
    assert policy.expected_hours(date(2026, 8, 22), cutoff.date(), observed_at) == [
        0, 1, 2, 3, 4,
    ]
    assert (
        policy.expected_hours(date(2026, 8, 21), cutoff.date(), observed_at)
        == VALID_HOURS_FULL_DAY
    )

    manifest = _manifest(
        "2026-08-22", "2026-08-22T05:00:00+09:00", "PRELIMINARY", [0, 1, 2, 3, 4]
    )
    candidate = _candidate(manifest)

    assert (
        policy.validate_manifest(
            manifest=candidate.manifest,
            manifest_key=candidate.manifest_key,
            expected_hours=[0, 1, 2, 3, 4],
        )
        is None
    )
    rejection = policy.validate_manifest(
        manifest=candidate.manifest,
        manifest_key=candidate.manifest_key,
        expected_hours=[0, 1, 2, 3, 4, 5],
    )
    assert rejection.code == policy.REASON_RANGE_MISMATCH


def test_expected_hours_confirmed_role_is_full_day_even_when_target_equals_run_date():
    """#195 회귀: 날짜 backfill/catchup은 target_date == cutoff.date()이면서도

    role=CONFIRMED로 하루 전체(0~23시)를 기대해야 한다. role을 무시하고 날짜
    비교만 하면 실행 시각 직전까지로 좁혀져 정상적으로 수집된 24시간 manifest가
    RANGE_MISMATCH로 거부된다.
    """
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")

    assert (
        policy.expected_hours(
            date(2026, 8, 22), cutoff.date(), cutoff, policy.ROLE_CONFIRMED
        )
        == VALID_HOURS_FULL_DAY
    )
    # role을 넘기지 않으면 기존 날짜 비교 동작을 그대로 유지한다.
    assert policy.expected_hours(date(2026, 8, 22), cutoff.date(), cutoff) == [
        0, 1, 2, 3, 4, 5,
    ]


def test_select_snapshots_accepts_full_day_backfill_targeting_run_date():
    """#195 회귀: 날짜 backfill의 cutoff.date()가 target_date와 같아도

    FINAL manifest가 요청한 24시간 전체가 그대로 승격 대상으로 선택돼야 한다.
    """
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    target_date = date(2026, 8, 22)
    windows = policy.build_date_backfill_windows(target_date)
    final = _manifest(
        "2026-08-22", "2026-08-22T06:00:00+09:00", "FINAL", VALID_HOURS_FULL_DAY
    )

    selection = policy.select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=[_candidate(final)],
        fallback_enabled=False,
    )

    assert selection.missing == []
    assert [s["target_date"] for s in selection.selected] == ["2026-08-22"]
    assert selection.selected[0]["snapshot_type"] == "FINAL"


def test_manifest_from_microsecond_cutoff_is_not_rejected_as_schema_invalid():
    """#182 회귀: collect_rental_history_raw.parse_collection_cutoff()가 마이크로초를
    자르므로, snapshot_keys()가 만든 key와 manifest에 저장된 observed_at이 실제
    프로덕션 경로 그대로도 일치해야 한다. 이 테스트는 test 전용 _cutoff 헬퍼가 아니라
    실제 parse_collection_cutoff를 통과시켜 selector까지 이어지는 전체 경로를 검증한다."""
    from jobs import collect_rental_history_raw as raw_job

    cutoff = raw_job.parse_collection_cutoff("2026-08-22T06:00:00.654321+09:00")
    payload_key, manifest_key = raw_job.snapshot_keys(date(2026, 8, 22), cutoff, "FINAL")

    manifest = {
        "dataset": "rental_history",
        "target_date": "2026-08-22",
        "observed_at": cutoff.astimezone(KST).isoformat(),
        "snapshot_type": "FINAL",
        "status": "COMPLETE",
        "requested_hours": VALID_HOURS_FULL_DAY,
        "completed_hours": VALID_HOURS_FULL_DAY,
        "page_count": 24,
        "row_count": 1000,
        "schema_valid": True,
        "payload_key": payload_key,
        "error": None,
    }

    rejection = policy.validate_manifest(
        manifest=manifest,
        manifest_key=manifest_key,
        expected_hours=VALID_HOURS_FULL_DAY,
    )
    assert rejection is None


# ------------------------------------------------------- manifest rejection


@pytest.mark.parametrize(
    "overrides, expected_code",
    [
        ({"status": "INCOMPLETE", "completed_hours": [0, 1]}, policy.REASON_INCOMPLETE),
        ({"status": "COMPLETE_EMPTY", "row_count": 0}, policy.REASON_EMPTY),
        ({"schema_valid": False}, policy.REASON_SCHEMA_INVALID),
        ({"row_count": 0}, policy.REASON_EMPTY),
        ({"dataset": "failure_report"}, policy.REASON_SCHEMA_INVALID),
        ({"target_date": "2026-08-20"}, policy.REASON_SCHEMA_INVALID),
        ({"payload_key": "raw/rental_history/api/other/payload.json"}, policy.REASON_SCHEMA_INVALID),
        ({"completed_hours": VALID_HOURS_FULL_DAY[:-1]}, policy.REASON_INCOMPLETE),
        ({"requested_hours": VALID_HOURS_FULL_DAY[:-1] + [22]}, policy.REASON_RANGE_MISMATCH),
    ],
)
def test_validate_manifest_rejects_unusable_snapshots(overrides, expected_code):
    manifest = _manifest(
        "2026-08-21", "2026-08-22T06:00:00+09:00", "FINAL", VALID_HOURS_FULL_DAY, **overrides
    )
    candidate = _candidate(manifest)

    rejection = policy.validate_manifest(
        manifest=manifest,
        manifest_key=candidate.manifest_key,
        expected_hours=VALID_HOURS_FULL_DAY,
    )

    assert rejection is not None
    assert rejection.code == expected_code


def test_validate_manifest_rejects_missing_payload_object():
    manifest = _manifest(
        "2026-08-21", "2026-08-22T06:00:00+09:00", "FINAL", VALID_HOURS_FULL_DAY
    )
    candidate = _candidate(manifest, payload_exists=False)

    rejection = policy.validate_manifest(
        manifest=manifest,
        manifest_key=candidate.manifest_key,
        expected_hours=VALID_HOURS_FULL_DAY,
        payload_exists=False,
    )

    assert rejection.code == policy.REASON_MISSING


def test_validate_manifest_rejects_broken_json_and_unparsable_key():
    assert (
        policy.validate_manifest(
            manifest=None,
            manifest_key=(
                "raw/rental_history/api/target_date=2026-08-21/"
                "observed_at=20260822T060000+0900/snapshot_type=FINAL/manifest.json"
            ),
            expected_hours=VALID_HOURS_FULL_DAY,
        ).code
        == policy.REASON_MISSING
    )
    manifest = _manifest(
        "2026-08-21", "2026-08-22T06:00:00+09:00", "FINAL", VALID_HOURS_FULL_DAY
    )
    assert (
        policy.validate_manifest(
            manifest=manifest,
            manifest_key="raw/rental_history/api/target_date=2026-08-21/manifest.json",
            expected_hours=VALID_HOURS_FULL_DAY,
        ).code
        == policy.REASON_SCHEMA_INVALID
    )


def test_final_reason_code_stays_inside_fixed_set():
    for code in (
        policy.REASON_MISSING,
        policy.REASON_INCOMPLETE,
        policy.REASON_EMPTY,
        policy.REASON_SCHEMA_INVALID,
        policy.REASON_RANGE_MISMATCH,
        policy.REASON_STALE_OR_FUTURE,
    ):
        assert policy.final_reason_code(code) in policy.FINAL_REASON_CODES


# ------------------------------------------------------- preliminary staleness


def test_stale_and_cross_day_preliminary_are_rejected():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")

    fresh = policy.validate_preliminary_freshness(
        observed_at=_cutoff("2026-08-22T05:00:00+09:00"),
        cutoff=cutoff,
        max_age_minutes=120,
    )
    assert fresh is None

    too_old = policy.validate_preliminary_freshness(
        observed_at=cutoff - timedelta(minutes=121),
        cutoff=cutoff,
        max_age_minutes=120,
    )
    assert too_old.code == policy.REASON_STALE_OR_FUTURE

    previous_day = policy.validate_preliminary_freshness(
        observed_at=_cutoff("2026-08-21T05:00:00+09:00"),
        cutoff=cutoff,
        max_age_minutes=120,
    )
    assert previous_day.code == policy.REASON_STALE_OR_FUTURE

    after_cutoff = policy.validate_preliminary_freshness(
        observed_at=_cutoff("2026-08-22T06:30:00+09:00"),
        cutoff=cutoff,
        max_age_minutes=120,
    )
    assert after_cutoff.code == policy.REASON_STALE_OR_FUTURE


# ----------------------------------------------------------------- selection


def test_select_snapshots_prefers_final_over_preliminary():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    windows = policy.build_final_windows(cutoff, date(2026, 8, 20), 3, False)
    final = _manifest(
        "2026-08-21", "2026-08-22T06:00:00+09:00", "FINAL", VALID_HOURS_FULL_DAY
    )
    preliminary = _manifest(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", VALID_HOURS_FULL_DAY
    )

    selection = policy.select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=[_candidate(preliminary), _candidate(final)],
        fallback_enabled=True,
    )

    assert selection.mode == policy.MODE_NORMAL
    assert selection.missing == []
    assert [s["snapshot_type"] for s in selection.selected] == ["FINAL"]
    assert selection.selected[0]["payload_key"] == final["payload_key"]
    assert selection.selected[0]["fallback_reason"] is None


def test_select_snapshots_uses_latest_valid_preliminary_when_final_unusable():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    windows = policy.build_final_windows(cutoff, date(2026, 8, 20), 3, False)
    broken_final = _manifest(
        "2026-08-21",
        "2026-08-22T06:00:00+09:00",
        "FINAL",
        VALID_HOURS_FULL_DAY,
        status="INCOMPLETE",
        completed_hours=[0, 1],
    )
    old_preliminary = _manifest(
        "2026-08-21", "2026-08-22T04:30:00+09:00", "PRELIMINARY", VALID_HOURS_FULL_DAY
    )
    new_preliminary = _manifest(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", VALID_HOURS_FULL_DAY
    )

    selection = policy.select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=[
            _candidate(broken_final),
            _candidate(old_preliminary),
            _candidate(new_preliminary),
        ],
        fallback_enabled=True,
    )

    assert selection.mode == policy.MODE_DEGRADED
    assert selection.selected[0]["payload_key"] == new_preliminary["payload_key"]
    assert selection.selected[0]["fallback_reason"] == "FINAL_INCOMPLETE"


def test_select_snapshots_fails_when_fallback_disabled():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    windows = policy.build_final_windows(cutoff, date(2026, 8, 20), 3, False)
    preliminary = _manifest(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", VALID_HOURS_FULL_DAY
    )

    selection = policy.select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=[_candidate(preliminary)],
        fallback_enabled=False,
    )

    assert selection.selected == []
    assert [m["target_date"] for m in selection.missing] == ["2026-08-21"]
    assert selection.missing[0]["reason"] == "FINAL_MISSING"


def test_select_snapshots_ignores_optional_current_window():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    windows = policy.build_final_windows(cutoff, date(2026, 8, 20), 3, False)
    final = _manifest(
        "2026-08-21", "2026-08-22T06:00:00+09:00", "FINAL", VALID_HOURS_FULL_DAY
    )

    selection = policy.select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=[_candidate(final)],
        fallback_enabled=False,
    )

    assert [s["target_date"] for s in selection.selected] == ["2026-08-21"]
    assert selection.missing == []
    assert selection.required_confirmed_dates == ["2026-08-21"]
    assert selection.current_date_required is False


def test_mixed_selection_is_degraded():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    windows = policy.build_final_windows(cutoff, date(2026, 8, 19), 3, True)
    old_final = _manifest(
        "2026-08-20", "2026-08-22T06:00:00+09:00", "FINAL", VALID_HOURS_FULL_DAY
    )
    broken_final = _manifest(
        "2026-08-21",
        "2026-08-22T06:00:00+09:00",
        "FINAL",
        VALID_HOURS_FULL_DAY,
        status="INCOMPLETE",
    )
    preliminary_previous = _manifest(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", VALID_HOURS_FULL_DAY
    )
    preliminary_current = _manifest(
        "2026-08-22", "2026-08-22T05:00:00+09:00", "PRELIMINARY", [0, 1, 2, 3, 4]
    )

    selection = policy.select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=[
            _candidate(old_final),
            _candidate(broken_final),
            _candidate(preliminary_previous),
            _candidate(preliminary_current),
        ],
        fallback_enabled=True,
    )

    assert selection.missing == []
    assert [(s["target_date"], s["snapshot_type"]) for s in selection.selected] == [
        ("2026-08-20", "FINAL"),
        ("2026-08-21", "PRELIMINARY"),
        ("2026-08-22", "PRELIMINARY"),
    ]
    assert selection.mode == policy.MODE_DEGRADED
    assert selection.current_date_required is True


def test_select_snapshots_refuses_preliminary_fallback_for_old_backlog():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    windows = policy.build_final_windows(cutoff, date(2026, 8, 18), 3, False)
    preliminary = _manifest(
        "2026-08-19", "2026-08-22T05:00:00+09:00", "PRELIMINARY", VALID_HOURS_FULL_DAY
    )

    selection = policy.select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=[_candidate(preliminary)],
        fallback_enabled=True,
    )

    assert [m["target_date"] for m in selection.missing] == [
        "2026-08-19",
        "2026-08-20",
        "2026-08-21",
    ]


# ----------------------------------------------------------------- meta keys


def test_promotion_and_selection_keys_are_deterministic():
    cutoff = _cutoff("2026-08-22T06:00:00+09:00")
    promotion_id = policy.build_promotion_id(cutoff)

    assert promotion_id == "20260822T060000+0900"
    prefix = (
        "_meta/promotion/bronze_rental_history/run_date=2026-08-22/"
        "promotion_id=20260822T060000+0900/"
    )
    assert policy.selection_key(cutoff.date(), promotion_id) == f"{prefix}selection.json"
    assert policy.promotion_key(cutoff.date(), promotion_id) == f"{prefix}promotion.json"
