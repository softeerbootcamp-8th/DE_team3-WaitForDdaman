"""Bronze 승격 잡의 매핑/멱등성/PyIceberg 다중 파티션 승격 테스트 (#194 - Spark 제거)."""
import json
from datetime import date, datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from moto import mock_aws
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo, Or

import config as config_module
from bronze import promote_rental_history_raw as promoter

KST = ZoneInfo("Asia/Seoul")
BUCKET = "test-promotion-bucket"
CUTOFF = "2026-08-22T06:00:00+09:00"

VALID_ROW = {
    "BIKE_ID": "SPB-1",
    "RENT_DT": "2026-08-21 04:00:00",
    "RENT_ID": "101",
    "RTN_DT": "2026-08-21 04:10:00",
    "RTN_ID": "102",
    "USE_MIN": "10",
    "USE_DST": "1234.5",
    "START_INDEX": 1,
    "END_INDEX": 1,
    "RNUM": "1",
}


def _payload_key(target_date: str, observed_at: str, snapshot_type: str) -> str:
    observed_key = datetime.fromisoformat(observed_at).astimezone(KST).strftime(
        "%Y%m%dT%H%M%S%z"
    )
    return (
        f"raw/rental_history/api/target_date={target_date}/"
        f"observed_at={observed_key}/snapshot_type={snapshot_type}/payload.json"
    )


def _selected(
    target_date: str,
    observed_at: str,
    snapshot_type: str,
    hours: list[int],
    row_count: int = 2,
    fallback_reason: str | None = None,
) -> dict:
    payload_key = _payload_key(target_date, observed_at, snapshot_type)
    return {
        "target_date": target_date,
        "snapshot_type": snapshot_type,
        "observed_at": datetime.fromisoformat(observed_at).astimezone(KST).isoformat(),
        "payload_key": payload_key,
        "manifest_key": payload_key.replace("payload.json", "manifest.json"),
        "requested_hours": list(hours),
        "row_count": row_count,
        "fallback_reason": fallback_reason,
    }


def _selection_document(selected: list[dict], mode: str = "NORMAL") -> dict:
    return {
        "dataset": "rental_history",
        "run_date": "2026-08-22",
        "promotion_id": "20260822T060000+0900",
        "collection_cutoff_at": CUTOFF,
        "source_bucket": BUCKET,
        "fallback_enabled": mode == "DEGRADED",
        "t0_enabled": False,
        "required_confirmed_dates": [
            s["target_date"] for s in selected if s["target_date"] != "2026-08-22"
        ],
        "current_date_required": any(s["target_date"] == "2026-08-22" for s in selected),
        "selected_snapshots": selected,
        "mode": mode,
        "selection_key": (
            "_meta/promotion/bronze_rental_history/run_date=2026-08-22/"
            "promotion_id=20260822T060000+0900/selection.json"
        ),
    }


# ------------------------------------------------------------------ mapping


def test_source_file_is_the_selected_payload_key_verbatim():
    selected = _selected(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", list(range(24))
    )
    document = _selection_document([selected], mode="DEGRADED")

    plan = promoter.plan_promotion(document)

    assert [item["rent_date_partition"] for item in plan] == ["2026-08-21"]
    assert plan[0]["source_file"] == selected["payload_key"]
    assert plan[0]["source_file"].startswith("raw/rental_history/api/target_date=")
    assert not plan[0]["source_file"].startswith("s3://")
    assert plan[0]["source_file"].endswith("payload.json")


def test_validate_selection_document_rejects_inconsistent_contract():
    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )
    promoter.validate_selection_document(document, promotion_id="20260822T060000+0900")

    with pytest.raises(promoter.PromotionError, match="promotion_id"):
        promoter.validate_selection_document(document, promotion_id="20260822T070000+0900")

    broken = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )
    broken["selected_snapshots"][0]["payload_key"] = _payload_key(
        "2026-08-20", CUTOFF, "FINAL"
    )
    with pytest.raises(promoter.PromotionError, match="payload_key"):
        promoter.validate_selection_document(broken, promotion_id="20260822T060000+0900")

    empty = _selection_document([])
    with pytest.raises(promoter.PromotionError, match="selected_snapshots"):
        promoter.validate_selection_document(empty, promotion_id="20260822T060000+0900")


def test_build_promotion_document_records_commit_marker():
    selected = [
        _selected("2026-08-20", CUTOFF, "FINAL", list(range(24)), row_count=5),
        _selected(
            "2026-08-21",
            "2026-08-22T05:00:00+09:00",
            "PRELIMINARY",
            list(range(24)),
            row_count=7,
            fallback_reason="FINAL_INCOMPLETE",
        ),
    ]
    document = _selection_document(selected, mode="DEGRADED")

    promotion = promoter.build_promotion_document(
        document, {"2026-08-20": 5, "2026-08-21": 7}
    )

    assert promotion["status"] == "COMPLETE"
    assert promotion["mode"] == "DEGRADED"
    assert promotion["promoted_partitions"] == ["2026-08-20", "2026-08-21"]
    assert promotion["bronze_row_count_by_partition"] == {
        "2026-08-20": 5,
        "2026-08-21": 7,
    }
    assert promotion["confirmed_through_candidate"] == "2026-08-21"
    assert promotion["promotion_reasons"] == {"2026-08-21": "FINAL_INCOMPLETE"}
    assert promotion["selected_snapshots"] == selected
    assert promotion["source_bucket"] == BUCKET
    assert promotion["promoted_at"].endswith("+00:00")


def test_partial_day_promotion_is_not_a_confirmed_candidate():
    selected = [
        _selected("2026-08-21", CUTOFF, "FINAL", list(range(24)), row_count=5),
        _selected("2026-08-22", CUTOFF, "FINAL", [0, 1, 2, 3, 4, 5], row_count=3),
    ]
    document = _selection_document(selected)

    promotion = promoter.build_promotion_document(
        document, {"2026-08-21": 5, "2026-08-22": 3}
    )

    assert promotion["promoted_partitions"] == ["2026-08-21", "2026-08-22"]
    assert promotion["confirmed_through_candidate"] == "2026-08-21"


def test_row_count_mismatch_blocks_the_commit_marker():
    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)), row_count=5)]
    )

    with pytest.raises(promoter.PromotionError, match="row count"):
        promoter.build_promotion_document(document, {"2026-08-21": 4})


# --------------------------------------------------------------- payload IO


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


def test_missing_payload_fails_before_spark_starts(s3_env):
    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )

    with pytest.raises(promoter.PromotionError, match="payload"):
        promoter.load_payloads(BUCKET, promoter.plan_promotion(document))


def test_load_payloads_returns_rows_per_partition(s3_env):
    from common.s3_utils import put_json

    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )
    plan = promoter.plan_promotion(document)
    put_json(BUCKET, plan[0]["source_file"], [VALID_ROW, VALID_ROW])

    loaded = promoter.load_payloads(BUCKET, plan)

    assert [item["rent_date_partition"] for item in loaded] == ["2026-08-21"]
    assert loaded[0]["rows"] == [VALID_ROW, VALID_ROW]


def test_payload_row_count_must_match_the_manifest(s3_env):
    from common.s3_utils import put_json

    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)), row_count=2)]
    )
    plan = promoter.plan_promotion(document)
    put_json(BUCKET, plan[0]["source_file"], [VALID_ROW])

    with pytest.raises(promoter.PromotionError, match="row count"):
        promoter.load_payloads(BUCKET, plan)


# ---------------------------------------------------------- PyArrow 매핑


def test_build_bronze_arrow_table_maps_columns_and_nulls_missing_optional():
    arrow_table = promoter.build_bronze_arrow_table(
        [VALID_ROW, VALID_ROW], "2026-08-21", "raw/rental_history/api/.../payload.json"
    )

    assert arrow_table.schema == promoter.ARROW_SCHEMA
    assert arrow_table.column("bike_id").to_pylist() == ["SPB-1", "SPB-1"]
    assert arrow_table.column("rent_station_no").to_pylist() == ["101", "101"]
    assert arrow_table.column("return_station_no").to_pylist() == ["102", "102"]
    # BIKE_SE_CD가 VALID_ROW에 없음(선택 컬럼) -> null로 채워지고 실패하지 않아야 함
    assert arrow_table.column("bike_se_cd").to_pylist() == [None, None]
    assert arrow_table.column("rent_date_partition").to_pylist() == [
        "2026-08-21",
        "2026-08-21",
    ]
    assert arrow_table.column("source_file").to_pylist() == [
        "raw/rental_history/api/.../payload.json",
    ] * 2


def test_build_bronze_arrow_table_rejects_missing_required_columns():
    incomplete_row = {k: v for k, v in VALID_ROW.items() if k != "RENT_DT"}

    with pytest.raises(Exception, match="필수 컬럼"):
        promoter.build_bronze_arrow_table([incomplete_row], "2026-08-21", "src")


# --------------------------------------------------- Bronze 테이블 선행 조건


def test_load_bronze_table_missing_requires_initial_load_first():
    class MissingTableCatalog:
        def load_table(self, identifier):
            raise NoSuchTableError(identifier)

    with pytest.raises(promoter.PromotionError, match="initial_load_rental_history"):
        promoter.load_bronze_table(catalog=MissingTableCatalog())


# --------------------------------------------- 다중 파티션 단일 snapshot 승격


def _mock_bronze_table(committed_counts: dict) -> MagicMock:
    """overwrite() 이후 재조회하면 committed_counts를 반환하는 가짜 Bronze 테이블."""
    table = MagicMock()
    table.name.return_value = "bronze.rental_history"
    table.refresh.return_value = table

    partition_values = [
        partition for partition, count in committed_counts.items() for _ in range(count)
    ]
    committed_arrow = pa.table({"rent_date_partition": partition_values})
    scan_result = MagicMock()
    scan_result.to_arrow.return_value = committed_arrow
    table.scan.return_value = scan_result
    return table


def test_promote_replaces_multiple_partitions_in_a_single_overwrite_call(monkeypatch):
    table = _mock_bronze_table({"2026-08-20": 2, "2026-08-21": 3})
    monkeypatch.setattr(promoter, "load_bronze_table", lambda: table)

    payloads = [
        {
            "rent_date_partition": "2026-08-20",
            "source_file": "s1",
            "row_count": 2,
            "rows": [dict(VALID_ROW, BIKE_ID=f"SPB-20-{i}") for i in range(2)],
        },
        {
            "rent_date_partition": "2026-08-21",
            "source_file": "s2",
            "row_count": 3,
            "rows": [dict(VALID_ROW, BIKE_ID=f"SPB-21-{i}") for i in range(3)],
        },
    ]

    counts = promoter.promote(payloads)

    assert counts == {"2026-08-20": 2, "2026-08-21": 3}

    # 파티션이 2개여도 overwrite()는 정확히 한 번만 호출돼야 한다 - 그래야 한 snapshot
    # commit이 원자 경계가 되고, 일부 파티션만 먼저 반영되는 중간 상태가 생기지 않는다.
    table.overwrite.assert_called_once()
    args, kwargs = table.overwrite.call_args
    committed_arrow_table = args[0]
    assert len(committed_arrow_table) == 5
    assert set(committed_arrow_table.column("source_file").to_pylist()) == {"s1", "s2"}

    # 여러 값을 커버하려면 EqualTo 하나가 아니라 OR(EqualTo(...), EqualTo(...))여야 한다.
    overwrite_filter = kwargs["overwrite_filter"]
    assert isinstance(overwrite_filter, Or)
    assert overwrite_filter == Or(
        EqualTo("rent_date_partition", "2026-08-20"),
        EqualTo("rent_date_partition", "2026-08-21"),
    )


def test_promote_row_count_mismatch_surfaces_in_committed_counts(monkeypatch):
    """커밋 후 실제 파티션 행 수가 입력과 다르면 build_promotion_document가 이를 잡아낸다."""
    table = _mock_bronze_table({"2026-08-21": 1})  # 입력은 2행인데 커밋 후 1행만 조회됨
    monkeypatch.setattr(promoter, "load_bronze_table", lambda: table)

    payloads = [
        {
            "rent_date_partition": "2026-08-21",
            "source_file": "s1",
            "row_count": 2,
            "rows": [dict(VALID_ROW, BIKE_ID=f"SPB-{i}") for i in range(2)],
        },
    ]

    counts = promoter.promote(payloads)
    assert counts == {"2026-08-21": 1}

    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)), row_count=2)]
    )
    with pytest.raises(promoter.PromotionError, match="row count"):
        promoter.build_promotion_document(document, counts)
