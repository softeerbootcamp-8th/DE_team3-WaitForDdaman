"""silver.bikeman_action DuckDB/PyIceberg 전환 계약 테스트 (#143)."""
from datetime import date, datetime, timezone

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from jobs.silver_bikeman_action import (
    BACKUP_TABLE,
    PARTITION_COLUMN,
    REBUILD_TABLE,
    SILVER_COLUMNS,
    SILVER_PARTITION_SPEC,
    SILVER_SCHEMA,
    SILVER_TABLE,
    _ensure_silver_table,
    _processing_window,
    transform,
    validate,
)


def _bronze_table(*rows: dict) -> pa.Table:
    defaults = {
        "bike_id": "SPB-10001",
        "event_type": "COLLECT",
        "occurred_at": "2026-08-21 04:00:00",
        "station_id": "ST-1",
        "received_at": "2026-08-21 04:01:00",
    }
    values = [{**defaults, **row} for row in (rows or ({},))]
    return pa.Table.from_pylist(values)


def test_transform_preserves_columns_and_adds_identity_partition():
    silver, quarantine = transform(
        _bronze_table(),
        date(2026, 8, 21),
        datetime(2026, 8, 22, tzinfo=timezone.utc),
    )

    assert silver.column_names == SILVER_COLUMNS
    assert silver.to_pylist()[0][PARTITION_COLUMN] == "2026-08-21"
    assert len(quarantine) == 0


def test_transform_deduplicates_same_bike_event_and_time():
    silver, _ = transform(
        _bronze_table({}, {}),
        date(2026, 8, 21),
        datetime(2026, 8, 22, tzinfo=timezone.utc),
    )

    assert len(silver) == 1


def test_transform_routes_invalid_time_to_quarantine():
    silver, quarantine = transform(
        _bronze_table({"occurred_at": "2026-08-21 04:02:00"}),
        date(2026, 8, 21),
        datetime(2026, 8, 22, tzinfo=timezone.utc),
    )

    assert len(silver) == 0
    assert "occurred_after_received" in quarantine.to_pylist()[0]["quarantine_reason"]


def test_partition_spec_is_identity_on_occurred_date_partition():
    field = SILVER_PARTITION_SPEC.fields[0]
    assert field.name == PARTITION_COLUMN
    assert field.transform.__class__.__name__ == "IdentityTransform"
    assert SILVER_SCHEMA.find_field(field.source_id).name == PARTITION_COLUMN


def test_validate_matches_previous_pydeequ_constraints():
    silver, _ = transform(
        _bronze_table(),
        date(2026, 8, 21),
        datetime(2026, 8, 22, tzinfo=timezone.utc),
    )

    assert validate(silver).is_success


def test_interrupted_swap_finishes_rebuild_instead_of_creating_empty_table():
    rebuilt = object()

    class InterruptedSwapCatalog:
        def __init__(self):
            self.tables = {BACKUP_TABLE: object(), REBUILD_TABLE: rebuilt}

        def create_namespace_if_not_exists(self, _namespace):
            return None

        def load_table(self, identifier):
            try:
                return self.tables[identifier]
            except KeyError as exc:
                raise NoSuchTableError(identifier) from exc

        def rename_table(self, source, target):
            self.tables[target] = self.tables.pop(source)

        def create_table(self, *_args, **_kwargs):
            raise AssertionError("중단된 swap을 신규 빈 테이블 생성으로 처리하면 안 된다")

    catalog = InterruptedSwapCatalog()

    result = _ensure_silver_table(catalog, date(2026, 8, 21))

    assert result is rebuilt
    assert SILVER_TABLE in catalog.tables
    assert REBUILD_TABLE not in catalog.tables


def test_asset_rerun_reprocesses_lookback_when_watermark_is_already_yesterday():
    reprocess_start, watermark_start = _processing_window(
        last_processed=date(2026, 8, 21),
        end_date=date(2026, 8, 21),
    )

    assert reprocess_start == date(2026, 8, 19)
    assert watermark_start == date(2026, 8, 22)
