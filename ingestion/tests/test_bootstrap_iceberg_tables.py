"""
bootstrap_iceberg_tables 단위 테스트 (Issue #216)

실제 pyiceberg SqlCatalog(sqlite + 로컬 파일시스템 warehouse)를 tmp_path에 띄워서
검증한다 - S3/moto가 필요 없다(create_table/load_table은 카탈로그 DB와 로컬 warehouse
경로만 건드림). 신규 JDBC 카탈로그에 필요한 Bronze 테이블만 생성하며 기존 데이터를
스캔하거나 마이그레이션하지 않는다.
"""
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

from jobs.bootstrap_iceberg_tables import BRONZE_TABLE_SPECS, _bootstrap_table, run
from jobs.daily_batch_station_active import ARROW_SCHEMA as STATION_ACTIVE_ARROW_SCHEMA
from jobs.daily_batch_station_master import ARROW_SCHEMA as STATION_MASTER_ARROW_SCHEMA

EXPECTED_IDENTIFIERS = {
    "bronze.rental_history",
    "bronze.failure_report",
    "bronze.station_master",
    "bronze.station_active",
    "bronze.bikeman_event",
    "bronze.bikeman_event_quarantine",
}


@pytest.fixture
def local_catalog(tmp_path):
    """S3 없이 sqlite + 로컬 파일시스템 warehouse로 뜨는 실제 pyiceberg SqlCatalog."""
    return SqlCatalog(
        "test_catalog",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )


@pytest.fixture
def patched_run(monkeypatch, local_catalog):
    """run()이 이 로컬 카탈로그를 쓰게 하고, S3 버킷 보장 호출은 스킵한다."""
    monkeypatch.setattr("jobs.bootstrap_iceberg_tables.build_iceberg_catalog", lambda: local_catalog)
    monkeypatch.setattr("jobs.bootstrap_iceberg_tables.ensure_bucket", lambda *_a, **_kw: None)
    return local_catalog


def test_run_creates_all_expected_bronze_tables(patched_run):
    catalog = patched_run
    run()

    for identifier in EXPECTED_IDENTIFIERS:
        table = catalog.load_table(identifier)
        assert table is not None


def test_run_covers_every_table_listed_in_issue_216(patched_run):
    identifiers = {spec.identifier for spec in BRONZE_TABLE_SPECS}
    assert identifiers == EXPECTED_IDENTIFIERS


def test_partitioned_tables_use_identity_partition_on_documented_column(patched_run):
    catalog = patched_run
    run()

    expected_partition_column = {
        "bronze.rental_history": "rent_date_partition",
        "bronze.failure_report": "reg_date_partition",
        "bronze.station_master": "snapshot_date",
        "bronze.station_active": "snapshot_date",
        "bronze.bikeman_event": "occurred_date_partition",
    }
    for identifier, column in expected_partition_column.items():
        fields = catalog.load_table(identifier).spec().fields
        assert len(fields) == 1
        assert fields[0].name == column
        assert fields[0].transform.__class__.__name__ == "IdentityTransform"


def test_bikeman_event_quarantine_table_is_unpartitioned(patched_run):
    catalog = patched_run
    run()

    table = catalog.load_table("bronze.bikeman_event_quarantine")
    assert table.spec().fields == ()


def test_run_is_idempotent_second_run_creates_nothing_new(patched_run):
    catalog = patched_run
    run()
    first_snapshot_ids = {
        identifier: catalog.load_table(identifier).current_snapshot()
        for identifier in EXPECTED_IDENTIFIERS
    }

    run()  # 재실행 - 예외 없이 끝나야 하고, 기존 테이블을 다시 만들면 안 된다

    for identifier in EXPECTED_IDENTIFIERS:
        assert catalog.load_table(identifier).current_snapshot() == first_snapshot_ids[identifier]


def test_run_does_not_touch_data_of_a_preexisting_table(patched_run):
    """이미 존재하는 테이블(과 그 데이터)은 손대지 않는다 - Issue #216 회귀 조건."""
    catalog = patched_run
    run()  # 먼저 전부 생성

    table = catalog.load_table("bronze.station_active")
    arrow_table = pa.table(
        {f.name: pa.array([None], type=pa.string() if f.field_type.__class__.__name__ != "TimestamptzType" else pa.timestamp("us", tz="UTC")) for f in table.schema().fields}
    )
    table.append(arrow_table)
    assert len(table.scan().to_arrow()) == 1

    run()  # 재실행 - 기존 데이터가 있는 테이블은 건드리지 않는다

    reloaded = catalog.load_table("bronze.station_active")
    assert len(reloaded.scan().to_arrow()) == 1


def test_bootstrap_table_returns_false_when_already_exists(patched_run):
    catalog = patched_run
    spec = next(s for s in BRONZE_TABLE_SPECS if s.identifier == "bronze.station_active")
    catalog.create_namespace_if_not_exists("bronze")

    assert _bootstrap_table(catalog, spec) is True
    assert _bootstrap_table(catalog, spec) is False


def test_station_active_schema_matches_daily_batch_arrow_schema(patched_run):
    """daily_batch_station_active.py가 실제로 쓰는 컬럼과 부트스트랩 스키마가 어긋나면
    daily_batch가 새 테이블에 첫 적재를 시도할 때 컬럼 불일치로 실패한다."""
    catalog = patched_run
    run()

    bootstrapped_columns = [f.name for f in catalog.load_table("bronze.station_active").schema().fields]
    assert bootstrapped_columns == STATION_ACTIVE_ARROW_SCHEMA.names


def test_station_master_schema_matches_daily_batch_arrow_schema(patched_run):
    catalog = patched_run
    run()

    bootstrapped_columns = [f.name for f in catalog.load_table("bronze.station_master").schema().fields]
    assert bootstrapped_columns == STATION_MASTER_ARROW_SCHEMA.names


def test_load_table_before_bootstrap_raises_no_such_table(local_catalog):
    """부트스트랩 전에는 테이블이 없다는 걸 명확히 한다 (Issue #216이 고치는 증상 자체)."""
    with pytest.raises(NoSuchTableError):
        local_catalog.load_table("bronze.station_active")
