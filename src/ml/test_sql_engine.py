"""SqlEngine 어댑터 테스트 (#149).

Spark DataFrame / DuckDB pa.Table을 register()로 이름 붙이고 sql()로 그
이름을 참조하는 SQL을 실행했을 때, 두 엔진에서 동일하게 동작하는지 검증한다.
"""
from __future__ import annotations

import duckdb

from common.duckdb_io import connect
import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.expressions import GreaterThanOrEqual

from ml.sql_engine import SqlEngine


class _FakeIcebergTable:
    def __init__(self, arrow_table: pa.Table):
        self._arrow_table = arrow_table
        self.scan_calls: list = []

    def scan(self, row_filter=None):
        self.scan_calls.append(row_filter)
        return self

    def to_arrow(self) -> pa.Table:
        return self._arrow_table


class _FakeCatalog:
    def __init__(self, table: _FakeIcebergTable):
        self._table = table

    def load_table(self, table_ref: str) -> _FakeIcebergTable:
        return self._table


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-sql-engine")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_duckdb_register_then_sql_roundtrip():
    engine = SqlEngine.for_duckdb(connect())
    engine.register("t", pd.DataFrame({"bike_id": ["B1", "B2"], "trips": [3, 5]}))

    result = engine.sql("SELECT bike_id, trips FROM t WHERE trips > 3")

    assert result.to_pydict() == {"bike_id": ["B2"], "trips": [5]}


def test_spark_register_then_sql_roundtrip(spark):
    engine = SqlEngine.for_spark(spark)
    df = spark.createDataFrame([("B1", 3), ("B2", 5)], ["bike_id", "trips"])
    engine.register("t", df)

    result = engine.sql("SELECT bike_id, trips FROM t WHERE trips > 3")

    assert [r.asDict() for r in result.collect()] == [{"bike_id": "B2", "trips": 5}]


def test_duckdb_dialect_is_duckdb():
    engine = SqlEngine.for_duckdb(connect())
    assert engine.dialect == "duckdb"


def test_spark_dialect_is_spark(spark):
    engine = SqlEngine.for_spark(spark)
    assert engine.dialect == "spark"


def test_sql_result_can_be_registered_for_next_step():
    """체이닝: sql() 결과를 다시 register()해서 다음 SQL에서 참조 가능해야 한다."""
    engine = SqlEngine.for_duckdb(connect())
    engine.register("t", pd.DataFrame({"bike_id": ["B1", "B2"], "trips": [3, 5]}))

    step1 = engine.sql("SELECT bike_id, trips * 2 AS trips2 FROM t")
    engine.register("step1", step1)
    step2 = engine.sql("SELECT bike_id FROM step1 WHERE trips2 > 8")

    assert step2.to_pydict() == {"bike_id": ["B2"]}


def test_read_table_duckdb_forwards_row_filter(monkeypatch):
    fake_table = _FakeIcebergTable(pa.table({"bike_id": pa.array([], type=pa.string())}))
    monkeypatch.setattr(
        "common.iceberg_catalog.build_iceberg_catalog", lambda: _FakeCatalog(fake_table)
    )
    engine = SqlEngine.for_duckdb(connect())
    row_filter = GreaterThanOrEqual("rent_date_partition", "2026-08-01")

    engine.read_table("silver.rental_history", "rental_raw", row_filter=row_filter)

    assert fake_table.scan_calls == [row_filter]


def test_read_table_duckdb_without_row_filter_scans_everything(monkeypatch):
    fake_table = _FakeIcebergTable(pa.table({"bike_id": pa.array([], type=pa.string())}))
    monkeypatch.setattr(
        "common.iceberg_catalog.build_iceberg_catalog", lambda: _FakeCatalog(fake_table)
    )
    engine = SqlEngine.for_duckdb(connect())

    engine.read_table("silver.rental_history", "rental_raw")

    assert fake_table.scan_calls == [None]
