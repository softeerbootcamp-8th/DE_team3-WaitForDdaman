"""
silver.station_active 정제 로직 테스트

브론즈는 원본 보존을 위해 전부 STRING이다. station_active는 station_id +
snapshot_date만 남기는 필터 테이블이라, station_id null 드롭 / 중복 제거 두
가지만 검증하면 된다.

#143에서 Spark를 걷어내고 DuckDB로 옮겼으므로 SparkSession 대신 PyArrow Table을
직접 만들어 넣는다 - Iceberg/S3도 필요 없다.
"""
from datetime import date

import pyarrow as pa

from silver.silver_station_active import SILVER_COLUMNS, normalize

# 실버가 읽는 브론즈 컬럼 (전부 STRING, daily_batch_station_active.py 참고)
BRONZE_COLUMNS = ["snapshot_date", "station_id"]

DEFAULT_ROW = {
    "snapshot_date": "2026-08-14",
    "station_id": "ST-4",
}


def bronze_table(*overrides) -> pa.Table:
    """브론즈 모양의 PyArrow Table을 만든다. 인자마다 한 행이 된다."""
    rows = []
    for over in overrides or [{}]:
        row = dict(DEFAULT_ROW)
        row.update(over)
        rows.append(row)
    return pa.table(
        {col: pa.array([r[col] for r in rows], type=pa.string()) for col in BRONZE_COLUMNS}
    )


def test_keeps_only_station_id_and_snapshot_date():
    table = normalize(bronze_table())
    assert table.column_names == SILVER_COLUMNS


def test_snapshot_date_becomes_date():
    row = normalize(bronze_table({"snapshot_date": "2026-08-14"})).to_pylist()[0]
    assert row["snapshot_date"] == date(2026, 8, 14)


def test_output_arrow_types_match_iceberg_schema():
    schema = normalize(bronze_table()).schema
    assert schema.field("snapshot_date").type == pa.date32()
    assert schema.field("station_id").type == pa.string()


def test_null_station_id_is_dropped():
    table = normalize(bronze_table({"station_id": "ST-1"}, {"station_id": None}))
    assert table.num_rows == 1
    assert table.to_pylist()[0]["station_id"] == "ST-1"


def test_duplicate_station_id_is_deduped():
    table = normalize(
        bronze_table(
            {"station_id": "ST-1", "snapshot_date": "2026-08-14"},
            {"station_id": "ST-1", "snapshot_date": "2026-08-14"},
        )
    )
    assert table.num_rows == 1


def test_distinct_station_ids_all_kept():
    table = normalize(
        bronze_table({"station_id": "ST-1"}, {"station_id": "ST-2"}, {"station_id": "ST-3"})
    )
    assert table.num_rows == 3
    assert {r["station_id"] for r in table.to_pylist()} == {"ST-1", "ST-2", "ST-3"}


def test_empty_input_yields_empty_output():
    """0행이면 run()이 적재를 중단시킨다 - normalize 자체는 조용히 0행을 돌려줘야 한다."""
    empty = pa.table({col: pa.array([], type=pa.string()) for col in BRONZE_COLUMNS})
    assert normalize(empty).num_rows == 0
