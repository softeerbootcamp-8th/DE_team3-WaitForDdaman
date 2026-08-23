"""#149: 학습(Spark)과 추론(DuckDB)이 같은 SQL 정의를 쓰는지 병행 검증.

같은 손으로 만든 소규모 입력을 두 엔진에 각각 넣고 결과가 일치하는지 확인한다
(이슈 완료 조건: "같은 앵커에 대해 Spark 경로와 DuckDB 경로의 피처 값 일치").
"""
from __future__ import annotations

from datetime import date, datetime

import duckdb
import pandas as pd
import pytest

from pipeline.train_risk_model.features import (
    _bike_class_sql,
    _fault_transform,
    _rental_transform,
    anchor_frame,
    apply_trip_filters,
    build_daily_agg,
    build_excluded,
    build_fault_features,
    build_samples,
    build_usage_features,
)
from pipeline.train_risk_model.settings import Config
from pipeline.train_risk_model.sql_engine import SqlEngine

CFG = Config(
    {
        "cleaning": {
            "instant_return_max_dist_m": 10,
            "max_speed_kmh": 45,
            "max_dist_m": 50000,
            "zero_dur_max_dist": 5000,
        },
        "run": {"fail_window_days": 150, "saessak_min_num": 80000},
    }
)

RENT_ROWS = [
    ("B1", datetime(2026, 1, 1, 9, 0), 1000.0, 10.0, "ST-1", "ST-2"),  # 일반 트립
    ("B1", datetime(2026, 1, 5, 9, 0), 5.0, 2.0, "ST-2", "ST-2"),  # 즉시반납 (같은 대여소, 5m)
    ("B1", datetime(2026, 1, 10, 9, 0), None, 3.0, "ST-2", "ST-3"),  # dist_m 결측
    ("B2", datetime(2026, 1, 2, 9, 0), 2000.0, 15.0, "ST-4", "ST-5"),
]
RENT_COLS = ["bike_id", "rent_at", "dist_m", "dur_min", "rent_station", "return_station"]

FAULT_ROWS = [
    # as_of=2026-01-15 기준: fail_150d 컷오프=2025-08-18, exclude_recent_days(30) 컷오프=2025-12-16
    ("B1", date(2026, 1, 10)),  # 150일 창 안 + 30일 창 안(최근 신고, excluded 대상)
    ("B1", date(2025, 9, 1)),  # 150일 창 안(컷오프 이후) + 30일 창 밖
    ("B2", date(2025, 6, 1)),  # 150일 창 밖(컷오프 이전) + 30일 창 밖
]
FAULT_COLS = ["bike_id", "reg_date"]

AS_OF = date(2026, 1, 15)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-features-parity")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _spark_engine(spark):
    return SqlEngine.for_spark(spark)


def _duckdb_engine():
    return SqlEngine.for_duckdb(duckdb.connect(":memory:"))


def _spark_rent(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("bike_id", T.StringType()),
            T.StructField("rent_at", T.TimestampType()),
            T.StructField("dist_m", T.DoubleType()),
            T.StructField("dur_min", T.DoubleType()),
            T.StructField("rent_station", T.StringType()),
            T.StructField("return_station", T.StringType()),
        ]
    )
    return spark.createDataFrame(list(RENT_ROWS), schema)


def _duckdb_rent():
    return pd.DataFrame(RENT_ROWS, columns=RENT_COLS)


def _spark_fault(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [T.StructField("bike_id", T.StringType()), T.StructField("reg_date", T.DateType())]
    )
    return spark.createDataFrame(list(FAULT_ROWS), schema)


def _duckdb_fault():
    return pd.DataFrame(FAULT_ROWS, columns=FAULT_COLS)


def _spark_anchors(spark):
    return anchor_frame(_spark_engine(spark), [AS_OF])


def _duckdb_anchors():
    return anchor_frame(_duckdb_engine(), [AS_OF])


def _rows(result, dialect: str) -> list[dict]:
    if dialect == "spark":
        rows = [r.asDict() for r in result.collect()]
    else:
        rows = result.to_pylist()
    # 함수마다 없는 컬럼도 있어(build_daily_agg엔 as_of가 없음) 두 키 다 문자열로
    # 안전하게 뽑아 정렬한다 - 엔진마다 그룹핑 결과 순서가 달라도 비교 가능하게.
    return sorted(
        rows,
        key=lambda r: (
            str(r.get("as_of", "")),
            r["bike_id"],
            str(r.get("rent_date", "")),
        ),
    )


def test_build_daily_agg_matches_between_engines(spark):
    spark_result = _rows(build_daily_agg(_spark_engine(spark), _spark_rent(spark), CFG), "spark")
    duckdb_result = _rows(build_daily_agg(_duckdb_engine(), _duckdb_rent(), CFG), "duckdb")

    assert spark_result == duckdb_result
    # B1의 즉시반납 1건 확인 (같은 대여소, 5m <= 10m 임계값)
    b1_jan5 = [r for r in spark_result if r["bike_id"] == "B1" and str(r["rent_date"]) == "2026-01-05"]
    assert b1_jan5[0]["instant_ret"] == 1


def test_build_usage_features_matches_between_engines(spark):
    window = 14
    spark_daily = build_daily_agg(_spark_engine(spark), _spark_rent(spark), CFG)
    duckdb_daily = build_daily_agg(_duckdb_engine(), _duckdb_rent(), CFG)

    spark_eng, duckdb_eng = _spark_engine(spark), _duckdb_engine()
    spark_result = _rows(
        build_usage_features(spark_eng, spark_daily, _spark_anchors(spark), window), "spark"
    )
    duckdb_result = _rows(
        build_usage_features(duckdb_eng, duckdb_daily, _duckdb_anchors(), window), "duckdb"
    )

    assert spark_result == duckdb_result
    b1 = [r for r in spark_result if r["bike_id"] == "B1"][0]
    assert b1["trips"] == 3
    assert b1["days_since_last_rent"] == 5  # as_of(01-15) - last_rent_date(01-10)


def test_build_fault_features_matches_between_engines(spark):
    spark_result = _rows(
        build_fault_features(_spark_engine(spark), _spark_fault(spark), _spark_anchors(spark), CFG),
        "spark",
    )
    duckdb_result = _rows(
        build_fault_features(_duckdb_engine(), _duckdb_fault(), _duckdb_anchors(), CFG), "duckdb"
    )

    assert spark_result == duckdb_result
    b1 = [r for r in spark_result if r["bike_id"] == "B1"][0]
    assert b1["fail_150d"] == 2  # 2026-01-10, 2025-09-01 둘 다 150일 창(컷오프 2025-08-18) 안
    b2 = [r for r in spark_result if r["bike_id"] == "B2"][0]
    assert b2["fail_150d"] == 0  # 2025-06-01은 150일 창 밖


def test_build_excluded_matches_between_engines(spark):
    exclude_recent_days = 30
    spark_result = _rows(
        build_excluded(
            _spark_engine(spark), _spark_fault(spark), _spark_anchors(spark), exclude_recent_days
        ),
        "spark",
    )
    duckdb_result = _rows(
        build_excluded(_duckdb_engine(), _duckdb_fault(), _duckdb_anchors(), exclude_recent_days),
        "duckdb",
    )

    assert spark_result == duckdb_result
    excluded_bikes = {r["bike_id"] for r in spark_result}
    assert excluded_bikes == {"B1"}  # B1의 2026-01-10 신고가 30일 창(컷오프 2025-12-16) 안, B2는 창 밖


RENTAL_CFG = Config(
    {
        "sources": {
            "mode": "iceberg",
            "rental_columns": {
                "bike_id": "bike_id",
                "rent_at": "rent_at",
                "return_at": "return_at",
                "dist_m": "dist_m",
                "rent_station": "rent_station",
                "return_station": "return_station",
                "dur_min": None,  # 명시 컬럼 없음 - return_at/rent_at에서 파생시켜야 함
            },
        }
    }
)

RAW_RENTAL_ROWS = [
    # 공백 2군데(정규식 전역치환 검증) + dur_min 파생(반납-대여, 초->분) 둘 다 걸리는 케이스
    ("B 1  X", datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 9, 30), 1000.0, "ST-1", "ST-2"),
    (None, datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 9, 30), 1000.0, "ST-1", "ST-2"),  # bike_id 결측 -> 제외
]
RAW_RENTAL_COLS = ["bike_id", "rent_at", "return_at", "dist_m", "rent_station", "return_station"]

FAULT_CFG = Config({"sources": {"mode": "iceberg", "fault_columns": {"bike_id": "bike_id", "reported_at": "reg_dttm"}}})

RAW_FAULT_ROWS = [
    ("b 2  y", datetime(2026, 1, 3, 10, 0)),
    (None, datetime(2026, 1, 3, 10, 0)),  # bike_id 결측 -> 제외
]
RAW_FAULT_COLS = ["bike_id", "reg_dttm"]


def test_rental_transform_matches_between_engines(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("bike_id", T.StringType()),
            T.StructField("rent_at", T.TimestampType()),
            T.StructField("return_at", T.TimestampType()),
            T.StructField("dist_m", T.DoubleType()),
            T.StructField("rent_station", T.StringType()),
            T.StructField("return_station", T.StringType()),
        ]
    )
    spark_eng = _spark_engine(spark)
    spark_eng.register("rental_raw", spark.createDataFrame(list(RAW_RENTAL_ROWS), schema))
    duckdb_eng = _duckdb_engine()
    duckdb_eng.register("rental_raw", pd.DataFrame(RAW_RENTAL_ROWS, columns=RAW_RENTAL_COLS))

    spark_result = _rows(_rental_transform(spark_eng, RENTAL_CFG), "spark")
    duckdb_result = _rows(_rental_transform(duckdb_eng, RENTAL_CFG), "duckdb")

    assert spark_result == duckdb_result
    assert len(spark_result) == 1  # bike_id 결측 행은 빠짐
    row = spark_result[0]
    assert row["bike_id"] == "B1X"  # 공백 전역 치환 + 대문자화
    assert row["dur_min"] == 30.0  # 09:30 - 09:00 = 30분


def test_fault_transform_matches_between_engines(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [T.StructField("bike_id", T.StringType()), T.StructField("reg_dttm", T.TimestampType())]
    )
    spark_eng = _spark_engine(spark)
    spark_eng.register("fault_raw", spark.createDataFrame(list(RAW_FAULT_ROWS), schema))
    duckdb_eng = _duckdb_engine()
    duckdb_eng.register("fault_raw", pd.DataFrame(RAW_FAULT_ROWS, columns=RAW_FAULT_COLS))

    spark_result = _rows(_fault_transform(spark_eng, FAULT_CFG), "spark")
    duckdb_result = _rows(_fault_transform(duckdb_eng, FAULT_CFG), "duckdb")

    assert spark_result == duckdb_result
    assert len(spark_result) == 1
    assert spark_result[0]["bike_id"] == "B2Y"
    assert spark_result[0]["reg_date"] == date(2026, 1, 3)


TRIP_ROWS = [
    # (bike_id, rent_at, return_at, dist_m, rent_station, return_station, dur_min)
    ("B1", datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 9, 10), 1000.0, "ST-1", "ST-2", 10.0),  # 정상(6km/h)
    ("B2", datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 9, 1), 5000.0, "ST-1", "ST-2", 1.0),  # 과속(300km/h) 제외
    ("B3", datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 10, 40), 60000.0, "ST-1", "ST-2", 100.0),  # 거리초과 제외
    ("B4", datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 9, 0), 6000.0, "ST-1", "ST-1", 0.0),  # 즉시반납인데 6km 이동 - 이상치 제외
]
TRIP_COLS = ["bike_id", "rent_at", "return_at", "dist_m", "rent_station", "return_station", "dur_min"]


def test_apply_trip_filters_matches_between_engines(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("bike_id", T.StringType()),
            T.StructField("rent_at", T.TimestampType()),
            T.StructField("return_at", T.TimestampType()),
            T.StructField("dist_m", T.DoubleType()),
            T.StructField("rent_station", T.StringType()),
            T.StructField("return_station", T.StringType()),
            T.StructField("dur_min", T.DoubleType()),
        ]
    )
    spark_eng = _spark_engine(spark)
    duckdb_eng = _duckdb_engine()

    spark_result = _rows(
        apply_trip_filters(spark_eng, spark.createDataFrame(list(TRIP_ROWS), schema), CFG), "spark"
    )
    duckdb_result = _rows(
        apply_trip_filters(duckdb_eng, pd.DataFrame(TRIP_ROWS, columns=TRIP_COLS), CFG), "duckdb"
    )

    assert spark_result == duckdb_result
    assert {r["bike_id"] for r in spark_result} == {"B1"}  # B2/B3/B4 전부 이상치로 제외


def test_anchor_frame_matches_between_engines(spark):
    spark_rows = [r.asDict() for r in anchor_frame(_spark_engine(spark), [AS_OF]).collect()]
    duckdb_rows = anchor_frame(_duckdb_engine(), [AS_OF]).to_pylist()

    assert spark_rows == duckdb_rows == [{"as_of": AS_OF}]


def test_bike_class_sql_saessak_vs_normal_vs_unknown():
    duckdb_eng = _duckdb_engine()
    duckdb_eng.register(
        "t", pd.DataFrame({"bike_id": ["ST-80001", "ST-79999", "NO-DIGITS"]})
    )
    expr = _bike_class_sql(duckdb_eng.dialect, "bike_id", 80000)
    result = duckdb_eng.sql(f"SELECT bike_id, {expr} AS bike_class FROM t")

    by_bike = dict(zip(result.to_pydict()["bike_id"], result.to_pydict()["bike_class"]))
    assert by_bike == {"ST-80001": "saessak", "ST-79999": "normal", "NO-DIGITS": "unknown"}


def test_build_samples_for_inference_matches_between_engines(spark):
    """#149 완료 조건: 같은 앵커에 대해 Spark 경로와 DuckDB 경로의 피처 값이 일치해야 한다.

    build_bike_features_daily.py(추론)가 실제로 쓰는 것과 동일한 경로 -
    rent/fault를 미리 넘겨서(override) read_rental/read_fault의 카탈로그 읽기는
    건너뛰고, build_samples()의 조립 로직 전체를 검증한다.
    """
    spark_eng = _spark_engine(spark)
    duckdb_eng = _duckdb_engine()

    spark_out = build_samples(
        spark_eng,
        CFG,
        [AS_OF],
        anchor_type="serve",
        rent=_spark_rent(spark),
        fault=_spark_fault(spark),
        with_labels=False,
    )
    duckdb_out = build_samples(
        duckdb_eng,
        CFG,
        [AS_OF],
        anchor_type="serve",
        rent=_duckdb_rent(),
        fault=_duckdb_fault(),
        with_labels=False,
    )

    spark_rows = _rows(spark_out, "spark")
    duckdb_rows = _rows(duckdb_out, "duckdb")
    for rows in (spark_rows, duckdb_rows):
        for r in rows:
            # label은 항상 NULL이라 존재만 확인 - Spark/DuckDB의 NULL bigint 표현
            # 방식 자체가 달라 원천적으로 값 비교 대상이 아니다.
            assert r.pop("label") is None
            # ingested_at은 CURRENT_TIMESTAMP라 두 엔진 실행 시점(실제 벽시계)이
            # 다르면 값이 갈리는 게 정상 - 컬럼 존재만 확인하고 비교에서 뺀다.
            assert r.pop("ingested_at") is not None

    assert spark_rows == duckdb_rows
    assert {r["bike_id"] for r in spark_rows} == {"B1", "B2"}
    b1 = [r for r in spark_rows if r["bike_id"] == "B1"][0]
    assert b1["snapshot_date"] == AS_OF
    assert b1["anchor_type"] == "serve"
    assert b1["feature_version"] == "v1"
    assert isinstance(b1["excluded"], bool)


def test_build_excluded_zero_days_returns_empty_on_both_engines(spark):
    spark_result = _rows(
        build_excluded(_spark_engine(spark), _spark_fault(spark), _spark_anchors(spark), 0), "spark"
    )
    duckdb_result = _rows(build_excluded(_duckdb_engine(), _duckdb_fault(), _duckdb_anchors(), 0), "duckdb")

    assert spark_result == duckdb_result == []
