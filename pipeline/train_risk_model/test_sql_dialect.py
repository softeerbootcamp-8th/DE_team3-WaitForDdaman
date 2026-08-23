"""sql_dialect.py 헬퍼 테스트 (#149) - 문자열 조립만 하는 순수 함수라 엔진 없이 검증."""
from pipeline.train_risk_model.sql_dialect import (
    date_add_days,
    date_sub_days,
    days_between,
    epoch_seconds,
    regexp_replace_global,
)


def test_date_sub_days_spark():
    assert date_sub_days("spark", "a.as_of", 14) == "date_sub(a.as_of, 14)"


def test_date_sub_days_duckdb():
    assert date_sub_days("duckdb", "a.as_of", 14) == "(a.as_of - INTERVAL '14' DAY)"


def test_date_add_days_spark():
    assert date_add_days("spark", "as_of", 14) == "date_add(as_of, 14)"


def test_date_add_days_duckdb():
    assert date_add_days("duckdb", "as_of", 14) == "(as_of + INTERVAL '14' DAY)"


def test_days_between_spark():
    assert days_between("spark", "start", "end") == "datediff(end, start)"


def test_days_between_duckdb():
    assert days_between("duckdb", "start", "end") == "date_diff('day', start, end)"


def test_regexp_replace_global_spark_has_no_flag():
    assert regexp_replace_global("spark", "c", r"\s+", "") == r"regexp_replace(c, '\\s+', '')"


def test_regexp_replace_global_duckdb_has_g_flag():
    assert regexp_replace_global("duckdb", "c", r"\s+", "") == r"regexp_replace(c, '\s+', '', 'g')"


def test_epoch_seconds_spark():
    assert epoch_seconds("spark", "ts") == "CAST(ts AS BIGINT)"


def test_epoch_seconds_duckdb():
    assert epoch_seconds("duckdb", "ts") == "epoch(ts)"
