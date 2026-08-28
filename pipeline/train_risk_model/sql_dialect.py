"""features.py의 SQL 템플릿에서 실제로 Spark SQL과 DuckDB SQL이 갈리는
지점만 모아둔 헬퍼 (#149). 날짜 리터럴(윈도우 일수 등)은 항상 파이썬에서
알고 있는 정수라서 문자열로 미리 박아 넣는다 - SQL 쪽에서 표현식으로
계산할 필요가 없다.
"""
from __future__ import annotations


def date_sub_days(dialect: str, col_expr: str, days: int) -> str:
    """col_expr에서 days일을 뺀 날짜."""
    if dialect == "spark":
        return f"date_sub({col_expr}, {int(days)})"
    return f"({col_expr} - INTERVAL '{int(days)}' DAY)"


def date_add_days(dialect: str, col_expr: str, days: int) -> str:
    """col_expr에 days일을 더한 날짜."""
    if dialect == "spark":
        return f"date_add({col_expr}, {int(days)})"
    return f"({col_expr} + INTERVAL '{int(days)}' DAY)"


def days_between(dialect: str, start_expr: str, end_expr: str) -> str:
    """end_expr - start_expr, 일 단위."""
    if dialect == "spark":
        return f"datediff({end_expr}, {start_expr})"
    return f"date_diff('day', {start_expr}, {end_expr})"


def escape_regex_literal(dialect: str, pattern: str) -> str:
    """SQL 문자열 리터럴에 넣을 정규식 패턴의 백슬래시를 방언에 맞게 처리한다.

    Spark SQL 문자열 리터럴은 백슬래시를 이스케이프해야 한다(\\s+ -> \\\\s+) -
    안 하면 파서가 \\s를 인식 못 하는 이스케이프로 보고 백슬래시를 버려서 패턴이
    s+(리터럴 's')가 돼버린다. DuckDB는 이스케이프 없이 그대로 받는다.
    """
    return pattern.replace("\\", "\\\\") if dialect == "spark" else pattern


def regexp_replace_global(dialect: str, col_expr: str, pattern: str, replacement: str) -> str:
    """패턴에 매칭되는 모든 곳을 치환한다 (전역 치환).

    Spark SQL의 regexp_replace는 인자가 3개뿐이고 항상 전역 치환이다. DuckDB는
    기본이 "첫 매치만" 치환이라 4번째 인자로 'g' 플래그를 명시해야 한다 - 안 하면
    공백이 여러 군데인 bike_id에서 첫 공백만 지워지는 조용한 버그가 된다
    (#149에서 실측 발견). 백슬래시 이스케이프는 escape_regex_literal() 참고.
    """
    escaped_pattern = escape_regex_literal(dialect, pattern)
    if dialect == "spark":
        return f"regexp_replace({col_expr}, '{escaped_pattern}', '{replacement}')"
    return f"regexp_replace({col_expr}, '{escaped_pattern}', '{replacement}', 'g')"


def epoch_seconds(dialect: str, col_expr: str) -> str:
    """타임스탬프를 UNIX epoch 초로. Spark는 timestamp->bigint 캐스트가 곧 이거지만
    DuckDB는 직접 캐스트가 안 되고 epoch() 함수가 필요하다."""
    if dialect == "spark":
        return f"CAST({col_expr} AS BIGINT)"
    return f"epoch({col_expr})"
