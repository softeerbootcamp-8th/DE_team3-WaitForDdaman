"""
DuckDB 조회 결과를 PyArrow Table로 받는 공통 헬퍼

왜 별도 함수가 필요한가 (실측, 2026-08-22):
  - `con.execute(sql).arrow()`는 duckdb 버전에 따라 반환 타입이 다르다. 0.10대에서는
    pa.Table을 주지만 1.5대에서는 RecordBatchReader를 준다. 이걸 그대로 다시
    `con.register(...)`에 넣고 같은 커넥션에서 질의하면, 아직 스트리밍 중인 결과를
    자기 자신이 읽으려 해서 프로세스가 그대로 멈춘다(교착).
  - `fetch_arrow_table()`은 모든 버전에서 pa.Table을 즉시 materialize해주지만
    1.4부터 deprecated라 호출할 때마다 DeprecationWarning이 찍힌다. 후속 이름은
    `to_arrow_table()`인데 구버전엔 없다.

두 이름 중 있는 쪽을 골라 쓰고, 반환 타입은 항상 pa.Table로 고정한다.
requirements의 duckdb>=0.10.0 범위 어디서든 같은 동작을 보장하기 위한 얇은 층이다.
"""
from __future__ import annotations

from typing import Any, Optional, Sequence

import duckdb
import pyarrow as pa

# 커넥션이 아니라 결과 객체(DuckDBPyConnection)에 붙어 있는 메서드다 - execute()가
# 커넥션 자신을 돌려주므로 클래스에서 한 번만 확인해두면 된다.
_ARROW_METHOD = (
    "to_arrow_table"
    if hasattr(duckdb.DuckDBPyConnection, "to_arrow_table")
    else "fetch_arrow_table"
)


def query_arrow(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: Optional[Sequence[Any]] = None,
) -> pa.Table:
    """sql을 실행하고 결과를 PyArrow Table로 materialize해서 반환한다."""
    result = con.execute(sql, params) if params is not None else con.execute(sql)
    return getattr(result, _ARROW_METHOD)()
