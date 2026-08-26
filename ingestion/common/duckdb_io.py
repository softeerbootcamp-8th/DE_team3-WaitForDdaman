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

import os
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

# Spark 제거(#142/#143) 이후 DuckDB가 Airflow 워커(호스트) 메모리 안에서 직접 도는데,
# duckdb.connect()는 기본적으로 쿼리 하나가 시스템 메모리의 최대 80%까지 자유롭게 쓴다.
# Spark는 서브프로세스 OOM이 나도 워커가 살아남는 격리막이었지만, 인프로세스 전환 뒤엔
# 그 격리가 없어져 워커(호스트) 전체가 단일 장애점이 된다 - #144가 예견했으나 완료조건
# 미충족인 채 방치됐다가 #285에서 재확인됨. 이 헬퍼로 모든 연결에 상한을 강제한다.
_DEFAULT_MEMORY_LIMIT = "3GB"
_DEFAULT_THREADS = "2"


def connect(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """memory_limit/threads가 강제된 duckdb 커넥션을 반환한다 (#285).

    상한값은 DUCKDB_MEMORY_LIMIT/DUCKDB_THREADS 환경변수로 오버라이드할 수 있다 -
    t4g.xlarge(16GB) 기준 BRONZE_POOL(1) + SILVER_POOL(1) + GOLD_POOL(1)이 동시에
    최대로 돌아도(3 x 3GB = 9GB) OS/Airflow 자체 오버헤드를 뺀 예산 안에 들어오도록
    기본값(3GB)을 잡았다.
    """
    con = duckdb.connect(database)
    memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", _DEFAULT_MEMORY_LIMIT)
    threads = os.environ.get("DUCKDB_THREADS", _DEFAULT_THREADS)
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    return con


def query_arrow(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: Optional[Sequence[Any]] = None,
) -> pa.Table:
    """sql을 실행하고 결과를 PyArrow Table로 materialize해서 반환한다."""
    result = con.execute(sql, params) if params is not None else con.execute(sql)
    return getattr(result, _ARROW_METHOD)()
