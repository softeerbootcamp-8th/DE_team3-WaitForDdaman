"""Spark/DuckDB register+SQL 인터페이스를 통일하는 얇은 어댑터 (#149).

features.py의 피처 계산 함수들이 이 클래스를 통해 SQL 문자열을 실행하면,
학습(Spark, build_train_samples)과 추론(DuckDB, build_bike_features_daily)이
같은 SQL 정의를 공유할 수 있다 - 방언이 실제로 갈리는 곳(날짜 산술 등)만
호출부에서 engine.dialect로 분기한다.

silver_failure_report.py가 이미 쓰는 "duckdb.connect(':memory:') + register() +
query_arrow()" 패턴, 그리고 Spark의 "createOrReplaceTempView() + spark.sql()"
패턴을 하나의 인터페이스로 감싼 것뿐이다 - 새 실행 방식을 만들지 않는다.
"""
from __future__ import annotations

from typing import Literal

from common.duckdb_io import query_arrow

Dialect = Literal["spark", "duckdb"]


class SqlEngine:
    def __init__(self, dialect: Dialect, spark=None, duckdb_con=None):
        self.dialect: Dialect = dialect
        self._spark = spark
        self._con = duckdb_con

    @classmethod
    def for_spark(cls, spark) -> "SqlEngine":
        return cls("spark", spark=spark)

    @classmethod
    def for_duckdb(cls, con) -> "SqlEngine":
        return cls("duckdb", duckdb_con=con)

    @property
    def spark(self):
        """anchor_frame()처럼 SQL이 아니라 SparkSession.createDataFrame이 필요한
        극소수 경우를 위한 탈출구. dialect=="spark"일 때만 의미가 있다."""
        return self._spark

    def register(self, name: str, obj) -> None:
        """obj를 SQL에서 FROM {name}으로 참조 가능하게 등록한다."""
        if self.dialect == "spark":
            obj.createOrReplaceTempView(name)
        else:
            self._con.register(name, obj)

    def sql(self, text: str):
        """text를 실행한다. Spark는 DataFrame, DuckDB는 pa.Table을 반환한다."""
        if self.dialect == "spark":
            return self._spark.sql(text)
        return query_arrow(self._con, text)

    def read_table(self, table_ref: str, register_as: str):
        """Iceberg 테이블을 읽어 register_as 이름으로 등록한다.

        Spark는 카탈로그에 이미 붙어 있으므로 spark.table()로 바로 읽고,
        DuckDB는 Spark 세션이 없으므로 pyiceberg로 직접 스캔한다
        (silver_failure_report.py 등 Silver DuckDB 잡과 동일한 방식).
        """
        if self.dialect == "spark":
            df = self._spark.table(table_ref)
        else:
            from common.iceberg_catalog import build_iceberg_catalog

            df = build_iceberg_catalog().load_table(table_ref).scan().to_arrow()
        self.register(register_as, df)
        return df
