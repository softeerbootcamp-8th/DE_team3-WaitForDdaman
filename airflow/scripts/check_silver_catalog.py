"""bike_catalog 연결과 silver/gold 테이블 상태를 확인하는 진단 스크립트.

학습 파이프라인을 실행하기 전에 먼저 이걸로:
  1) 팀 표준 Spark 세션(build_spark_session)이 로컬에서 카탈로그에 붙는지
  2) 지정한 네임스페이스의 테이블들이 존재하는지
  3) 존재한다면 스키마와 행수가 예상과 맞는지
를 확인한다. 데이터가 0행이어도 실행 자체는 성공해야 한다 — 이 스크립트가
실패하면 카탈로그/버전 설정 문제고, "테이블 없음/0행" 로그가 찍히면 ETL 쪽 이슈다.

  python scripts/check_silver_catalog.py                    # silver 5종 확인
  python scripts/check_silver_catalog.py --namespace gold    # gold 확인
  python scripts/check_silver_catalog.py --namespace silver --table rental_history
"""

from __future__ import annotations

import argparse

import config
from ingestion.common.spark_session import build_spark_session
from pyspark.sql import SparkSession

SILVER_TABLES = ["rental_history", "failure_report", "station_master", "station_active", "bike_man_action"]
GOLD_TABLES = ["dim_bike", "dim_station", "fact_bike_risk_store", "mart_bike_risk_current", "fact_bike_urgent_store"]


def check_table(spark: SparkSession, catalog: str, namespace: str, name: str) -> None:
    ref = f"{catalog}.{namespace}.{name}"
    print(f"\n── {ref} ──")
    try:
        exists = spark.catalog.tableExists(ref)
    except Exception as e:  # 카탈로그 자체 연결 문제와 테이블 부재를 구분
        print(f"  [오류] 카탈로그 조회 실패: {type(e).__name__}: {e}")
        return

    if not exists:
        print("  존재하지 않음 — 테이블 미생성 (ETL backfill 미완료 가능성)")
        return

    df = spark.table(ref)
    print("  스키마:")
    for f in df.schema.fields:
        print(f"    {f.name:<24} {f.dataType.simpleString()}")

    try:
        n = df.count()
    except Exception as e:
        print(f"  [오류] count() 실패 (메타데이터는 있으나 데이터 파일 접근 불가): {e}")
        return

    print(f"  행수: {n:,}")
    if n == 0:
        print("  [경고] 테이블은 존재하나 데이터 파일이 없음 — INSERT 단계 확인 필요")
        return

    df.show(3, truncate=False)
    date_cols = [c for c in df.columns if "date" in c.lower() or c.lower().endswith("_dt") or c.lower().endswith("dttm")]
    if date_cols:
        c = date_cols[0]
        spark.sql(f"SELECT min({c}) AS min_{c}, max({c}) AS max_{c} FROM {ref}").show(truncate=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default="silver", help="silver | gold | bronze")
    ap.add_argument("--table", default=None, help="특정 테이블만 확인 (기본: 네임스페이스 기본 목록)")
    args = ap.parse_args()

    catalog = config.SETTINGS.iceberg_catalog_name
    print(
        f"catalog={catalog}  type={config.SETTINGS.iceberg_catalog_type}  "
        f"warehouse={config.SETTINGS.iceberg_warehouse_path}"
    )

    spark = build_spark_session("risk-model-catalog-check")
    try:
        try:
            namespaces = [r.namespace for r in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()]
            print(f"네임스페이스: {namespaces}")
        except Exception as e:
            print(f"[오류] 카탈로그 연결 실패: {type(e).__name__}: {e}")
            return

        if args.table:
            targets = [args.table]
        elif args.namespace == "gold":
            targets = GOLD_TABLES
        else:
            targets = SILVER_TABLES

        for t in targets:
            check_table(spark, catalog, args.namespace, t)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
