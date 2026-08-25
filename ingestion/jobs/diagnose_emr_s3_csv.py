"""EMR Serverless에서 S3 CSV 경로를 진단하는 일회성 읽기 전용 잡.

같은 EMR Application/실행 Role로 실행되던 초기 적재의 S3 CSV 읽기 직전 상태를
재현한다. S3나 Iceberg에 쓰지 않고, Hadoop glob 결과·boto3 목록 조회·실제
``spark.read.csv`` 결과만 출력한다.

실행 예:
    python diagnose_emr_s3_csv.py \
        s3a://bucket/raw/rental_history/_utf8_staging/example.csv

EMR Serverless에서는 커스텀 이미지에 포함된 경로를 entryPoint로 지정한다.
"""
from __future__ import annotations

import argparse
from urllib.parse import urlparse

import boto3

from common.spark_session import build_spark_session


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri.replace("s3a://", "s3://", 1))
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"S3 URI가 아닙니다: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def diagnose_csv_path(spark, csv_source: str) -> None:
    print(f"=== DIAGNOSTIC START: {csv_source} ===", flush=True)

    # [1] Spark/Hadoop가 실제로 보는 glob 결과
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        jpath = spark._jvm.org.apache.hadoop.fs.Path
        path_obj = jpath(csv_source)
        filesystem = path_obj.getFileSystem(hadoop_conf)
        matched = filesystem.globStatus(path_obj)
        matched = list(matched) if matched else []
        print(f"[1] fs.globStatus() matched: {len(matched)}", flush=True)
        for status in matched:
            print(f"    -> {status.getPath()} ({status.getLen()} bytes)", flush=True)
    except Exception as exc:  # 진단 결과를 남기고 다음 단계도 시도한다.
        print(f"[1] globStatus EXCEPTION: {type(exc).__name__}: {exc}", flush=True)

    # [2] 같은 EMR 실행 Role로 boto3 ListObjectsV2
    try:
        bucket, key = _parse_s3_uri(csv_source)
        prefix = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
        response = boto3.client("s3").list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=20,
        )
        keys = [item["Key"] for item in response.get("Contents", [])]
        print(f"[2] ListObjectsV2 s3://{bucket}/{prefix} -> {len(keys)}개", flush=True)
        for listed_key in keys:
            print(f"    -> {listed_key}", flush=True)
    except Exception as exc:
        print(f"[2] ListObjectsV2 EXCEPTION: {type(exc).__name__}: {exc}", flush=True)

    # [3] 원래 실패했던 호출을 그대로 실행한다.
    try:
        dataframe = spark.read.option("header", "true").csv(csv_source)
        sample = dataframe.take(1)
        print(
            f"[3] spark.read.csv() SUCCESS: columns={dataframe.columns}, sample_rows={len(sample)}",
            flush=True,
        )
    except Exception as exc:
        print(f"[3] spark.read.csv() EXCEPTION: {type(exc).__name__}: {exc}", flush=True)

    print("=== DIAGNOSTIC END ===", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="EMR Serverless S3 CSV 읽기 진단")
    parser.add_argument("csv_uri", help="진단할 s3a://bucket/key CSV 경로")
    args = parser.parse_args()

    spark = build_spark_session("diagnose-emr-s3-csv")
    try:
        diagnose_csv_path(spark, args.csv_uri)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
