#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:-emr-spark-prod:test}"

echo "== jar baked-in 확인 =="
docker run --rm --entrypoint /bin/bash "$IMAGE" -c '
  set -e
  for jar in \
    iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    iceberg-aws-bundle-1.5.2.jar \
    hadoop-aws-3.3.4.jar \
    postgresql-42.7.3.jar
  do
    test -f "$SPARK_HOME/jars/$jar" || { echo "MISSING: $jar"; exit 1; }
    echo "OK: $jar"
  done
'

echo "== 잡 import 스모크 테스트 =="
docker run --rm --entrypoint /usr/bin/python3 \
  -e PYTHONPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/pyspark.zip:/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/app:/opt/app/ingestion:/opt/app/pipeline/risk_model" \
  "$IMAGE" -c "
import jobs.build_bike_features_daily
import jobs.build_fact_bike_risk
import jobs.build_fact_bike_decision
import pipeline.train_risk_model.samples
print('IMPORT_OK')
"

echo "== SparkSession + jar 클래스 로딩 확인 (네트워크 불필요) =="
docker run --rm \
  --entrypoint /usr/bin/python3 \
  -e PYTHONPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/pyspark.zip:/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/app:/opt/app/ingestion:/opt/app/pipeline/risk_model" \
  -e ICEBERG_CATALOG_TYPE=hadoop \
  -e ICEBERG_WAREHOUSE_PATH=/tmp/iceberg_warehouse \
  "$IMAGE" -c "
from common.spark_session import build_spark_session

spark = build_spark_session('emr-image-smoke-test')
jvm = spark.sparkContext._jvm
for cls in [
    'org.apache.iceberg.spark.SparkCatalog',
    'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'org.postgresql.Driver',
]:
    jvm.java.lang.Class.forName(cls)
    print('CLASS_OK:', cls)
spark.stop()
print('SPARK_SESSION_OK')
"

echo "모든 검증 통과"
