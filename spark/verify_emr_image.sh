#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:-emr-spark-prod:test}"

echo "== jar baked-in 확인 =="
# Iceberg / Hadoop-AWS는 베이스 이미지가 extraClassPath로 이미 제공하므로
# 굽지 않는다(Dockerfile.prod 주석 참고). 우리가 굽는 jar는 Postgres JDBC 뿐이다.
docker run --rm --entrypoint /bin/bash "$IMAGE" -c '
  set -e
  for jar in \
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

# 로딩 가능 여부뿐 아니라 '어느 jar가 그 클래스를 공급했는지'까지 출력한다.
# Iceberg/Hadoop-AWS는 베이스 이미지 자체 빌드(extraClassPath)에서 오는 게
# 정상이라 경로를 단정하지 않고 정보성으로만 찍는다 - EMR 베이스 이미지 내부
# 레이아웃이라 이 저장소가 통제하는 값이 아니다. 반면 Postgres JDBC는 우리가
# 유일하게 굽는 jar이므로 실제 공급 경로가 postgresql-42.7.3.jar인지 단정한다.
for cls in [
    'org.apache.iceberg.spark.SparkCatalog',
    'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'org.postgresql.Driver',
]:
    k = jvm.java.lang.Class.forName(cls)
    src = k.getProtectionDomain().getCodeSource()
    loc = str(src.getLocation().toString()) if src is not None else '<unknown>'
    print('CLASS_OK:', cls, '<-', loc)
    if cls == 'org.postgresql.Driver' and 'postgresql-42.7.3.jar' not in loc:
        raise SystemExit(
            'FAIL: org.postgresql.Driver 가 baked-in postgresql-42.7.3.jar 이 아닌 '
            + loc + ' 에서 로딩되었습니다.'
        )
spark.stop()
print('SPARK_SESSION_OK')
"

echo "== RISK_MODEL_CONFIG 기본값으로 load_config() 동작 확인 =="
docker run --rm --entrypoint /usr/bin/python3 \
  -e PYTHONPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/pyspark.zip:/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/app:/opt/app/ingestion:/opt/app/pipeline/risk_model" \
  "$IMAGE" -c "
import os
from pipeline.train_risk_model.settings import load_config

# 잡(build_bike_features_daily / build_fact_bike_risk)은 인자 없이 load_config()를
# 부른다 - 컨테이너에 RISK_MODEL_CONFIG가 없으면 Airflow 전용 기본 경로를 보고
# FileNotFoundError로 죽는다. 그 경로를 그대로 재현한다.
print('RISK_MODEL_CONFIG =', os.environ.get('RISK_MODEL_CONFIG'))
cfg = load_config()
assert cfg, 'risk_model.yaml 이 비어 있습니다'
print('LOAD_CONFIG_OK')
"

echo "== entryPoint 직접 파일 실행 확인 (local:///opt/app/... 규약) =="
# EMR Serverless는 entryPoint를 파일로 직접 실행한다(python -m 아님).
# 상대 import가 남아 있으면 여기서만 ImportError로 잡힌다.
# --anchor-plan 누락으로 argparse가 SystemExit(2)를 내는 건 정상 - import 단계를
# 통과했다는 뜻이므로 stderr에 ImportError가 없는지로 판정한다.
for entry in \
  /opt/app/pipeline/train_risk_model/samples.py \
  /opt/app/pipeline/risk_model/jobs/build_bike_features_daily.py \
  /opt/app/pipeline/risk_model/jobs/build_fact_bike_risk.py \
  /opt/app/pipeline/risk_model/jobs/build_fact_bike_decision.py
do
  out=$(docker run --rm --entrypoint /usr/bin/python3 \
    -e PYTHONPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/pyspark.zip:/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/app:/opt/app/ingestion:/opt/app/pipeline/risk_model" \
    "$IMAGE" "$entry" --help 2>&1) || true
  if echo "$out" | grep -q "ImportError\|ModuleNotFoundError"; then
    echo "FAIL: $entry 직접 실행 시 import 실패"
    echo "$out"
    exit 1
  fi
  echo "ENTRYPOINT_OK: $entry"
done

echo "모든 검증 통과"
