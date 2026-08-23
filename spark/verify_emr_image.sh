#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:-emr-spark-prod:test}"

# EMR Serverless 워커가 실제로 잡을 띄울 때와 같은 PYTHONPATH.
# 베이스 이미지의 pyspark 경로 + Dockerfile.prod의 ENV PYTHONPATH 2분할.
PYPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/pyspark.zip:/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/app:/opt/app/ingestion"

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
# EMR Serverless는 entryPoint를 파일로 직접 실행한다(python -m 아님).
# 그래서 여기서도 local:///opt/app/... 규약대로 파일 경로를 직접 실행해
# import 단계가 통과하는지 본다. 대상 파일마다 안전하게 실행할 수 있는
# 지점이 달라 검증 방식이 3가지로 갈린다.

# (1) initial_load_*.py - `if __name__ == "__main__":`에서 INPUT_FILE이 없으면
#     Spark 세션을 만들기도 전에 "사용법:"을 찍고 exit 1로 깨끗하게 끝난다.
#     즉 exit 1 + "사용법:" == 모든 import가 해결됐다는 뜻이다.
for entry in \
  /opt/app/ingestion/jobs/initial_load_rental_history.py \
  /opt/app/ingestion/jobs/initial_load_failure_report.py
do
  set +e
  out=$(docker run --rm --entrypoint /usr/bin/python3 \
    -e PYTHONPATH="$PYPATH" \
    "$IMAGE" "$entry" 2>&1)
  rc=$?
  set -e
  if [ "$rc" -ne 1 ] || ! echo "$out" | grep -q "사용법:"; then
    echo "FAIL: $entry (exit=$rc, '사용법:' 안내 없음 - import 단계에서 깨진 것으로 보임)"
    echo "$out"
    exit 1
  fi
  echo "USAGE_GUARD_OK: $entry"
done

# (2) samples.py / check_silver_catalog.py - argparse + main() 가드가 있어
#     --help면 Spark 세션 없이 usage만 찍고 exit 0.
for entry in \
  /opt/app/pipeline/train_risk_model/samples.py \
  /opt/app/airflow/scripts/check_silver_catalog.py
do
  set +e
  out=$(docker run --rm --entrypoint /usr/bin/python3 \
    -e PYTHONPATH="$PYPATH" \
    "$IMAGE" "$entry" --help 2>&1)
  rc=$?
  set -e
  if [ "$rc" -ne 0 ] || ! echo "$out" | grep -q "usage:"; then
    echo "FAIL: $entry --help (exit=$rc, usage 출력 없음)"
    echo "$out"
    exit 1
  fi
  echo "HELP_OK: $entry"
done

# (3) check_gold_dim_fact.py / check_silver_gold.py - 가드 없이 모듈 최상단에서
#     즉시 build_spark_session() + 실제 카탈로그 테이블 read를 한다. 실행하면
#     실제 S3/RDS 접속을 시도해 멈추거나 애매하게 실패하므로 실행하지 않고
#     이미지 안에 파일이 있는지 + 문법이 유효한지만 확인한다. (수동 검증
#     스크립트라 실제 실행 검증은 실 환경에서 한다.)
for entry in \
  /opt/app/airflow/scripts/check_gold_dim_fact.py \
  /opt/app/airflow/scripts/check_silver_gold.py
do
  docker run --rm --entrypoint /usr/bin/python3 "$IMAGE" \
    -c "import ast; ast.parse(open('$entry').read()); print('SYNTAX_OK: $entry')"
done

echo "== SparkSession + jar 클래스 로딩 확인 (네트워크 불필요) =="
docker run --rm \
  --entrypoint /usr/bin/python3 \
  -e PYTHONPATH="$PYPATH" \
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
  -e PYTHONPATH="$PYPATH" \
  "$IMAGE" -c "
import os
from pipeline.train_risk_model.settings import load_config

# samples.py 계열은 load_config()를 인자 없이 부를 수 있다 - 컨테이너에
# RISK_MODEL_CONFIG가 없으면 Airflow 전용 기본 경로를 보고 FileNotFoundError로
# 죽는다. 그 경로를 그대로 재현한다.
print('RISK_MODEL_CONFIG =', os.environ.get('RISK_MODEL_CONFIG'))
cfg = load_config()
assert cfg, 'risk_model.yaml 이 비어 있습니다'
print('LOAD_CONFIG_OK')
"

echo "모든 검증 통과"
