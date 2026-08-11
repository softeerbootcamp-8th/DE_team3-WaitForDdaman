"""
Spark + Iceberg 세션 빌더

로컬(LocalStack) / AWS 전환은 config.py의 환경변수로만 처리하고,
이 모듈의 코드는 두 환경에서 동일하다.

    ICEBERG_CATALOG_TYPE=hadoop -> 로컬 개발 (LocalStack S3 기반 Hadoop Catalog)
    ICEBERG_CATALOG_TYPE=glue   -> AWS 배포 (Glue Data Catalog)

NOTE: spark.jars.packages로 지정한 Iceberg/Hadoop-AWS 패키지는 최초 실행 시
      Maven Central에서 다운로드된다 (인터넷 접속 필요, 이후에는 로컬 캐시 사용).
"""
import logging
import os

from pyspark.sql import SparkSession

from common import config

logger = logging.getLogger(__name__)

ICEBERG_SPARK_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
ICEBERG_AWS_BUNDLE_PACKAGE = "org.apache.iceberg:iceberg-aws-bundle:1.5.2"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def build_spark_session(app_name: str) -> SparkSession:
    settings = config.SETTINGS
    catalog = settings.iceberg_catalog_name

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([ICEBERG_SPARK_RUNTIME_PACKAGE, ICEBERG_AWS_BUNDLE_PACKAGE, HADOOP_AWS_PACKAGE]),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", settings.iceberg_warehouse_path)
        # ---- S3A 커넥터 (LocalStack / AWS 공통) ----
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # 백필 시 재실행이 곧 "동일 날짜 파티션 덮어쓰기"가 되도록 dynamic overwrite 사용
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if settings.iceberg_catalog_type == "hadoop":
        # 로컬 개발: Hadoop Catalog (LocalStack S3 경로 기반, Glue 불필요)
        builder = (
            builder.config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
    else:
        # AWS 배포: Glue Data Catalog (Hive Metastore 자체 운영 불필요)
        builder = builder.config(f"spark.sql.catalog.{catalog}.type", "glue").config(
            f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )

    if settings.env == "local":
        # macOS 등 로컬 환경에서 호스트명이 바인딩 불가능한 주소로 풀리면
        # "BindException: Can't assign requested address"가 발생한다.
        # 로컬 드라이버는 127.0.0.1로 명시 바인딩 (EMR 등 실제 클러스터 실행에는 영향 없음 - local 분기 한정).
        bind_address = os.getenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")
        builder = builder.config("spark.driver.bindAddress", bind_address).config(
            "spark.driver.host", bind_address
        )

        # LocalStack(특히 PERSISTENCE=1 파일 기반 백엔드)은 동시 PutObject 요청이
        # 많아지면 "read of closed file" 레이스 컨디션으로 500을 뱉는 게 실측으로 확인됐다
        # (반기 CSV 1개에 파티션 180개+ -> overwritePartitions가 한꺼번에 병렬 업로드 시도).
        # 로컬 실행은 master를 명시적으로 낮은 병렬도로 고정해 동시 요청 수를 줄인다.
        # (AWS/EMR 클러스터 실행은 spark-submit --master가 따로 지정되므로 이 분기는 영향 없음)
        local_master = os.getenv("SPARK_LOCAL_MASTER", "local[2]")
        # 로컬 모드는 driver == executor가 같은 JVM이라, 기본 힙(1g)으로는 반기 CSV
        # (최대 700MB대) 읽기 + repartition 셔플 + cache를 감당 못 해 OutOfMemoryError가
        # 실측으로 발생했다. 드라이버 메모리를 넉넉히 올리고, 셔플 파티션 수도
        # 기본값(200)이 이 볼륨엔 과한 오버헤드라 로컬 한정으로 줄인다.
        driver_memory = os.getenv("SPARK_LOCAL_DRIVER_MEMORY", "6g")
        shuffle_partitions = os.getenv("SPARK_LOCAL_SHUFFLE_PARTITIONS", "8")
        builder = (
            builder.master(local_master)
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.shuffle.partitions", shuffle_partitions)
            .config("spark.hadoop.fs.s3a.connection.maximum", "5")
            .config("spark.hadoop.fs.s3a.threads.max", "5")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession 생성 완료 (catalog_type=%s)", settings.iceberg_catalog_type)
    return spark