"""
Spark + Iceberg 세션 빌더

로컬(LocalStack) / AWS 전환은 config.py의 환경변수로만 처리하고,
이 모듈의 코드는 두 환경에서 동일하다. 서로 독립된 두 축으로 나뉜다:

    ICEBERG_CATALOG_TYPE=hadoop -> Hadoop Catalog (S3/LocalStack 경로 기반, Glue 불필요)
    ICEBERG_CATALOG_TYPE=glue   -> Glue Data Catalog

    APP_ENV=local -> S3A 엔드포인트/자격증명을 LocalStack용으로 설정
    APP_ENV=aws   -> S3A 엔드포인트/자격증명을 실 AWS 기본 체인에 맡김(IAM Role/STS 세션 토큰 포함)

    SPARK_LOCAL_EXECUTION -> 이 프로세스가 자원이 작은 로컬 머신에서 도는지 (기본값은
                             APP_ENV=local과 동일하되 독립적으로 켜고 끌 수 있음).
                             local[N]/드라이버 메모리/셔플 파티션 수를 줄인다.

Glue 권한이 없는 AWS 계정에서 실 S3에 Hadoop Catalog로 붙으면서도 컨테이너는 로컬
머신에서 도는 조합(APP_ENV=aws + SPARK_LOCAL_EXECUTION=true)이 실제로 필요해서
env와 spark_local_execution을 분리했다 - 합쳐져 있던 시절에는 실 S3로 전환하자마자
로컬 메모리 튜닝이 통째로 빠지고 반기 CSV(최대 700MB대) 처리 중 OOM이 재현됐다.

NOTE: spark.jars.packages로 지정한 Iceberg/Hadoop-AWS 패키지는 최초 실행 시
      Maven Central에서 다운로드된다 (인터넷 접속 필요, 이후에는 로컬 캐시 사용).
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

import config

logger = logging.getLogger(__name__)

ICEBERG_SPARK_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
ICEBERG_AWS_BUNDLE_PACKAGE = "org.apache.iceberg:iceberg-aws-bundle:1.5.2"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
# jdbc 카탈로그(JdbcCatalog)가 포인터를 저장하는 Postgres에 접속하려면 JDBC 드라이버가
# 필요하다 - iceberg-spark-runtime에는 JdbcCatalog 구현체만 있고 드라이버는 별도.
POSTGRESQL_JDBC_DRIVER_PACKAGE = "org.postgresql:postgresql:42.7.3"


def _jars_packages_config(
    settings: "config.Settings", extra_packages: list[str] | None
) -> dict[str, str]:
    """spark.jars.packages에 넣을 설정을 계산한다 (없으면 빈 dict).

    EMR Serverless 커스텀 이미지처럼 jar가 이미 $SPARK_HOME/jars에 baked-in된
    환경(settings.spark_jars_already_baked=True)에서는 이 설정 자체를 생략한다 -
    이 설정이 있으면 Spark가 Ivy로 매 실행마다 해당 좌표를 네트워크에서 재해석
    하려 들어서, 인터넷이 없는 EMR Serverless 워커에서 잡이 실패한다.
    """
    if settings.spark_jars_already_baked:
        return {}

    packages = [ICEBERG_SPARK_RUNTIME_PACKAGE, ICEBERG_AWS_BUNDLE_PACKAGE, HADOOP_AWS_PACKAGE]
    if settings.iceberg_catalog_type == "jdbc":
        packages.append(POSTGRESQL_JDBC_DRIVER_PACKAGE)
    if extra_packages:
        packages.extend(extra_packages)
    return {"spark.jars.packages": ",".join(packages)}


def build_spark_session(
    app_name: str,
    extra_packages: list[str] | None = None,
    extra_excludes: list[str] | None = None,
) -> SparkSession:
    settings = config.SETTINGS
    catalog = settings.iceberg_catalog_name

    builder = SparkSession.builder.appName(app_name)
    for key, value in _jars_packages_config(settings, extra_packages).items():
        builder = builder.config(key, value)

    builder = (
        builder
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", settings.iceberg_warehouse_path)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # 백필 시 재실행이 곧 "동일 날짜 파티션 덮어쓰기"가 되도록 dynamic overwrite 사용
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if extra_excludes:
        builder = builder.config("spark.jars.excludes", ",".join(extra_excludes))

    if settings.iceberg_catalog_type == "hadoop":
        # Hadoop Catalog (S3 경로 기반 메타데이터, Glue 불필요) - 로컬 LocalStack뿐 아니라
        # Glue 권한이 없는 AWS 계정(교육용 계정 등)에서 실 S3에 붙일 때도 이 분기를 쓴다.
        # 단점: 파일 규칙(version-hint.text) 기반이라 pyiceberg 등 Spark 밖 클라이언트가
        # 카탈로그를 읽을 표준 방법이 없다 - 그래서 jdbc 분기를 추가했다.
        builder = builder.config(f"spark.sql.catalog.{catalog}.type", "hadoop")
    elif settings.iceberg_catalog_type == "jdbc":
        # JDBC Catalog - "테이블 -> 최신 metadata.json 위치" 포인터를 Postgres DB에
        # 저장한다. 데이터/메타데이터 파일 자체는 그대로 warehouse(S3)에 있고, 이 DB는
        # 포인터만 담는다. pyiceberg의 SqlCatalog가 같은 스키마를 그대로 읽을 수 있어서
        # Spark 없이도(가벼운 메타데이터 조회 목적) 접근 가능해진다.
        # io-impl은 일부러 안 지정한다 - hadoop 분기와 동일하게 기본값(HadoopFileIO)을
        # 써서 아래 env=="local" 블록의 spark.hadoop.fs.s3a.*(LocalStack 엔드포인트/자격증명)
        # 설정을 그대로 재사용한다. glue 분기의 S3FileIO는 여기서 안 맞는다 - S3FileIO는
        # LocalStack 엔드포인트를 Iceberg 전용 s3.endpoint 계열 설정으로 따로 받아야 해서
        # 지금 이미 있는 s3a 설정과 별도로 관리해야 하는 부담이 생긴다.
        builder = (
            builder.config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
            .config(f"spark.sql.catalog.{catalog}.uri", settings.iceberg_jdbc_catalog_uri)
            .config(f"spark.sql.catalog.{catalog}.jdbc.user", settings.iceberg_jdbc_catalog_user)
            .config(f"spark.sql.catalog.{catalog}.jdbc.password", settings.iceberg_jdbc_catalog_password)
        )
    else:
        # AWS 배포: Glue Data Catalog (Hive Metastore 자체 운영 불필요)
        builder = builder.config(f"spark.sql.catalog.{catalog}.type", "glue").config(
            f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )

    if settings.env == "local":
        # LocalStack은 더미 access/secret key(기본값 "test")로 인증한다. 세션 토큰이 없는
        # SimpleAWSCredentialsProvider(access/secret key 명시 시 기본으로 붙는 provider)로 충분하다.
        # AWS 배포에서는 이 config를 아예 안 넣어서 hadoop-aws의 기본 credential provider chain이
        # 환경변수(AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN, ASIA로 시작하는 임시 STS 자격증명 포함)나
        # IAM Role을 그대로 읽게 한다 - 여기서 access/secret만 명시하면 세션 토큰이 빠져
        # ExpiredToken/InvalidAccessKeyId로 실패한다.
        builder = builder.config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key).config(
            "spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key
        )

        # S3_ENDPOINT(LocalStack, 기본 http://localhost:4566)로 리다이렉트 + SSL 비활성화.
        # 실 AWS S3(위 hadoop 분기가 Glue 미보유 계정에서 탄 경우 포함)는 이 설정이 없어야
        # hadoop-aws가 리전에 맞는 실제 S3 https 엔드포인트를 기본값으로 쓴다.
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint).config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
        )

        if settings.iceberg_catalog_type == "jdbc":
            # 위 전역 spark.hadoop.fs.s3a.*는 "type=hadoop/glue" 같은 Spark 내장 카탈로그
            # 통합에는 그대로 먹힌다(SparkSession의 세션 Hadoop Configuration을 그대로
            # 넘겨받아서) - 하지만 JdbcCatalog처럼 catalog-impl로 리플렉션 생성되는 커스텀
            # 카탈로그는 그 세션 Configuration을 자동으로 물려받지 않고, catalog-impl에
            # 전달되는 속성 맵(spark.sql.catalog.<name>.* 중 hadoop.*로 시작하는 것만
            # Iceberg가 별도로 뽑아 Configuration에 얹어줌)만 받는다. 실측으로 확인(2026-08-22):
            # 이 설정 없이 register_table을 호출하면 "No AWS Credentials provided"로 실패한다.
            #
            # settings.s3_access_key/secret_key가 빈 문자열이면(.env의 AWS_ACCESS_KEY_ID=,
            # AWS_SECRET_ACCESS_KEY= - 실측 확인) "test"로 대체한다. 전역 spark.hadoop.fs.s3a.*
            # 경로(hadoop/glue 카탈로그가 쓰는 경로)는 빈 문자열도 LocalStack이 그냥 받아주지만,
            # 이 catalog-impl 전용 경로는 SimpleAWSCredentialsProvider가 빈 문자열을 "값 없음"과
            # 동일하게 취급해 통째로 스킵해버려서 빈 문자열로는 아예 안 먹힌다(실측 확인,
            # 2026-08-22) - LocalStack은 어차피 자격증명 값 자체를 검증하지 않으므로 실제
            # 값이 뭐든 무방하다.
            jdbc_access_key = settings.s3_access_key or "test"
            jdbc_secret_key = settings.s3_secret_key or "test"
            builder = (
                builder.config(f"spark.sql.catalog.{catalog}.hadoop.fs.s3a.access.key", jdbc_access_key)
                .config(f"spark.sql.catalog.{catalog}.hadoop.fs.s3a.secret.key", jdbc_secret_key)
                .config(f"spark.sql.catalog.{catalog}.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
                .config(f"spark.sql.catalog.{catalog}.hadoop.fs.s3a.connection.ssl.enabled", "false")
            )

    if settings.spark_local_execution:
        # 이 블록은 "S3가 LocalStack이냐 실 AWS냐"와 무관하게, 지금 이 프로세스가 자원이
        # 작은 로컬 머신(개발 노트북, 로컬 docker-compose 등)에서 도는지만 본다.
        # Glue 권한이 없어 실 S3 + Hadoop Catalog(APP_ENV=aws)를 쓰면서도 컨테이너는 로컬
        # 머신에서 그대로 도는 조합이 실제로 있어서 env(local/aws)와 분리했다 - 이 블록이
        # env=="local"에 묶여 있던 시절에는, 실 S3로 전환하자마자 이 튜닝이 통째로 빠지고
        # 반기 CSV(최대 700MB대) 처리 중 OOM이 재현됐다.

        # macOS 등 로컬 환경에서 호스트명이 바인딩 불가능한 주소로 풀리면
        # "BindException: Can't assign requested address"가 발생한다.
        # 로컬 드라이버는 127.0.0.1로 명시 바인딩 (EMR 등 실제 클러스터 실행에는 영향 없음).
        bind_address = os.getenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")
        builder = builder.config("spark.driver.bindAddress", bind_address).config(
            "spark.driver.host", bind_address
        )

        # LocalStack(특히 PERSISTENCE=1 파일 기반 백엔드)은 동시 PutObject 요청이 많아지면
        # "read of closed file" 레이스 컨디션으로 500을 뱉는 게 실측으로 확인됐다(반기 CSV 1개에
        # 파티션 180개+ -> overwritePartitions가 한꺼번에 병렬 업로드 시도). 실 S3에서는 이
        # 레이스는 없지만, 로컬 머신의 대역폭/CPU도 한정적이라 동시 연결 수를 낮게 유지하는 게
        # 여전히 안전하다. 로컬 실행은 master를 명시적으로 낮은 병렬도로 고정해 동시 요청 수를
        # 줄인다. (AWS/EMR 클러스터 실행은 spark-submit --master가 따로 지정되므로 이 분기는
        # 영향 없음 - spark_local_execution을 false로 둔다)
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
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.hadoop.fs.s3a.connection.maximum", "5")
            .config("spark.hadoop.fs.s3a.threads.max", "5")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession 생성 완료 (env=%s, catalog_type=%s, spark_local_execution=%s)",
        settings.env,
        settings.iceberg_catalog_type,
        settings.spark_local_execution,
    )
    return spark


def build_spark_session_with_deequ(app_name: str) -> SparkSession:
    """
    PyDeequ(데이터 품질 검증) 사용하는 잡 전용 세션 빌더.

    PyDeequ는 SPARK_VERSION 환경변수를 보고 맞는 Deequ jar 좌표(Scala)를 고른다 -
    반드시 `import pydeequ`보다 먼저 이 환경변수를 설정해야 한다.
    NOTE: pydeequ가 현재 Spark 버전(3.5.1)의 Deequ 빌드를 지원하는지는
          requirements.txt의 pydeequ 버전에 따라 달라질 수 있으니, 실행 시
          "Deequ jar를 찾을 수 없음" 류 에러가 나면 pydeequ 버전을 먼저 확인할 것.
    """
    os.environ.setdefault("SPARK_VERSION", "3.5")
    import pydeequ

    return build_spark_session(
        app_name,
        extra_packages=[pydeequ.deequ_maven_coord],
        extra_excludes=[pydeequ.f2j_maven_coord],
    )


def stop_spark_session_with_deequ(spark: SparkSession) -> None:
    """
    PyDeequ가 VerificationSuite 실행 시 열어두는 py4j 콜백 서버(non-daemon 스레드)를
    먼저 내려야 한다. 이걸 안 하면 spark.stop()이나 sys.exit()을 호출해도 그 스레드가
    프로세스를 계속 붙잡고 있어 잡이 "성공/실패 판정과 무관하게 영원히 안 끝나는" 상태가
    된다 (실측: transform_silver_rental_history가 sys.exit(1) 이후에도 13분+ 살아있었음).
    """
    try:
        spark.sparkContext._gateway.shutdown_callback_server()
    except Exception:
        logger.warning("PyDeequ 콜백 서버 종료 중 오류 (무시하고 계속)", exc_info=True)
    spark.stop()