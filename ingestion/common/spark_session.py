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
import logging
import os

from pyspark.sql import SparkSession

import config

logger = logging.getLogger(__name__)

ICEBERG_SPARK_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
ICEBERG_AWS_BUNDLE_PACKAGE = "org.apache.iceberg:iceberg-aws-bundle:1.5.2"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"

_MB = 1024 * 1024


def _s3a_pool_size(env: str) -> str:
    """
    S3A 커넥션/스레드 풀 크기.

    LocalStack(PERSISTENCE=1 파일 백엔드)은 동시 PutObject가 많아지면 "read of closed
    file" 레이스로 500을 뱉는 게 실측으로 확인돼서 낮게 묶는다. 실 S3에는 이 레이스가
    없어서 낮은 값은 순수하게 처리량 손실이다 - 그래서 spark_local_execution(이 프로세스가
    작은 머신에서 도는지)이 아니라 env(S3가 LocalStack이냐 실 S3냐)로 분기한다.

    hadoop-aws 기본값(96)을 그대로 쓰지 않는 이유: t4g.large가 2 vCPU라 그만큼의 커넥션
    풀은 소켓/메모리만 잡고 이득이 없다.
    """
    return os.getenv("SPARK_S3A_POOL_SIZE", "5" if env == "local" else "32")


def _spark_tuning_config() -> dict[str, str]:
    """
    작은 머신에서 도는 실행(spark_local_execution)에 적용하는 튜닝값.

    2026-08-21 EC2(t4g.large, 8GB)에서 silver_failure_report가 write 단계에서
    OutOfMemoryError로 죽었다. 원인은 힙 크기가 아니라 write 병렬도였다 - 읽기는
    934 태스크였는데 write 스테이지는 3 태스크까지 줄어들어(AQE 셔플 병합), 태스크
    하나가 전체의 1/3 + 수백 개 날짜 파티션(days(reg_dttm))의 Parquet writer를 동시에
    열었다. 필요한 힙 = (동시에 열린 writer 수) x (writer당 버퍼)이므로 양쪽을 다 줄인다.

        shuffle.partitions            8 -> 64   동시에 열리는 writer 수를 줄인다
        advisoryPartitionSizeInBytes 64MB -> 8MB  늘린 파티션이 다시 병합되지 않게
        parquet.block.size          128MB -> 32MB writer 하나가 잡는 버퍼를 줄인다

    AQE 병합을 아예 끄지 않고 기준만 낮추는 이유: 데이터 양에 따라 태스크 수가 따라
    움직이게 남겨둔다. 일 배치처럼 작은 write는 그대로 1~2 태스크로 병합되고, 전량
    재처리처럼 큰 write만 쪼개진다. 끄면 볼륨과 무관하게 항상 64로 고정된다.

    NOTE: 파티션 테이블 + write.distribution-mode=hash 조합에서는 같은 파티션 값이 한
    태스크로 모이므로, 태스크 수를 늘려도 출력 파일 수는 늘지 않는다(파일 수 ~= 파티션
    수). 파일 수가 태스크 수에 비례하는 건 파티션이 없는 테이블(quarantine, dq_results
    같은 append 대상)이므로, 그쪽에서 Small Files가 문제되면 이 값이 아니라 해당 잡에서
    coalesce를 검토할 것.
    """
    advisory_mb = int(os.getenv("SPARK_ADVISORY_PARTITION_SIZE_MB", "8"))
    parquet_block_mb = int(os.getenv("SPARK_PARQUET_BLOCK_SIZE_MB", "32"))
    return {
        "spark.driver.memory": os.getenv("SPARK_LOCAL_DRIVER_MEMORY", "6g"),
        "spark.sql.shuffle.partitions": os.getenv("SPARK_LOCAL_SHUFFLE_PARTITIONS", "64"),
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(advisory_mb * _MB),
        "spark.hadoop.parquet.block.size": str(parquet_block_mb * _MB),
    }


def build_spark_session(
    app_name: str,
    extra_packages: list[str] | None = None,
    extra_excludes: list[str] | None = None,
) -> SparkSession:
    settings = config.SETTINGS
    catalog = settings.iceberg_catalog_name

    packages = [ICEBERG_SPARK_RUNTIME_PACKAGE, ICEBERG_AWS_BUNDLE_PACKAGE, HADOOP_AWS_PACKAGE]
    if extra_packages:
        packages.extend(extra_packages)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
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

    # S3A 커넥션/스레드 풀은 "S3가 LocalStack이냐 실 S3냐"에만 달렸다(_s3a_pool_size 참고).
    # spark_local_execution 블록에 들어 있던 시절에는 실 S3로 전환해도 LocalStack 레이스
    # 방어용 값(5)이 그대로 따라와 처리량만 깎였다.
    s3a_pool_size = _s3a_pool_size(settings.env)
    builder = builder.config("spark.hadoop.fs.s3a.connection.maximum", s3a_pool_size).config(
        "spark.hadoop.fs.s3a.threads.max", s3a_pool_size
    )

    if settings.iceberg_catalog_type == "hadoop":
        # Hadoop Catalog (S3 경로 기반 메타데이터, Glue 불필요) - 로컬 LocalStack뿐 아니라
        # Glue 권한이 없는 AWS 계정(교육용 계정 등)에서 실 S3에 붙일 때도 이 분기를 쓴다.
        builder = builder.config(f"spark.sql.catalog.{catalog}.type", "hadoop")
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

        # 로컬 실행은 master를 명시적으로 낮은 병렬도로 고정한다. (AWS/EMR 클러스터 실행은
        # spark-submit --master가 따로 지정되므로 이 분기는 영향 없음 -
        # spark_local_execution을 false로 둔다)
        local_master = os.getenv("SPARK_LOCAL_MASTER", "local[2]")
        # 로컬 모드는 driver == executor가 같은 JVM이라, 기본 힙(1g)으로는 반기 CSV
        # (최대 700MB대) 읽기 + repartition 셔플 + cache를 감당 못 해 OutOfMemoryError가
        # 실측으로 발생했다. 나머지 값의 근거는 _spark_tuning_config 참고.
        builder = builder.master(local_master)
        for key, value in _spark_tuning_config().items():
            builder = builder.config(key, value)

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