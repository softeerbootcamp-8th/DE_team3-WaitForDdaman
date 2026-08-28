"""
pyiceberg 카탈로그 빌더 - Spark 없이 Iceberg 테이블 메타데이터만 가볍게 조회할 때 쓴다.

지금은 jdbc 카탈로그(config.SETTINGS.iceberg_catalog_type == "jdbc")만 지원한다 - hadoop
catalog는 파일 규칙(version-hint.text) 기반이라 pyiceberg가 읽을 표준 방법이 없다
(spark_session.py 참고). hadoop/glue로 돌아가는 환경에서 이 함수를 부르면 잡의 목적
자체가 성립하지 않으므로 명확한 에러를 낸다.

MIN/MAX처럼 파티션 값 범위만 필요한 조회는 table.inspect.partitions()로 매니페스트
(메타데이터)만 읽으면 되고 실제 데이터 파일을 스캔하지 않는다 - Spark 세션을 새로
기동하는 것보다 훨씬 가볍다 (jobs/check_watermark_date.py, jobs/bootstrap_silver_watermark.py
에서 이 이유로 도입, 2026-08-22).
"""
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

import config


def build_iceberg_catalog() -> Catalog:
    settings = config.SETTINGS
    if settings.iceberg_catalog_type != "jdbc":
        raise RuntimeError(
            f"build_iceberg_catalog()은 jdbc 카탈로그 전용이다 (현재 ICEBERG_CATALOG_TYPE="
            f"{settings.iceberg_catalog_type}). hadoop/glue는 pyiceberg가 읽을 표준 방법이 없다."
        )

    # settings.iceberg_jdbc_catalog_uri는 Spark JdbcCatalog 형식("jdbc:postgresql://host:port/db").
    # pyiceberg의 SqlCatalog는 SQLAlchemy URI("postgresql+psycopg2://user:pass@host:port/db")를
    # 받는다 - 이 변환을 여기 한 곳에만 두고 흩어지지 않게 한다.
    host_and_path = settings.iceberg_jdbc_catalog_uri.removeprefix("jdbc:postgresql://")
    sqlalchemy_uri = (
        f"postgresql+psycopg2://{settings.iceberg_jdbc_catalog_user}:"
        f"{settings.iceberg_jdbc_catalog_password}@{host_and_path}"
    )

    properties = {"uri": sqlalchemy_uri, "warehouse": settings.iceberg_warehouse_path}
    if settings.env == "local":
        # spark_session.py의 env=="local" 분기와 동일한 이유 - LocalStack은 더미
        # access/secret key로 인증하고, 실 AWS에서는 이 속성들을 아예 안 넘겨서
        # pyiceberg의 기본 자격증명 체인(IAM Role 등)이 그대로 동작하게 한다.
        properties.update(
            {
                "s3.endpoint": settings.s3_endpoint,
                "s3.access-key-id": settings.s3_access_key or "test",
                "s3.secret-access-key": settings.s3_secret_key or "test",
                "s3.path-style-access": "true",
            }
        )

    return SqlCatalog(settings.iceberg_catalog_name, **properties)
