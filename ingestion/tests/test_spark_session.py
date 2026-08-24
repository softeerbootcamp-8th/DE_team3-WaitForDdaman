"""
spark_session.py의 spark.jars.packages 조건부 설정 단위 테스트 (Issue #183)

EMR Serverless 워커는 인터넷이 없어 spark.jars.packages(Ivy 런타임 다운로드)를
쓸 수 없다 - 이미지에 jar를 baked-in한 뒤엔 이 설정 자체를 생략해야 한다.
실제 SparkSession은 JVM이 뜬 뒤 설정을 바꿀 수 없어 두 분기를 한 프로세스에서
비교 테스트할 수 없으므로, 설정 딕셔너리만 만드는 순수 함수를 분리해 테스트한다.
"""
import config as config_module
from common.spark_session import (
    HADOOP_AWS_PACKAGE,
    ICEBERG_AWS_BUNDLE_PACKAGE,
    ICEBERG_SPARK_RUNTIME_PACKAGE,
    POSTGRESQL_JDBC_DRIVER_PACKAGE,
    _jars_packages_config,
)


def test_jars_packages_config_default_includes_core_packages():
    settings = config_module.Settings(spark_jars_already_baked=False, iceberg_catalog_type="hadoop")
    result = _jars_packages_config(settings, extra_packages=None)
    assert result == {
        "spark.jars.packages": ",".join(
            [ICEBERG_SPARK_RUNTIME_PACKAGE, ICEBERG_AWS_BUNDLE_PACKAGE, HADOOP_AWS_PACKAGE]
        )
    }


def test_jars_packages_config_adds_postgres_driver_for_jdbc_catalog():
    settings = config_module.Settings(spark_jars_already_baked=False, iceberg_catalog_type="jdbc")
    result = _jars_packages_config(settings, extra_packages=None)
    assert result["spark.jars.packages"].endswith(POSTGRESQL_JDBC_DRIVER_PACKAGE)


def test_jars_packages_config_includes_extra_packages():
    settings = config_module.Settings(spark_jars_already_baked=False, iceberg_catalog_type="hadoop")
    result = _jars_packages_config(settings, extra_packages=["com.example:extra:1.0"])
    assert result["spark.jars.packages"].endswith("com.example:extra:1.0")


def test_jars_packages_config_empty_when_already_baked():
    settings = config_module.Settings(spark_jars_already_baked=True, iceberg_catalog_type="jdbc")
    result = _jars_packages_config(settings, extra_packages=None)
    assert result == {}
