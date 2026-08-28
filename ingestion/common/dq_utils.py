"""
PyDeequ 검증 유틸리티 - Silver 레이어 공용.

Bronze는 "행 단위" 검증만 했다(schema/*.py의 validate_and_report, event_type
enum 체크). Silver는 그 위에 PyDeequ로 "데이터셋 단위" 품질 지표(완전성 등)를
정식으로 측정하고, 매 실행마다 dq_check_result 테이블에 적재해서 회차 간 추이를
본다 - 평가기준의 "탐지·기록·처리" 요구사항과 최초 기획 문서의
"결과는 dq_check_result 테이블에 매 실행 적재" 요구사항에 대응.

⚠️ Maven 좌표(com.amazon.deequ:deequ:2.0.20-spark-3.5)는 Spark 3.5/Scala 2.12
   기준으로 확인한 값이다. 최초 실행 시 Maven Central에서 다운로드되므로 인터넷
   접속이 필요하고(이후 캐시됨), 혹시 버전 충돌 에러가 나면 이 상수만 교체하면 된다.
"""
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F

import config

logger = logging.getLogger(__name__)

DEEQU_MAVEN_PACKAGE = "com.amazon.deequ:deequ:2.0.20-spark-3.5"
DEEQU_EXCLUDE = "net.sourceforge.f2j:arpack_combined_all"

# pydeequ가 내부적으로 SPARK_VERSION 환경변수를 참조하는 경로가 있어 방어적으로 설정.
# 위에서 Maven 좌표를 직접 고정했으므로 이 값 자체가 좌표 계산에 쓰이진 않지만,
# 버전 미설정 시 예외를 던지는 내부 체크가 있어 넣어둔다.
os.environ.setdefault("SPARK_VERSION", "3.5")


def _dq_result_table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.dq_check_result"


def ensure_dq_result_table(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_dq_result_table_name()} (
            dataset            STRING,
            occurred_date      STRING,
            check_name         STRING,
            check_level        STRING,
            check_status       STRING,
            constraint_desc    STRING,
            constraint_status  STRING,
            constraint_message STRING,
            run_at             TIMESTAMP
        )
        USING iceberg
        """
    )


def verify_bikeman_action(spark, df, dataset: str, occurred_date: str) -> bool:
    """
    Silver로 넘어갈 df(이미 1차 필터링된 valid_rows 기준)에 대해 데이터셋 단위
    품질 검증을 수행한다. 개별 행을 걸러내는 게 아니라 "이 배치 전체가 신뢰할
    만한가"를 판단하는 마지막 게이트 - 실패하면 True/False로 상위에 알린다.

    결과 로깅(append)이 실패해도 본 검증 통과 여부 판단 자체는 막지 않는다
    (로깅 실패로 정상 데이터 적재까지 막히는 건 과함 - 경고만 남기고 진행).
    """
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

    check = (
        Check(spark, CheckLevel.Error, "bikeman_action_silver_checks")
        .isComplete("bike_id")
        .isComplete("event_type")
        .isComplete("occurred_at")
        .isContainedIn("event_type", ["COLLECT", "DEPLOY"])
    )

    result = VerificationSuite(spark).onData(df).addCheck(check).run()
    passed = result.status == "Success"

    try:
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        now = datetime.now(timezone.utc)
        logged_df = (
            result_df.withColumn("dataset", F.lit(dataset))
            .withColumn("occurred_date", F.lit(occurred_date))
            .withColumn("run_at", F.lit(now))
            .selectExpr(
                "dataset",
                "occurred_date",
                "check as check_name",
                "check_level",
                "check_status",
                "constraint as constraint_desc",
                "constraint_status",
                "constraint_message",
                "run_at",
            )
        )
        logged_df.writeTo(_dq_result_table_name()).append()
    except Exception:
        logger.exception(
            "dq_check_result 적재 실패 (%s, %s) - 검증 결과 자체는 %s",
            dataset, occurred_date, "PASS" if passed else "FAIL",
        )

    if not passed:
        logger.error(
            "PyDeequ 검증 실패 (%s, %s) - 상세는 dq_check_result 테이블 참고", dataset, occurred_date
        )
    return passed
