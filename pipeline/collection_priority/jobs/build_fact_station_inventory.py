"""
Gold - 대여소별 자전거 재고 (gold.bike_location + gold.station_active +
silver.bikeman_action -> gold.fact_station_inventory)

### 최종 위치 결정 규칙
자전거의 "진짜 현재 위치"는 대여이력 기준 위치(gold.bike_location)와 수거/배치
이벤트(silver.bikeman_action) 중 시각이 더 최신인 쪽을 따른다.

    1. bikeman_action에 해당 자전거 이벤트가 아예 없거나, 있어도
       bike_location.last_event_at보다 오래됐다 -> bike_location 그대로 사용
    2. 가장 최신 이벤트가 COLLECT(수거)다 -> 필드에서 제거된 상태이므로
       재고 집계에서 제외 (어느 대여소에도 속하지 않음)
    3. 가장 최신 이벤트가 DEPLOY(배치)이고 station_id가 있다 -> 그 station_id로
       위치를 덮어씀 (배치 이벤트가 대여이력보다 최신 정보이므로)
    4. DEPLOY인데 station_id가 없다(이례적 케이스) -> bike_location 값으로 폴백

### bike_cnt / target_bike_cnt - 왜 이 집계 자체는 증분화하지 않는가
bike_cnt는 대여소별 "지금 몇 대 있는가"를 매번 다시 세야 하는 집계값이다.
bike_location처럼 "안 바뀐 자전거는 그대로 두고 바뀐 것만 갱신"하는 방식이
안 통한다 - 자전거 한 대만 위치가 바뀌어도 그 대여소의 합계가 통째로
바뀌기 때문이다. 다만 집계 재료 중 gold.bike_location/gold.bike_last_action은
둘 다 "자전거 수만큼"(수만 건)으로 크기가 고정된 상태 테이블이라 매번 전체를
다시 읽어도 비용이 크지 않다 - 증분화가 필요한 건 그 재료를 만드는 쪽이다.

gold.station_active(운영 중인 대여소만)를 기준으로 대여소별 자전거 수를 센다.
자전거가 하나도 없는 대여소도 0으로 나와야 하므로 station_active를 기준(left side)
으로 두고 자전거 수를 왼쪽 조인한다. target_bike_cnt는 거치대 수(hold_num)를
목표치로 사용한다. station_active는 대여소 수만큼(수백~수천 건)이라 build_station_active.py와
동일하게 broadcast 힌트를 준다 - bike_counts는 그보다 더 작거나 같으므로(대여소당
최대 1행) 셔플 조인보다 확실히 저렴하다.

### gold.bike_last_action - 증분 유지되는 "자전거별 최신 수거/배치 이벤트"
silver.bikeman_action은 매일 계속 쌓이는 이벤트 로그라, 매번 전체를 훑어서
자전거별 최신 이벤트를 찾으면(예전 방식) 데이터가 쌓일수록 느려진다.
gold.bike_location과 동일한 baseline+delta 패턴으로 증분 유지한다. 이 상태
테이블도 "자전거 수만큼"으로 크기가 고정되므로 이후 join/집계 비용은
커지지 않는다.

### 델타 구간을 "어제 하루"로 고정하지 않고 자기 자신의 snapshot_date로 추적하는 이유
gold.bike_location과 동일한 이유(자세한 설명은 build_bike_location.py 참고) -
어제 하루로 고정하면 gold_dim_fact가 하루라도 실행을 건너뛴 사이의
bikeman_action 이벤트는 어떤 미래 실행에서도 다시는 스캔되지 않아 조용히
유실된다. gold.bike_last_action 자신의 snapshot_date(MAX)를 워터마크로 재사용해서
[직전 snapshot_date, 이번 snapshot_date - 1] 구간을 스캔한다 - 평소엔 하루뿐이라
성능은 동일하고, 실행이 밀렸을 때만 구간이 넓어져 스스로 복구된다.

`occurred_date_partition` identity 파티션 범위로 파일을 먼저 줄이고, 정확한 경계는
기존처럼 occurred_at 타임스탬프 범위(`>= 시작 AND < 끝+1일`)로 다시 제한한다.

### 전체 덮어쓰기 (TEMP류 입력에 의존하는 최신 상태)
### 적재 전 PyDeequ 검증
build_dim_bike.py와 동일한 패턴으로 gold.bike_last_action/gold.fact_station_inventory
둘 다 쓰기 전에 검증한다. 실패하면 GoldValidationError로 배치를 즉시 중단한다.

사용법:
    python -m jobs.build_fact_station_inventory
    SNAPSHOT_DATE=2026-08-17 python -m jobs.build_fact_station_inventory
"""
import logging
import os
import sys
from datetime import date, datetime, time, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _bike_location_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_location"


def _station_active_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.station_active"


def _bikeman_action_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.bikeman_action"


def _bike_last_action_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_last_action"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_station_inventory"


def _ensure_gold_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            station_id      STRING,
            bike_cnt        INT,
            hold_num        INT,
            target_bike_cnt INT,
            snapshot_date   DATE
        )
        USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_bike_last_action_table()} (
            bike_id            STRING,
            action_event_type  STRING,
            action_station_id  STRING,
            action_at          TIMESTAMP,
            snapshot_date      DATE
        )
        USING iceberg
        """
    )


def _bike_last_action_baseline(spark):
    """직전 상태 전체 (테이블이 비어있으면 빈 DataFrame - cold start도 안전)."""
    return spark.read.table(_bike_last_action_table()).select(
        "bike_id",
        F.col("action_event_type").alias("base_event_type"),
        F.col("action_station_id").alias("base_station_id"),
        F.col("action_at").alias("base_at"),
    )


def _bike_last_action_baseline_snapshot_date(spark) -> date | None:
    """gold.bike_last_action에 남아있는 MAX(snapshot_date) - 그 값보다 이전
    bikeman_action 이벤트는 이미 baseline에 반영되어 있으므로, 이번 실행의
    델타 시작일로 그대로 쓸 수 있다. 테이블이 비어있으면(cold start) None."""
    return spark.read.table(_bike_last_action_table()).agg(F.max("snapshot_date")).collect()[0][0]


def _bike_last_action_delta(spark, start_date: date | None, end_date: date):
    """bikeman_action에서 아직 baseline에 반영 안 된 구간 [start_date, end_date]만
    스캔해서 자전거별 최신 이벤트 1건만 채택한다. start_date가 None이면(cold start)
    하한 없이 end_date까지 전체를 스캔한다."""
    end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min)
    bikeman_action_df = spark.read.table(_bikeman_action_table()).filter(
        (F.col("occurred_date_partition") <= F.lit(end_date.isoformat()))
        & (F.col("occurred_at") < F.lit(end_exclusive))
    )
    if start_date is not None:
        start_inclusive = datetime.combine(start_date, time.min)
        bikeman_action_df = bikeman_action_df.filter(
            (F.col("occurred_date_partition") >= F.lit(start_date.isoformat()))
            & (F.col("occurred_at") >= F.lit(start_inclusive))
        )

    window = Window.partitionBy("bike_id").orderBy(F.col("occurred_at").desc())
    return (
        bikeman_action_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "bike_id",
            F.col("event_type").alias("delta_event_type"),
            F.col("station_id").alias("delta_station_id"),
            F.col("occurred_at").alias("delta_at"),
        )
    )


def _merge_bike_last_action(baseline_df, delta_df, snapshot_date: date):
    """baseline(직전 상태)과 delta(신규 반영분)를 병합한다 - Spark 세션/카탈로그
    없이 두 DataFrame만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    merged = baseline_df.join(delta_df, on="bike_id", how="outer")

    delta_is_newer = F.col("delta_at").isNotNull() & (
        F.col("base_at").isNull() | (F.col("delta_at") > F.col("base_at"))
    )

    return merged.select(
        "bike_id",
        F.when(delta_is_newer, F.col("delta_event_type")).otherwise(F.col("base_event_type")).alias(
            "action_event_type"
        ),
        F.when(delta_is_newer, F.col("delta_station_id")).otherwise(F.col("base_station_id")).alias(
            "action_station_id"
        ),
        F.when(delta_is_newer, F.col("delta_at")).otherwise(F.col("base_at")).alias("action_at"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )


def build_bike_last_action(spark, snapshot_date: date):
    """gold.bike_last_action의 오늘자 스냅샷을 증분(baseline+delta)으로 계산한다."""
    baseline = _bike_last_action_baseline(spark)
    delta_start = _bike_last_action_baseline_snapshot_date(spark)
    delta_end = snapshot_date - timedelta(days=1)
    delta = _bike_last_action_delta(spark, delta_start, delta_end)

    return _merge_bike_last_action(baseline, delta, snapshot_date)


def _resolve_bike_station(bike_location_df, latest_action_df):
    """자전거별 최종 위치(effective_station_id, excluded)를 계산한다 - 두
    DataFrame만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    joined = bike_location_df.join(
        latest_action_df.select(
            "bike_id",
            F.col("action_event_type"),
            F.col("action_station_id"),
            F.col("action_at"),
        ),
        on="bike_id",
        how="left",
    )

    action_is_newer = F.col("action_at").isNotNull() & (
        F.col("action_at") > F.col("last_event_at")
    )

    return joined.select(
        "bike_id",
        F.when(
            action_is_newer & (F.col("action_event_type") == "COLLECT"),
            F.lit(None).cast("string"),
        )
        .when(
            action_is_newer & (F.col("action_event_type") == "DEPLOY"),
            F.coalesce(F.col("action_station_id"), F.col("last_station_id")),
        )
        .otherwise(F.col("last_station_id"))
        .alias("effective_station_id"),
        # 최신 이벤트가 COLLECT면 필드에서 제거된 상태 -> 재고 집계 제외
        (action_is_newer & (F.col("action_event_type") == "COLLECT")).alias("excluded"),
    )


def _aggregate_station_inventory(resolved_df, station_active_df, snapshot_date: date):
    """대여소별 재고를 집계한다 - 두 DataFrame만으로 동작하는 순수 로직이라
    단위 테스트가 가능하다."""
    active_bikes = resolved_df.filter(~F.col("excluded") & F.col("effective_station_id").isNotNull())

    # left outer join은 build(broadcast) 대상이 오른쪽(non-preserved side)이어야 한다 -
    # 왼쪽(station_active, 이 조인의 outer/preserved 쪽)에 broadcast를 걸면 Spark가
    # 힌트를 무시하고 경고만 남긴다(실측으로 확인: "Hint (strategy=broadcast) is not
    # supported ... build left for left outer join"). bike_counts는 대여소당 최대
    # 1행이라 station_active보다도 작거나 같으므로 이쪽에 broadcast를 건다.
    bike_counts = F.broadcast(
        active_bikes.groupBy(F.col("effective_station_id").alias("station_id")).agg(
            F.count("*").alias("bike_cnt")
        )
    )

    return (
        station_active_df.select("station_id", "hold_num")
        .join(bike_counts, on="station_id", how="left")
        .select(
            "station_id",
            F.coalesce(F.col("bike_cnt"), F.lit(0)).cast("int").alias("bike_cnt"),
            "hold_num",
            F.col("hold_num").alias("target_bike_cnt"),
            F.lit(snapshot_date).cast("date").alias("snapshot_date"),
        )
    )


def build_fact_station_inventory(spark, snapshot_date: date, latest_action):
    bike_location = spark.read.table(_bike_location_table())
    resolved = _resolve_bike_station(bike_location, latest_action)

    station_active = spark.read.table(_station_active_table())
    return _aggregate_station_inventory(resolved, station_active, snapshot_date)


def _validate_bike_last_action(spark, df) -> None:
    # bikeman_action(수거/배치) 이벤트가 아직 하나도 없는 환경(신규 배포 직후 등)에서는
    # 이 결과가 통째로 0행일 수 있다 - 정상 상태다. PyDeequ의 hasUniqueness/isComplete는
    # 빈 데이터셋에서 "값이 전부 NULL이었다"는 의미로 실패 처리해버리므로(실측으로 확인),
    # 빈 경우엔 검증 자체를 스킵한다 - 안 그러면 실제로는 문제 없는 정상 배치가
    # 이벤트 데이터가 아직 없다는 이유만으로 계속 막히게 된다.
    if df.isEmpty():
        logger.info("gold.bike_last_action 결과가 비어있음(bikeman_action 이벤트 없음) - PyDeequ 검증 스킵")
        return

    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

    check = Check(spark, CheckLevel.Error, "bike_last_action_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(check.hasUniqueness(["bike_id"], lambda fraction: fraction > 0.99).isComplete("bike_id"))
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"gold.bike_last_action 품질 검증 실패: {failed_constraints}")


def _validate_fact_station_inventory(spark, df) -> None:
    # gold.station_active(운영 중 대여소)가 아직 없으면 이 결과도 0행일 수 있다 -
    # bike_last_action과 동일한 이유로 빈 데이터셋에서는 PyDeequ 검증을 스킵한다.
    if df.isEmpty():
        logger.info("gold.fact_station_inventory 결과가 비어있음 - PyDeequ 검증 스킵")
        return

    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

    check = Check(spark, CheckLevel.Error, "fact_station_inventory_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.hasUniqueness(["station_id"], lambda fraction: fraction > 0.99)
            .isComplete("station_id")
            .isNonNegative("bike_cnt")
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"gold.fact_station_inventory 품질 검증 실패: {failed_constraints}")


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("gold-build-fact-station-inventory")
    try:
        _ensure_gold_table(spark)

        # gold.bike_last_action을 먼저 증분 갱신 - 캐싱해서 아래 계산에 재사용하고
        # 그대로 테이블에도 써서 다음 실행의 baseline이 되게 한다.
        latest_action = build_bike_last_action(spark, snapshot_date).cache()
        latest_action.count()
        try:
            _validate_bike_last_action(spark, latest_action)
        except GoldValidationError as e:
            logger.error("%s: gold.bike_last_action 검증 실패, 적재 중단: %s", snapshot_date_str, e)
            latest_action.unpersist()
            sys.exit(1)
        latest_action.writeTo(_bike_last_action_table()).overwritePartitions()

        out_df = build_fact_station_inventory(spark, snapshot_date, latest_action).cache()
        row_count = out_df.count()
        try:
            _validate_fact_station_inventory(spark, out_df)
        except GoldValidationError as e:
            logger.error("%s: gold.fact_station_inventory 검증 실패, 적재 중단: %s", snapshot_date_str, e)
            out_df.unpersist()
            latest_action.unpersist()
            sys.exit(1)

        out_df.writeTo(_gold_table()).overwritePartitions()
        out_df.unpersist()
        latest_action.unpersist()

        logger.info(
            "%s: gold.fact_station_inventory %d행 갱신 완료 (bike_last_action 증분 처리)",
            snapshot_date_str, row_count,
        )
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
