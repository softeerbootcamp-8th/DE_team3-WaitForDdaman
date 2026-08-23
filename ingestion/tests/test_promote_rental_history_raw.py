"""Bronze 승격 잡의 매핑/멱등성 테스트.

Spark/Iceberg 통합 테스트는 Maven에서 Iceberg 런타임을 받아야 해서 기본 실행에서는
건너뛴다. jar가 준비된 컨테이너에서 RENTAL_HISTORY_SPARK_IT=1로 명시적으로 켠다.
"""
import json
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from moto import mock_aws

import config as config_module
from jobs import promote_rental_history_raw as promoter

KST = ZoneInfo("Asia/Seoul")
BUCKET = "test-promotion-bucket"
CUTOFF = "2026-08-22T06:00:00+09:00"

SPARK_IT = pytest.mark.skipif(
    os.getenv("RENTAL_HISTORY_SPARK_IT") != "1",
    reason="Iceberg 런타임 jar가 준비된 환경에서만 실행 (RENTAL_HISTORY_SPARK_IT=1)",
)

VALID_ROW = {
    "BIKE_ID": "SPB-1",
    "RENT_DT": "2026-08-21 04:00:00",
    "RENT_ID": "101",
    "RTN_DT": "2026-08-21 04:10:00",
    "RTN_ID": "102",
    "USE_MIN": "10",
    "USE_DST": "1234.5",
    "START_INDEX": 1,
    "END_INDEX": 1,
    "RNUM": "1",
}


def _payload_key(target_date: str, observed_at: str, snapshot_type: str) -> str:
    observed_key = datetime.fromisoformat(observed_at).astimezone(KST).strftime(
        "%Y%m%dT%H%M%S%z"
    )
    return (
        f"raw/rental_history/api/target_date={target_date}/"
        f"observed_at={observed_key}/snapshot_type={snapshot_type}/payload.json"
    )


def _selected(
    target_date: str,
    observed_at: str,
    snapshot_type: str,
    hours: list[int],
    row_count: int = 2,
    fallback_reason: str | None = None,
) -> dict:
    payload_key = _payload_key(target_date, observed_at, snapshot_type)
    return {
        "target_date": target_date,
        "snapshot_type": snapshot_type,
        "observed_at": datetime.fromisoformat(observed_at).astimezone(KST).isoformat(),
        "payload_key": payload_key,
        "manifest_key": payload_key.replace("payload.json", "manifest.json"),
        "requested_hours": list(hours),
        "row_count": row_count,
        "fallback_reason": fallback_reason,
    }


def _selection_document(selected: list[dict], mode: str = "NORMAL") -> dict:
    return {
        "dataset": "rental_history",
        "run_date": "2026-08-22",
        "promotion_id": "20260822T060000+0900",
        "collection_cutoff_at": CUTOFF,
        "source_bucket": BUCKET,
        "fallback_enabled": mode == "DEGRADED",
        "t0_enabled": False,
        "required_confirmed_dates": [
            s["target_date"] for s in selected if s["target_date"] != "2026-08-22"
        ],
        "current_date_required": any(s["target_date"] == "2026-08-22" for s in selected),
        "selected_snapshots": selected,
        "mode": mode,
        "selection_key": (
            "_meta/promotion/bronze_rental_history/run_date=2026-08-22/"
            "promotion_id=20260822T060000+0900/selection.json"
        ),
    }


# ------------------------------------------------------------------ mapping


def test_source_file_is_the_selected_payload_key_verbatim():
    selected = _selected(
        "2026-08-21", "2026-08-22T05:00:00+09:00", "PRELIMINARY", list(range(24))
    )
    document = _selection_document([selected], mode="DEGRADED")

    plan = promoter.plan_promotion(document)

    assert [item["rent_date_partition"] for item in plan] == ["2026-08-21"]
    assert plan[0]["source_file"] == selected["payload_key"]
    assert plan[0]["source_file"].startswith("raw/rental_history/api/target_date=")
    assert not plan[0]["source_file"].startswith("s3://")
    assert plan[0]["source_file"].endswith("payload.json")


def test_validate_selection_document_rejects_inconsistent_contract():
    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )
    promoter.validate_selection_document(document, promotion_id="20260822T060000+0900")

    with pytest.raises(promoter.PromotionError, match="promotion_id"):
        promoter.validate_selection_document(document, promotion_id="20260822T070000+0900")

    broken = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )
    broken["selected_snapshots"][0]["payload_key"] = _payload_key(
        "2026-08-20", CUTOFF, "FINAL"
    )
    with pytest.raises(promoter.PromotionError, match="payload_key"):
        promoter.validate_selection_document(broken, promotion_id="20260822T060000+0900")

    empty = _selection_document([])
    with pytest.raises(promoter.PromotionError, match="selected_snapshots"):
        promoter.validate_selection_document(empty, promotion_id="20260822T060000+0900")


def test_build_promotion_document_records_commit_marker():
    selected = [
        _selected("2026-08-20", CUTOFF, "FINAL", list(range(24)), row_count=5),
        _selected(
            "2026-08-21",
            "2026-08-22T05:00:00+09:00",
            "PRELIMINARY",
            list(range(24)),
            row_count=7,
            fallback_reason="FINAL_INCOMPLETE",
        ),
    ]
    document = _selection_document(selected, mode="DEGRADED")

    promotion = promoter.build_promotion_document(
        document, {"2026-08-20": 5, "2026-08-21": 7}
    )

    assert promotion["status"] == "COMPLETE"
    assert promotion["mode"] == "DEGRADED"
    assert promotion["promoted_partitions"] == ["2026-08-20", "2026-08-21"]
    assert promotion["bronze_row_count_by_partition"] == {
        "2026-08-20": 5,
        "2026-08-21": 7,
    }
    assert promotion["confirmed_through_candidate"] == "2026-08-21"
    assert promotion["promotion_reasons"] == {"2026-08-21": "FINAL_INCOMPLETE"}
    assert promotion["selected_snapshots"] == selected
    assert promotion["source_bucket"] == BUCKET
    assert promotion["promoted_at"].endswith("+00:00")


def test_partial_day_promotion_is_not_a_confirmed_candidate():
    selected = [
        _selected("2026-08-21", CUTOFF, "FINAL", list(range(24)), row_count=5),
        _selected("2026-08-22", CUTOFF, "FINAL", [0, 1, 2, 3, 4, 5], row_count=3),
    ]
    document = _selection_document(selected)

    promotion = promoter.build_promotion_document(
        document, {"2026-08-21": 5, "2026-08-22": 3}
    )

    assert promotion["promoted_partitions"] == ["2026-08-21", "2026-08-22"]
    assert promotion["confirmed_through_candidate"] == "2026-08-21"


def test_row_count_mismatch_blocks_the_commit_marker():
    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)), row_count=5)]
    )

    with pytest.raises(promoter.PromotionError, match="row count"):
        promoter.build_promotion_document(document, {"2026-08-21": 4})


# --------------------------------------------------------------- payload IO


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket=BUCKET,
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket(BUCKET)
        yield


def test_missing_payload_fails_before_spark_starts(s3_env):
    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )

    with pytest.raises(promoter.PromotionError, match="payload"):
        promoter.load_payloads(BUCKET, promoter.plan_promotion(document))


def test_load_payloads_returns_rows_per_partition(s3_env):
    from common.s3_utils import put_json

    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)))]
    )
    plan = promoter.plan_promotion(document)
    put_json(BUCKET, plan[0]["source_file"], [VALID_ROW, VALID_ROW])

    loaded = promoter.load_payloads(BUCKET, plan)

    assert [item["rent_date_partition"] for item in loaded] == ["2026-08-21"]
    assert loaded[0]["rows"] == [VALID_ROW, VALID_ROW]


def test_payload_row_count_must_match_the_manifest(s3_env):
    from common.s3_utils import put_json

    document = _selection_document(
        [_selected("2026-08-21", CUTOFF, "FINAL", list(range(24)), row_count=2)]
    )
    plan = promoter.plan_promotion(document)
    put_json(BUCKET, plan[0]["source_file"], [VALID_ROW])

    with pytest.raises(promoter.PromotionError, match="row count"):
        promoter.load_payloads(BUCKET, plan)


# ------------------------------------------------------- Spark 통합 (opt-in)


@pytest.fixture(scope="module")
def iceberg_spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    spark = (
        SparkSession.builder.appName("promote-rental-history-test")
        .master("local[1]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.test_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.test_catalog.type", "hadoop")
        .config("spark.sql.catalog.test_catalog.warehouse", str(warehouse))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@SPARK_IT
def test_repromoting_the_same_selection_keeps_partition_counts_and_checksum(
    iceberg_spark, monkeypatch
):
    from pyspark.sql import functions as F

    monkeypatch.setattr(
        config_module,
        "SETTINGS",
        config_module.Settings(
            env="aws", raw_bucket=BUCKET, iceberg_catalog_name="test_catalog"
        ),
    )

    selected = [
        _selected("2026-08-20", CUTOFF, "FINAL", list(range(24)), row_count=2),
        _selected(
            "2026-08-21",
            "2026-08-22T05:00:00+09:00",
            "PRELIMINARY",
            list(range(24)),
            row_count=3,
            fallback_reason="FINAL_INCOMPLETE",
        ),
    ]
    document = _selection_document(selected, mode="DEGRADED")
    plan = promoter.plan_promotion(document)
    payloads = [
        {**item, "rows": [dict(VALID_ROW, BIKE_ID=f"SPB-{item['rent_date_partition']}-{i}")
                          for i in range(item["row_count"])]}
        for item in plan
    ]

    promoter.ensure_bronze_table(iceberg_spark)

    first = promoter.promote(iceberg_spark, payloads)
    second = promoter.promote(iceberg_spark, payloads)

    assert first == {"2026-08-20": 2, "2026-08-21": 3}
    assert second == first

    table = iceberg_spark.table(promoter.bronze_table_name())
    assert table.count() == 5
    source_files = {
        row["source_file"] for row in table.select("source_file").distinct().collect()
    }
    assert source_files == {item["source_file"] for item in plan}

    business_columns = [
        c for c in table.columns if c not in ("source_file", "ingested_at")
    ]
    checksum = (
        table.select(F.xxhash64(*business_columns).alias("h"))
        .agg(F.sum("h"))
        .collect()[0][0]
    )
    promoter.promote(iceberg_spark, payloads)
    recomputed = (
        iceberg_spark.table(promoter.bronze_table_name())
        .select(F.xxhash64(*business_columns).alias("h"))
        .agg(F.sum("h"))
        .collect()[0][0]
    )
    assert recomputed == checksum
