"""
spark_session의 "튜닝값 결정" 로직만 검증한다 - SparkSession을 실제로 만들지 않는다.
JVM을 띄우지 않고 "어떤 값이 왜 선택되는가"만 보는 게 목적이라, 결정 로직을 순수 함수로
분리해두고 그 함수만 테스트한다.

2026-08-21 EC2(t4g.large, 8GB)에서 silver_failure_report가 write 단계에서
java.lang.OutOfMemoryError: Java heap space로 죽은 사고의 회귀 테스트다.
로그에서 드러난 원인은 힙 크기가 아니라 write 병렬도였다:

    [Stage 15: ... (928 + 2) / 934]   <- 읽기/검증: 934 태스크
    [Stage 18: ... (911 + 2) / 934]
    [Stage 20:>          (0 + 2) / 3] <- write: 3 태스크 -> 여기서 OOM

shuffle.partitions=8로 시작한 write 셔플이 AQE 병합으로 3까지 줄어들어, 태스크 하나가
전체의 1/3 + 수백 개 날짜 파티션(days(reg_dttm))의 Parquet writer를 동시에 열었다.
Parquet writer는 파티션마다 row group 버퍼를 잡으므로 3g 힙에서 산수가 맞지 않는다.
그래서 (a) 셔플 파티션을 늘리고 (b) AQE 병합 기준을 낮춰 write 태스크 수를 되찾고
(c) writer 하나가 쓰는 버퍼도 줄인다.
"""
import pytest

from common.spark_session import _s3a_pool_size, _spark_tuning_config

MB = 1024 * 1024


@pytest.fixture(autouse=True)
def _clear_tuning_env(monkeypatch):
    """환경변수 오버라이드가 테스트 간에 새지 않게 한다."""
    for key in (
        "SPARK_S3A_POOL_SIZE",
        "SPARK_LOCAL_SHUFFLE_PARTITIONS",
        "SPARK_ADVISORY_PARTITION_SIZE_MB",
        "SPARK_PARQUET_BLOCK_SIZE_MB",
    ):
        monkeypatch.delenv(key, raising=False)


class TestS3aPoolSize:
    def test_localstack은_레이스_회피용으로_낮게_묶는다(self):
        assert _s3a_pool_size("local") == "5"

    def test_실_s3는_낮은_값이_처리량_손실이라_올린다(self):
        assert _s3a_pool_size("aws") == "32"

    def test_환경변수로_덮어쓸_수_있다(self, monkeypatch):
        monkeypatch.setenv("SPARK_S3A_POOL_SIZE", "100")

        assert _s3a_pool_size("local") == "100"
        assert _s3a_pool_size("aws") == "100"


class TestSparkTuningConfig:
    def test_셔플_파티션_기본값이_write_병렬도를_확보한다(self):
        # 8이면 AQE 병합 후 write 태스크가 3까지 줄어 OOM이 재현된다.
        assert _spark_tuning_config()["spark.sql.shuffle.partitions"] == "64"

    def test_aqe_병합_기준을_spark_기본값보다_낮춘다(self):
        # 기본 64MB에서는 늘린 셔플 파티션이 다시 3개로 병합돼 의미가 없어진다.
        advisory = _spark_tuning_config()["spark.sql.adaptive.advisoryPartitionSizeInBytes"]

        assert int(advisory) == 8 * MB

    def test_parquet_writer_버퍼를_줄여_동시_writer를_감당한다(self):
        # 기본 128MB * 동시에 열린 파티션 writer 수 = 힙 초과의 직접 원인.
        block_size = _spark_tuning_config()["spark.hadoop.parquet.block.size"]

        assert int(block_size) == 32 * MB

    def test_셔플_파티션을_환경변수로_덮어쓸_수_있다(self, monkeypatch):
        monkeypatch.setenv("SPARK_LOCAL_SHUFFLE_PARTITIONS", "16")

        assert _spark_tuning_config()["spark.sql.shuffle.partitions"] == "16"

    def test_MB_단위_환경변수는_바이트로_변환된다(self, monkeypatch):
        monkeypatch.setenv("SPARK_ADVISORY_PARTITION_SIZE_MB", "16")
        monkeypatch.setenv("SPARK_PARQUET_BLOCK_SIZE_MB", "64")

        config = _spark_tuning_config()

        assert int(config["spark.sql.adaptive.advisoryPartitionSizeInBytes"]) == 16 * MB
        assert int(config["spark.hadoop.parquet.block.size"]) == 64 * MB
