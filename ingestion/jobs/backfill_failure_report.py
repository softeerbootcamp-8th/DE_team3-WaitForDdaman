"""
Bronze 백필 잡 - 서울시 공공자전거 고장신고 내역 (OA-15644)

실행 방식: 열린데이터광장에서 반기별로 "대량 다운로드"한 원본 파일들을
로컬 디렉토리(INPUT_DIR)에 모아두고 1회 실행한다.

주의: 가장 오래된 파일(2015_2020.10)은 .xlsx 형식이고, 나머지는 .csv(일부는
확장자만 .csv인 zip)다. 둘 다 이 잡에서 자동으로 처리한다.

멱등성: 재실행 시 동일 날짜(reg_date_partition) 파티션을 덮어쓴다.
안전한 실패: 파일 하나가 스키마 검증에 실패해도 전체 배치를 죽이지 않고 스킵한다.

사용법:
    INPUT_DIR=./raw_downloads python -m jobs.backfill_failure_report
    INPUT_DIR=./raw_downloads INPUT_FILE_PATTERN="*2601*" python -m jobs.backfill_failure_report
"""
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

import config
from common.encoding_utils import convert_euckr_file_to_utf8
from common.file_utils import NotThisDatasetError, convert_xlsx_to_utf8_csv, unzip_if_needed
from common.s3_utils import ensure_bucket, upload_file
from common.spark_session import build_spark_session
from schema.failure_report_schema import (
    SchemaValidationError,
    build_select_exprs,
    is_failure_report_file,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.failure_report"


def _ensure_bronze_table(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name()} (
            bike_no STRING,
            reg_dttm STRING,
            failure_type STRING,
            reg_date_partition STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (reg_date_partition)
        """
    )
    # 대여이력과 동일한 이유로 hash distribution을 명시 - FanoutWriter의 높은 메모리
    # 사용(파티션별 파일을 동시에 여러 개 열어둠)을 피하고 Iceberg가 직접 분산+정렬하게 함.
    spark.sql(
        f"ALTER TABLE {_table_name()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )


def _derive_date_partition(df, source_col: str):
    """REGDTTM 형식 편차(YYYYMMDD / YYYY-MM-DD HH:mm:ss 등)에 대응해 숫자만 추출 후 정규화."""
    digits = F.regexp_replace(F.col(source_col), r"[^0-9]", "")
    return F.concat_ws(
        "-",
        F.substring(digits, 1, 4),
        F.substring(digits, 5, 2),
        F.substring(digits, 7, 2),
    )


def _stage_as_utf8_csv(raw_path: Path, workdir: Path) -> Path:
    """
    원본 파일(csv/zip 안의 csv/xlsx)을 UTF-8 CSV로 통일해서 스테이징한다.
    - .xlsx: pandas로 읽어 바로 UTF-8 CSV로 변환 (이미 텍스트라 EUC-KR 변환 불필요)
    - .csv(EUC-KR): iconv -c와 동일한 방식으로 UTF-8 변환
    """
    if raw_path.suffix.lower() == ".xlsx":
        return convert_xlsx_to_utf8_csv(raw_path, workdir)

    utf8_path = workdir / f"{raw_path.stem}.utf8.csv"
    convert_result = convert_euckr_file_to_utf8(raw_path, utf8_path)
    if convert_result["dropped_bytes"] > 0:
        logger.warning("파일 %s: 손상 바이트 %d개 폐기", raw_path.name, convert_result["dropped_bytes"])
    return utf8_path


def _process_one_file(spark, raw_path: Path, workdir: Path):
    utf8_path = _stage_as_utf8_csv(raw_path, workdir)

    raw_df = spark.read.option("header", "true").csv(str(utf8_path))
    actual_columns = raw_df.columns

    if not is_failure_report_file(actual_columns):
        raise NotThisDatasetError(f"고장신고 스키마와 겹치는 컬럼이 거의 없음 (실제 컬럼: {actual_columns})")

    landing_date = datetime.utcnow().strftime("%Y-%m-%d")
    ensure_bucket(config.SETTINGS.raw_bucket)
    upload_file(raw_path, config.SETTINGS.raw_bucket, f"raw/failure_report/_landing/{landing_date}/{raw_path.name}")

    validate_and_report(actual_columns)

    select_exprs = build_select_exprs(actual_columns)
    mapped_df = raw_df.select(*select_exprs)

    bronze_df = (
        mapped_df.withColumn("reg_date_partition", _derive_date_partition(mapped_df, "reg_dttm"))
        .withColumn("source_file", F.lit(raw_path.name))
        .withColumn("ingested_at", F.current_timestamp())
    )
    return bronze_df


def run(input_dir: str) -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-backfill-failure-report")
    _ensure_bronze_table(spark)

    file_pattern = os.getenv("INPUT_FILE_PATTERN", "*")
    input_files = sorted(Path(input_dir).glob(file_pattern))
    if not input_files:
        logger.error("입력 디렉토리에 파일이 없습니다: %s (패턴: %s)", input_dir, file_pattern)
        sys.exit(1)

    total_files, total_rows, failed_files, skipped_files = 0, 0, [], []

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        for raw_path in input_files:
            for target_path in unzip_if_needed(raw_path, workdir):
                total_files += 1
                try:
                    bronze_df = _process_one_file(spark, target_path, workdir).cache()
                    row_count = bronze_df.count()

                    bronze_df.writeTo(_table_name()).overwritePartitions()
                    bronze_df.unpersist()

                    total_rows += row_count
                    logger.info("적재 완료: %s (%d행)", target_path.name, row_count)
                except NotThisDatasetError as e:
                    logger.info("고장신고 데이터셋이 아닌 것으로 보여 스킵: %s (%s)", target_path.name, e)
                    skipped_files.append(target_path.name)
                    continue
                except SchemaValidationError as e:
                    logger.error("스키마 검증 실패, 파일 스킵: %s (%s)", target_path.name, e)
                    failed_files.append(target_path.name)
                    continue

    logger.info(
        "백필 종료 - 총 %d개 파일 중 적재 %d개(%d행), 다른 데이터셋으로 스킵 %d개, 스키마 실패 %d개 %s",
        total_files,
        total_files - len(failed_files) - len(skipped_files),
        total_rows,
        len(skipped_files),
        len(failed_files),
        failed_files,
    )
    if failed_files:
        sys.exit(1)


if __name__ == "__main__":
    run(os.getenv("INPUT_DIR", "./raw_downloads"))
