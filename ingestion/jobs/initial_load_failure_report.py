"""
Bronze 초기 적재 잡 - 서울시 공공자전거 고장신고 내역 (OA-15644)

실행 방식: 파일 하나(INPUT_FILE)를 받아 독립된 프로세스(=하나의 Spark 세션/JVM)로
처리하고 종료한다. 파일 다운로드와 대상 목록 나열은 jobs/list_input_files.py가
먼저 수행한다 - Airflow DAG는 그 목록으로 Dynamic Task Mapping을 돌려 파일마다
이 스크립트를 별도 프로세스로 실행한다.

주의: 가장 오래된 파일(2015_2020.10)은 .xlsx 형식이고, 나머지는 .csv(일부는
확장자만 .csv인 zip)다. 둘 다 이 잡에서 자동으로 처리한다.

멱등성: 재실행 시 동일 날짜(reg_date_partition) 파티션을 덮어쓴다.
안전한 실패: 압축 파일 하나가 여러 CSV로 풀리는 경우, 그중 스키마 검증에 실패하는
            CSV가 있어도 전체를 죽이지 않고 그 CSV만 스킵한다.

사용법:
    INPUT_FILE=./raw_downloads/failure_2601.csv python -m jobs.initial_load_failure_report
"""
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

import config
from common.encoding_utils import EncodingMismatchError, convert_euckr_file_to_utf8
from common.file_utils import NotThisDatasetError, convert_xlsx_to_utf8_csv, is_xlsx, unzip_if_needed
from common.s3_utils import download_file, ensure_bucket, split_s3_uri, upload_file
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
    if is_xlsx(raw_path):
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


def run(input_file: str) -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-initial-load-failure-report")
    _ensure_bronze_table(spark)

    total_rows, failed, skipped = 0, False, False

    # 파일 1개 처리 범위로 한정된 TemporaryDirectory. 프로세스도 파일 1개만 처리하고
    # 종료하므로, 정상/OOM 종료 여부와 무관하게 이 파일 하나 분량만 디스크에 남을 수 있다.
    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        if input_file.startswith("s3://"):
            bucket, key = split_s3_uri(input_file)
            raw_path = workdir / Path(key).name
            download_file(bucket, key, raw_path)
        else:
            raw_path = Path(input_file)
            if not raw_path.exists():
                logger.error("입력 파일이 없습니다: %s", input_file)
                sys.exit(1)

        for target_path in unzip_if_needed(raw_path, workdir):
            try:
                bronze_df = _process_one_file(spark, target_path, workdir).cache()
                row_count = bronze_df.count()

                bronze_df.writeTo(_table_name()).overwritePartitions()
                bronze_df.unpersist()

                total_rows += row_count
                logger.info("적재 완료: %s (%d행)", target_path.name, row_count)
            except NotThisDatasetError as e:
                logger.info("고장신고 데이터셋이 아닌 것으로 보여 스킵: %s (%s)", target_path.name, e)
                skipped = True
            except SchemaValidationError as e:
                logger.error("스키마 검증 실패: %s (%s)", target_path.name, e)
                failed = True
            except EncodingMismatchError as e:
                # EUC-KR/CP949가 아닌 다른 인코딩으로 추정됨 - 조용히 깨진 데이터를
                # 적재하는 대신 스키마 검증 실패와 동일하게 취급한다 (이 파일만 스킵).
                logger.error("인코딩 불일치로 스킵: %s (%s)", target_path.name, e)
                failed = True

    logger.info(
        "파일 처리 종료: %s, 총 %d행, 다른 데이터셋으로 스킵=%s, 스키마 실패=%s",
        input_file,
        total_rows,
        skipped,
        failed,
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    input_file = os.getenv("INPUT_FILE")
    if not input_file:
        logger.error("사용법: INPUT_FILE=./raw_downloads/failure_2601.csv python -m jobs.initial_load_failure_report")
        sys.exit(1)
    run(input_file)
