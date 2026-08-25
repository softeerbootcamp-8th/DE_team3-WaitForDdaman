"""
Bronze 초기 적재 잡 - 서울시 공공자전거 고장신고 내역 (OA-15644)

실행 방식: 파일 목록(INPUT_FILES, JSON 배열)을 받아 하나의 프로세스(=하나의 Spark
세션/JVM) 안에서 파일마다 순차로 처리하고 종료한다. 파일 다운로드와 대상 목록 나열은
jobs/list_input_files.py가 먼저 수행하고, DAG가 그 목록을 배치로 잘라(dag_common.
chunk_list, #249) Dynamic Task Mapping으로 배치마다 이 스크립트를 별도 프로세스로
실행한다 - initial_load_rental_history.py와 동일한 구조/이유(그 파일 문서 참고).

주의: 가장 오래된 파일(2015_2020.10)은 .xlsx 형식이고, 나머지는 .csv(일부는
확장자만 .csv인 zip)다. 둘 다 이 잡에서 자동으로 처리한다.

멱등성: 재실행 시 동일 날짜(reg_date_partition) 파티션을 덮어쓴다.
안전한 실패: 배치 안의 한 파일에서, 압축 파일 하나가 여러 CSV로 풀리는 경우 그중
            스키마 검증에 실패하는 CSV가 있어도 전체를 죽이지 않고 그 CSV만 스킵한
            뒤 나머지 파일까지 계속 처리한다. 실패한 파일이 있으면 배치 전체를
            종료 코드 1로 끝낸다(재시도는 배치 단위 - 멱등이라 안전하지만 이미 성공한
            파일도 다시 처리된다).

사용법:
    INPUT_FILES='["./raw_downloads/failure_2601.csv"]' python -m jobs.initial_load_failure_report
"""
import json
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

import config
from common.encoding_utils import EncodingMismatchError, convert_euckr_file_to_utf8
from common.file_utils import NotThisDatasetError, is_xlsx, unzip_if_needed
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


def _read_xlsx_as_spark_df(spark, path: Path):
    """
    xlsx를 pandas로 읽어 driver 메모리에서 바로 Spark DataFrame으로 변환한다 - 로컬이든
    S3든 중간 CSV 파일을 전혀 거치지 않는다. "pandas로 CSV 변환 -> (로컬 또는 S3에)
    스테이징 -> spark.read.csv()로 재읽기" 경로를 여러 번 시도했으나, EMR Serverless
    에서 원인 불명의 [UNABLE_TO_INFER_SCHEMA]로 계속 실패했다(실측: 2026-08-24 - 파일
    내용/인코딩/버킷 리전 전부 정상 확인됐음에도 재현, Issue #223 후속). 파일시스템을
    아예 안 거치면 이 문제 자체가 성립하지 않는다.

    NaN은 이전 경로(pandas.to_csv가 빈 문자열로 씀 -> Spark가 그 빈 문자열을 그대로
    읽음)와 동일한 결과가 되도록 fillna("")로 맞춘다.

    engine을 명시하지 않으면 pandas가 확장자로 엔진을 추론하는데, .csv로 위장된 xlsx는
    확장자만으로 엔진을 못 정해서 에러가 난다 - 내용은 항상 xlsx이므로 고정한다
    (is_xlsx()가 확장자가 아니라 내용으로 이미 판별한 뒤 호출됨).
    """
    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    return spark.createDataFrame(df)


def _read_csv_as_spark_df(spark, path: Path):
    """
    변환된 UTF-8 CSV를 pandas로 읽어 driver 메모리에서 바로 Spark DataFrame으로
    변환한다 - _read_xlsx_as_spark_df와 동일한 이유(위 문단 참고). S3 스테이징 후
    spark.read.csv()로 재읽기(#224)가 일반 EUC-KR 변환 CSV에서도 동일하게 재현돼
    (실측: 2026-08-24, 대여이력 22,966행 정상 파일에서도 재현), 이 파일도 통일한다.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return spark.createDataFrame(df)


def _process_one_file(spark, raw_path: Path, workdir: Path):
    if is_xlsx(raw_path):
        raw_df = _read_xlsx_as_spark_df(spark, raw_path)
    else:
        utf8_path = workdir / f"{raw_path.stem}.utf8.csv"
        convert_result = convert_euckr_file_to_utf8(raw_path, utf8_path)
        if convert_result["dropped_bytes"] > 0:
            logger.warning("파일 %s: 손상 바이트 %d개 폐기", raw_path.name, convert_result["dropped_bytes"])

        raw_df = _read_csv_as_spark_df(spark, utf8_path)
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


def _process_one_input_file(spark, input_file: str) -> tuple[int, bool, bool]:
    """input_file 하나를 처리한다. (row_count, failed, skipped)를 반환한다.

    파일 1개 처리 범위로 한정된 TemporaryDirectory - 이 함수가 반환하면(정상/예외
    무관) 그 즉시 정리되므로, 배치 안에 파일이 여러 개 있어도 동시에 두 파일 분량이
    디스크에 누적되지 않는다.
    """
    row_count, failed, skipped = 0, False, False
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
                return row_count, True, skipped

        for target_path in unzip_if_needed(raw_path, workdir):
            try:
                bronze_df = _process_one_file(spark, target_path, workdir).cache()
                file_row_count = bronze_df.count()

                bronze_df.writeTo(_table_name()).overwritePartitions()
                bronze_df.unpersist()

                row_count += file_row_count
                logger.info("적재 완료: %s (%d행)", target_path.name, file_row_count)
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

    return row_count, failed, skipped


def run(input_files: list[str]) -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-initial-load-failure-report")
    _ensure_bronze_table(spark)

    total_rows = 0
    failed_files: list[str] = []
    skipped_files: list[str] = []

    # 배치 안의 파일을 순차 처리한다 - 하나의 Spark 세션/JVM(=EMR JobRun 하나)을 재사용해
    # 파일마다 새 JobRun을 띄우던 시작 오버헤드를 없앤다(#249). 전체 파일을 한 DataFrame
    # 으로 합치지 않고 파일 단위로 읽고/쓰고/unpersist하므로, 파일 하나 실패가 나머지
    # 파일 처리를 막지 않는다.
    for input_file in input_files:
        row_count, failed, skipped = _process_one_input_file(spark, input_file)
        total_rows += row_count
        if failed:
            failed_files.append(input_file)
        if skipped:
            skipped_files.append(input_file)

    logger.info(
        "배치 처리 종료: 파일 %d개, 총 %d행, 다른 데이터셋으로 스킵=%s, 스키마 실패=%s",
        len(input_files),
        total_rows,
        skipped_files,
        failed_files,
    )
    if failed_files:
        sys.exit(1)


if __name__ == "__main__":
    raw_input_files = os.getenv("INPUT_FILES")
    if not raw_input_files:
        logger.error(
            "사용법: INPUT_FILES='[\"./raw_downloads/failure_2601.csv\"]' "
            "python -m jobs.initial_load_failure_report"
        )
        sys.exit(1)
    run(json.loads(raw_input_files))
