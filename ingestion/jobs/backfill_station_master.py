"""
Bronze 백필 잡 - 서울시 공공자전거 대여소 정보 (OA-13252)

앞선 두 데이터셋과 결정적으로 다른 점: 이건 이벤트 로그가 아니라 마스터(스냅샷) 데이터다.
- 증분 개념이 없다 → 워터마크 불필요
- 파티션 키가 "발생일"이 아니라 "스냅샷 기준일(snapshot_date)"
- 기준일은 파일명에서 파싱한다 (파일 안에 기준일 컬럼이 없음 - source_data 실측)
  예) "공공자전거 대여소 정보(26.6월 기준).xlsx" → 2026-06-30

파일이 병합 헤더(1~5행) 구조라 헤더 자동 인식이 불가능하다. skiprows=5 + 컬럼 순서
수동 지정으로 파싱하고, 컬럼 개수가 예상과 다르면 잘못된 위치 매핑 대신 즉시 실패한다.

사용법:
    INPUT_DIR=./data/station_master python -m jobs.backfill_station_master
    INPUT_DIR=./data/station_master SNAPSHOT_DATE=2026-06-30 python -m jobs.backfill_station_master
"""
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import functions as F

from common import config
from common.file_utils import NotThisDatasetError
from common.s3_utils import ensure_bucket, upload_file
from common.spark_session import build_spark_session
from schema.station_master_schema import (
    SchemaValidationError,
    build_select_exprs,
    is_station_master_file,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# source_data 실측 기준: skiprows=5 후 이 순서로 컬럼명을 수동 지정해야 한다 (병합 헤더 때문)
FILE_COLUMN_NAMES = [
    "대여소번호",
    "대여소명",
    "자치구",
    "상세주소",
    "위도",
    "경도",
    "설치시기",
    "LCD거치대수",
    "QR거치대수",
    "운영방식",
]
FILE_SKIPROWS = 5


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.station_master"


def _ensure_bronze_table(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name()} (
            station_no STRING,
            station_id STRING,
            station_name STRING,
            station_id_name STRING,
            district STRING,
            hold_num STRING,
            address1 STRING,
            address2 STRING,
            latitude STRING,
            longitude STRING,
            install_date STRING,
            lcd_hold_num STRING,
            qr_hold_num STRING,
            operation_type STRING,
            snapshot_date STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )
    # 다른 Bronze 테이블과 동일한 이유 - Iceberg가 직접 분산/정렬해서 FanoutWriter의
    # 높은 메모리 사용(파티션별 파일 동시 오픈)을 피하게 한다.
    spark.sql(
        f"ALTER TABLE {_table_name()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )


def parse_snapshot_date_from_filename(filename: str) -> str:
    """
    파일명에서 스냅샷 기준일을 파싱한다. 파일 안에 기준일 컬럼이 없으므로
    (source_data 실측) 파일명이 유일한 근거다. 파싱 실패 시 잘못된 날짜로 적재하는 대신
    예외를 던져서, 호출부가 SNAPSHOT_DATE를 명시적으로 받도록 유도한다.

    지원 형태:
        "공공자전거 대여소 정보(26.6월 기준).xlsx"       -> 2026-06-30 (해당 월 말일)
        "공공자전거 대여소 정보(2026.6월 기준).xlsx"     -> 2026-06-30
        "... 2026-06-30 ..."                             -> 2026-06-30
    """
    # 1) YYYY-MM-DD 형태가 그대로 있으면 그걸 사용
    m = re.search(r"(20\d{2})[-.](\d{1,2})[-.](\d{1,2})", filename)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mo:02d}-{d:02d}"

    # 2) "26.6월" / "2026.6월" 형태 -> 해당 월의 말일로 간주 (반기/월 기준 스냅샷이므로)
    m = re.search(r"(\d{2,4})\s*[.\-]\s*(\d{1,2})\s*월", filename)
    if m:
        year_raw, month = m.group(1), int(m.group(2))
        year = int(year_raw) if len(year_raw) == 4 else 2000 + int(year_raw)
        # 다음 달 1일에서 하루 빼서 말일 계산 (calendar 의존 없이)
        if month == 12:
            next_month_first = datetime(year + 1, 1, 1)
        else:
            next_month_first = datetime(year, month + 1, 1)
        last_day = (next_month_first - timedelta(days=1)).day
        return f"{year:04d}-{month:02d}-{last_day:02d}"

    raise ValueError(
        f"파일명에서 스냅샷 기준일을 파싱할 수 없습니다: {filename!r} - "
        "SNAPSHOT_DATE 환경변수로 직접 지정하세요 (예: SNAPSHOT_DATE=2026-06-30)"
    )


def _stage_xlsx_as_csv(xlsx_path: Path, workdir: Path) -> Path:
    """병합 헤더를 건너뛰고 컬럼명을 수동 지정해 UTF-8 CSV로 변환한다."""
    import pandas as pd

    df = pd.read_excel(xlsx_path, skiprows=FILE_SKIPROWS, header=None, dtype=str)
    if df.shape[1] != len(FILE_COLUMN_NAMES):
        raise SchemaValidationError(
            f"예상 컬럼 수({len(FILE_COLUMN_NAMES)})와 실제 컬럼 수({df.shape[1]})가 다름 - "
            f"파일 구조가 바뀐 것으로 보임 (skiprows={FILE_SKIPROWS}/컬럼 순서 재확인 필요): {xlsx_path.name}"
        )
    df.columns = FILE_COLUMN_NAMES

    out_path = workdir / f"{xlsx_path.stem}.staged.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path


def _process_one_file(spark, raw_path: Path, workdir: Path, snapshot_date_override: str | None):
    if raw_path.suffix.lower() not in (".xlsx", ".csv"):
        raise NotThisDatasetError(f"지원하지 않는 확장자: {raw_path.suffix}")

    if raw_path.suffix.lower() == ".xlsx":
        staged_csv = _stage_xlsx_as_csv(raw_path, workdir)
    else:
        # csv로 배포된 회차 - 헤더가 있다고 가정하고 그대로 읽는다
        staged_csv = raw_path

    raw_df = spark.read.option("header", "true").csv(str(staged_csv))
    actual_columns = raw_df.columns

    if not is_station_master_file(actual_columns):
        raise NotThisDatasetError(f"대여소정보 스키마와 겹치는 컬럼이 거의 없음 (실제 컬럼: {actual_columns})")

    snapshot_date = snapshot_date_override or parse_snapshot_date_from_filename(raw_path.name)
    logger.info("파일 %s -> 스냅샷 기준일 %s", raw_path.name, snapshot_date)

    ensure_bucket(config.SETTINGS.raw_bucket)
    upload_file(
        raw_path,
        config.SETTINGS.raw_bucket,
        f"raw/station_master/_landing/{snapshot_date}/{raw_path.name}",
    )

    validate_and_report(actual_columns)

    mapped_df = raw_df.select(*build_select_exprs(actual_columns))
    bronze_df = (
        mapped_df.withColumn("snapshot_date", F.lit(snapshot_date))
        .withColumn("source_file", F.lit(raw_path.name))
        .withColumn("ingested_at", F.current_timestamp())
    )
    return bronze_df


def run(input_dir: str) -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-backfill-station-master")
    _ensure_bronze_table(spark)

    file_pattern = os.getenv("INPUT_FILE_PATTERN", "*")
    snapshot_date_override = os.getenv("SNAPSHOT_DATE")
    input_files = sorted(Path(input_dir).glob(file_pattern))
    if not input_files:
        logger.error("입력 디렉토리에 파일이 없습니다: %s (패턴: %s)", input_dir, file_pattern)
        sys.exit(1)

    total_files, total_rows, failed_files, skipped_files = 0, 0, [], []

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        for raw_path in input_files:
            total_files += 1
            try:
                bronze_df = _process_one_file(spark, raw_path, workdir, snapshot_date_override).cache()
                row_count = bronze_df.count()

                # 같은 스냅샷 기준일로 재실행하면 그 파티션만 덮어쓴다 (멱등성)
                bronze_df.writeTo(_table_name()).overwritePartitions()
                bronze_df.unpersist()

                total_rows += row_count
                logger.info("적재 완료: %s (%d행)", raw_path.name, row_count)
            except NotThisDatasetError as e:
                logger.info("대여소정보 데이터셋이 아닌 것으로 보여 스킵: %s (%s)", raw_path.name, e)
                skipped_files.append(raw_path.name)
                continue
            except (SchemaValidationError, ValueError) as e:
                logger.error("처리 실패, 파일 스킵: %s (%s)", raw_path.name, e)
                failed_files.append(raw_path.name)
                continue

    logger.info(
        "백필 종료 - 총 %d개 파일 중 적재 %d개(%d행), 다른 데이터셋으로 스킵 %d개, 실패 %d개 %s",
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
    run(os.getenv("INPUT_DIR", "./data/station_master"))
