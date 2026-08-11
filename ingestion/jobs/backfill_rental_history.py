"""
Bronze 백필 잡 - 서울시 공공자전거 대여이력 (OA-15182)

실행 방식: 열린데이터광장에서 반기/월별로 "대량 다운로드"한 원본 파일들을
로컬 디렉토리(INPUT_DIR)에 모아두고 1회 실행한다.

멱등성: 재실행 시 동일 날짜(rent_date_partition) 파티션을 덮어쓴다
       (Iceberg overwritePartitions) -> 같은 입력으로 몇 번 돌려도 결과가 같다.

안전한 실패: 파일 하나가 스키마 검증에 실패해도 전체 배치를 죽이지 않고
            해당 파일만 스킵 + 실패 목록을 남긴 뒤, 실패가 있었으면 종료 코드 1로 종료한다
            (Airflow가 실패를 감지할 수 있도록).

사용법:
    INPUT_DIR=./raw_downloads python -m jobs.backfill_rental_history
    INPUT_DIR=./raw_downloads INPUT_FILE_PATTERN="*2601*" python -m jobs.backfill_rental_history  # 1개월치만
"""
import logging
import os
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

from common import config
from common.encoding_utils import convert_euckr_file_to_utf8
from common.s3_utils import ensure_bucket, upload_file
from common.spark_session import build_spark_session
from schema.rental_history_schema import (
    SchemaValidationError,
    build_select_exprs,
    is_rental_history_file,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class NotThisDatasetError(Exception):
    """입력 파일이 대여이력 데이터셋이 아닌 것으로 판단됨 (다른 팀원 데이터셋 등).
    스키마가 깨진 게 아니라 애초에 대상이 아니므로 실패가 아니라 정상 스킵으로 처리한다."""


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.rental_history"


def _unzip_if_needed(path: Path, workdir: Path) -> list[Path]:
    """
    확장자가 아니라 실제 파일 내용(zip 매직바이트)으로 압축 여부를 판별한다.
    서울시 공공데이터에서 확장자는 .csv인데 실제로는 zip 바이너리인 파일이
    실측으로 확인됐다(2026-08-11) - 확장자만 믿으면 안 된다.
    """
    with open(path, "rb") as f:
        magic = f.read(4)
    is_zip = magic[:2] == b"PK"

    if not is_zip:
        return [path] if path.suffix.lower() == ".csv" else []

    extracted = []
    with zipfile.ZipFile(path) as zf:
        zf.extractall(workdir)
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                extracted.append(workdir / name)
    return extracted


def _ensure_bronze_table(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name()} (
            bike_id STRING,
            rent_dt STRING,
            rent_station_no STRING,
            rent_station_name STRING,
            rent_hold STRING,
            return_dt STRING,
            return_station_no STRING,
            return_station_name STRING,
            return_hold STRING,
            use_min STRING,
            use_distance_m STRING,
            user_class_cd STRING,
            sex_cd STRING,
            birth_year STRING,
            rent_station_id STRING,
            return_station_id STRING,
            bike_se_cd STRING,
            rent_date_partition STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (rent_date_partition)
        """
    )
    # write.distribution-mode=hash로 지정해야 Iceberg가 "직접" 분산+정렬 요구사항을
    # Spark 실행계획에 주입한다. 우리가 수동으로 repartition/sort를 해줘도 Iceberg는
    # 그걸 신뢰하지 않고 안전하게 FanoutWriter(파티션별 파일을 동시에 여러 개 열어둠)를
    # 쓰는 게 실측으로 확인됨 -> 이 설정 없이는 여러 날짜가 한 태스크에 몰릴 때 OOM 위험.
    # 이미 존재하는 테이블에도 매번 적용되도록(ALTER는 idempotent) run() 시작마다 호출한다.
    spark.sql(
        f"ALTER TABLE {_table_name()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )


def _derive_date_partition(df, source_col: str = "rent_dt"):
    """RENT_DT 형식 편차(YYYYMMDD / YYYY-MM-DD 등)에 대응해 숫자만 추출 후 YYYY-MM-DD로 정규화."""
    digits = F.regexp_replace(F.col(source_col), r"[^0-9]", "")
    return F.concat_ws(
        "-",
        F.substring(digits, 1, 4),
        F.substring(digits, 5, 2),
        F.substring(digits, 7, 2),
    )


def _process_one_csv(spark, csv_path: Path, staging_dir: Path):
    utf8_path = staging_dir / f"{csv_path.stem}.utf8.csv"
    convert_result = convert_euckr_file_to_utf8(csv_path, utf8_path)
    if convert_result["dropped_bytes"] > 0:
        logger.warning("파일 %s: 손상 바이트 %d개 폐기", csv_path.name, convert_result["dropped_bytes"])

    raw_df = spark.read.option("header", "true").csv(str(utf8_path))
    actual_columns = raw_df.columns

    if not is_rental_history_file(actual_columns):
        # 대여이력 컬럼과 거의 안 겹침 -> 다른 팀원 데이터셋(고장신고 등)이 같은 폴더에
        # 섞여 들어온 것으로 판단. 원본 업로드도 하지 않고(잘못된 lineage 방지) 조용히 스킵.
        raise NotThisDatasetError(f"대여이력 스키마와 겹치는 컬럼이 거의 없음 (실제 컬럼: {actual_columns})")

    # 이 시점부터는 대여이력 데이터셋으로 확인됨 -> 원본을 raw zone에 그대로 보존 (lineage / 재처리 대비)
    landing_date = datetime.utcnow().strftime("%Y-%m-%d")
    upload_file(csv_path, config.SETTINGS.raw_bucket, f"raw/rental_history/_landing/{landing_date}/{csv_path.name}")

    validate_and_report(actual_columns)  # 필수 컬럼 누락 시 SchemaValidationError -> 호출부에서 스킵 처리

    select_exprs = build_select_exprs(actual_columns)
    mapped_df = raw_df.select(*select_exprs)

    bronze_df = (
        mapped_df.withColumn("rent_date_partition", _derive_date_partition(mapped_df))
        .withColumn("source_file", F.lit(csv_path.name))
        .withColumn("ingested_at", F.current_timestamp())
    )
    return bronze_df


def run(input_dir: str) -> None:
    # Iceberg(warehouse_bucket)와 원본 랜딩(raw_bucket) 버킷 모두 Spark 세션 생성 전에
    # 존재해야 한다. LocalStack은 버킷을 자동으로 만들어주지 않아서, 이 호출이 없으면
    # CREATE TABLE 단계에서 "NoSuchBucket"으로 실패한다.
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-backfill-rental-history")
    _ensure_bronze_table(spark)

    # 로컬 테스트 시 폴더 전체(수 GB) 대신 파일명 패턴으로 일부만 골라 처리할 수 있게 함
    # 예) INPUT_FILE_PATTERN="*2601*" -> 2026년 1월치 파일 1개만 백필
    file_pattern = os.getenv("INPUT_FILE_PATTERN", "*")
    input_files = sorted(Path(input_dir).glob(file_pattern))
    if not input_files:
        logger.error("입력 디렉토리에 파일이 없습니다: %s (패턴: %s)", input_dir, file_pattern)
        sys.exit(1)

    total_files, total_rows, failed_files, skipped_files = 0, 0, [], []

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        for raw_path in input_files:
            for csv_path in _unzip_if_needed(raw_path, workdir):
                total_files += 1
                try:
                    # write.distribution-mode=hash를 테이블 속성으로 지정해뒀으므로
                    # Iceberg가 스스로 분산+정렬을 처리한다. 우리가 직접 repartition/sort를
                    # 하면 Iceberg 입장에서는 신뢰할 수 없는 정렬이라 중복 셔플만 될 뿐이라 뺐다.
                    bronze_df = _process_one_csv(spark, csv_path, workdir).cache()
                    row_count = bronze_df.count()

                    # 재실행 시 동일 날짜 파티션만 덮어써서 멱등성 보장 (다른 파티션엔 영향 없음)
                    bronze_df.writeTo(_table_name()).overwritePartitions()
                    bronze_df.unpersist()

                    total_rows += row_count
                    logger.info("적재 완료: %s (%d행)", csv_path.name, row_count)
                except NotThisDatasetError as e:
                    # 실패가 아니라 정상 스킵 - 입력 폴더에 다른 데이터셋 파일이 섞여 있는 경우
                    logger.info("대여이력 데이터셋이 아닌 것으로 보여 스킵: %s (%s)", csv_path.name, e)
                    skipped_files.append(csv_path.name)
                    continue
                except SchemaValidationError as e:
                    logger.error("스키마 검증 실패, 파일 스킵: %s (%s)", csv_path.name, e)
                    failed_files.append(csv_path.name)
                    continue  # 안전하게 실패: 이 파일만 스킵하고 나머지는 계속 처리

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
        sys.exit(1)  # non-zero exit -> Airflow가 실패로 감지 (스킵은 실패로 취급하지 않음)


if __name__ == "__main__":
    run(os.getenv("INPUT_DIR", "./raw_downloads"))