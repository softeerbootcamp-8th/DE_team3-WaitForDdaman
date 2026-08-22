"""
Bronze 초기 적재 잡 - 서울시 공공자전거 대여이력 (OA-15182)

실행 방식: 파일 하나(INPUT_FILE)를 받아 독립된 프로세스(=하나의 Spark 세션/JVM)로
처리하고 종료한다. 파일 다운로드와 대상 목록 나열은 jobs/list_input_files.py가
먼저 수행한다 - Airflow DAG는 그 목록으로 Dynamic Task Mapping을 돌려 파일마다
이 스크립트를 별도 프로세스로 실행한다. 파일 하나 = JVM 하나로 격리해야, 반기
파일(최대 700MB급)을 여러 개 순회하면서 임시 파일/힙이 누적돼 OOM 나는 걸 막을 수
있다 (기존에는 폴더 전체를 세션 하나로 순회했다).

멱등성: 재실행 시 동일 날짜(rent_date_partition) 파티션을 덮어쓴다
       (Iceberg overwritePartitions) -> 같은 입력으로 몇 번 돌려도 결과가 같다.

안전한 실패: 파일 안에 스키마 검증에 실패하는 CSV가 있어도(압축 파일 하나가 여러
            CSV로 풀리는 경우) 그 CSV만 스킵하고 계속 처리한 뒤, 실패가 있었으면
            종료 코드 1로 종료한다 (Airflow가 실패를 감지할 수 있도록).

사용법:
    INPUT_FILE=./raw_downloads/2601.csv python -m jobs.initial_load_rental_history
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
from common.file_utils import unzip_if_needed
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


def run(input_file: str) -> None:
    # Iceberg(warehouse_bucket)와 원본 랜딩(raw_bucket) 버킷 모두 Spark 세션 생성 전에
    # 존재해야 한다. LocalStack은 버킷을 자동으로 만들어주지 않아서, 이 호출이 없으면
    # CREATE TABLE 단계에서 "NoSuchBucket"으로 실패한다.
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-initial-load-rental-history")
    _ensure_bronze_table(spark)

    raw_path = Path(input_file)
    if not raw_path.exists():
        logger.error("입력 파일이 없습니다: %s", input_file)
        sys.exit(1)

    total_rows, failed, skipped = 0, False, False

    # 이 TemporaryDirectory는 파일 1개 처리 범위로 한정된다 (프로세스 자체도 파일 1개만
    # 처리하고 종료한다). 압축 해제 결과물/UTF-8 변환본이 여기 쌓이는데, 프로세스가
    # 정상 종료되든 OOM으로 강제 종료되든 이 파일 하나 분량만 디스크에 남을 수 있다 -
    # 예전처럼 폴더 전체를 한 세션으로 순회하며 무한정 누적되는 구조가 아니다.
    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        for csv_path in unzip_if_needed(raw_path, workdir):
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
                # 실패가 아니라 정상 스킵 - 입력 파일이 다른 데이터셋인 경우
                logger.info("대여이력 데이터셋이 아닌 것으로 보여 스킵: %s (%s)", csv_path.name, e)
                skipped = True
            except SchemaValidationError as e:
                logger.error("스키마 검증 실패: %s (%s)", csv_path.name, e)
                failed = True  # 안전하게 실패: 이 CSV만 스킵하고 나머지(압축 안 다른 CSV)는 계속 처리
            except EncodingMismatchError as e:
                # EUC-KR/CP949가 아닌 다른 인코딩으로 추정됨 - 조용히 깨진 데이터를
                # 적재하는 대신 스키마 검증 실패와 동일하게 취급한다 (이 CSV만 스킵).
                logger.error("인코딩 불일치로 스킵: %s (%s)", csv_path.name, e)
                failed = True

    logger.info(
        "파일 처리 종료: %s, 총 %d행, 다른 데이터셋으로 스킵=%s, 스키마 실패=%s",
        input_file,
        total_rows,
        skipped,
        failed,
    )
    if failed:
        sys.exit(1)  # non-zero exit -> Airflow가 실패로 감지 (스킵은 실패로 취급하지 않음)


if __name__ == "__main__":
    input_file = os.getenv("INPUT_FILE")
    if not input_file:
        logger.error("사용법: INPUT_FILE=./raw_downloads/2601.csv python -m jobs.initial_load_rental_history")
        sys.exit(1)
    run(input_file)