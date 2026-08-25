"""
Bronze 초기 적재 잡 - 서울시 공공자전거 대여이력 (OA-15182)

실행 방식: 파일 목록(INPUT_FILES, JSON 배열)을 받아 하나의 프로세스(=하나의 Spark
세션/JVM) 안에서 파일마다 순차로 처리하고 종료한다. 파일 다운로드와 대상 목록 나열은
jobs/list_input_files.py가 먼저 수행하고, DAG가 그 목록을 배치로 잘라(dag_common.
chunk_list, #249) Dynamic Task Mapping으로 배치마다 이 스크립트를 별도 프로세스로
실행한다.

#249: 예전에는 파일 하나 = JVM 하나 = EMR Serverless JobRun 하나였다. 파일이
수십 개면 JobRun 시작 오버헤드(애플리케이션 큐잉, 드라이버 초기화)도 수십 번
반복됐다 - 배치로 묶어 오버헤드를 (파일 수 / 배치 크기)번으로 줄인다. 대신 파일
단위 메모리 안전성은 그대로 유지한다: 파일마다 독립된 TemporaryDirectory를 열고
닫아서, 반기 파일(최대 700MB급)을 여러 개 순회해도 임시 파일/힙이 파일 하나 분량
이상 누적되지 않는다(기존 "파일 하나 = JVM 하나"의 격리 취지를 JVM 안에서 재현).
전체 파일을 하나의 DataFrame으로 합치는 일은 없다 - 파일마다 읽고, 쓰고, unpersist
한 뒤 다음 파일로 넘어간다.

멱등성: 재실행 시 동일 날짜(rent_date_partition) 파티션을 덮어쓴다
       (Iceberg overwritePartitions) -> 같은 입력으로 몇 번 돌려도 결과가 같다.

안전한 실패: 배치 안의 한 파일에서 스키마 검증에 실패하는 CSV가 있어도(압축 파일
            하나가 여러 CSV로 풀리는 경우) 그 CSV만 스킵하고 나머지 파일까지 계속
            처리한다. 실패한 파일이 하나라도 있었으면 배치 전체를 종료 코드 1로
            끝내(Airflow가 실패를 감지해 배치 태스크를 재시도하도록) 실패 파일
            목록을 로그에 남긴다. 재시도는 배치 단위다 - 같은 배치를 통째로
            다시 돌리면 이미 성공한 파일도 재처리되지만 overwritePartitions가
            멱등이라 결과는 같고, 시간만 더 든다(배치 크기를 작게 잡을수록 이
            비용이 줄어든다 - dag_common.chunk_list 문서 참고).

전달 방식(entryPointArguments 우선, INPUT_FILES 환경변수는 하위호환용, #255):
EMR Serverless의 sparkSubmitParameters(--conf ...ENV=값)는 자체 파서를 쓰는데, 셸이
아니라서 공백/특수문자에서 토큰을 잘라먹는다(#218에서 실측 확인). entryPointArguments는
JSON 리스트를 그대로 각 토큰으로 드라이버에 전달하므로 이 파싱 문제 자체가 없다 - DAG는
이제 이 잡을 entryPointArguments(--input-files-json)로 부른다. INPUT_FILES 환경변수는
로컬 BashOperator(파일당 프로세스) 등 기존 호출부가 그대로 동작하도록 fallback으로 남긴다.

사용법:
    python -m jobs.initial_load_rental_history --input-files-json '["./raw_downloads/2601.csv"]'
    INPUT_FILES='["./raw_downloads/2601.csv"]' python -m jobs.initial_load_rental_history
"""
import argparse
import json
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
from common.s3_utils import (
    download_file,
    ensure_bucket,
    split_s3_uri,
    to_spark_readable_path,
    upload_file,
)
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

    csv_source = to_spark_readable_path(
        utf8_path,
        config.SETTINGS.raw_bucket,
        "raw/rental_history/_utf8_staging",
    )
    raw_df = spark.read.option("header", "true").csv(csv_source)
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


def _process_one_input_file(spark, input_file: str) -> tuple[int, bool, bool]:
    """input_file 하나를 처리한다. (row_count, failed, skipped)를 반환한다.

    이 TemporaryDirectory는 파일 1개 처리 범위로 한정된다 - 압축 해제 결과물/UTF-8
    변환본이 여기 쌓이는데, 이 함수가 반환하는 순간(정상 종료든 예외든 with 블록이
    닫히며) 파일 하나 분량만 디스크에 남았다가 즉시 정리된다. 배치 안에 파일이
    여러 개 있어도 동시에 두 파일 분량이 누적되지 않는다.
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

        for csv_path in unzip_if_needed(raw_path, workdir):
            try:
                # write.distribution-mode=hash를 테이블 속성으로 지정해뒀으므로
                # Iceberg가 스스로 분산+정렬을 처리한다. 우리가 직접 repartition/sort를
                # 하면 Iceberg 입장에서는 신뢰할 수 없는 정렬이라 중복 셔플만 될 뿐이라 뺐다.
                bronze_df = _process_one_csv(spark, csv_path, workdir).cache()
                csv_row_count = bronze_df.count()

                # 재실행 시 동일 날짜 파티션만 덮어써서 멱등성 보장 (다른 파티션엔 영향 없음)
                bronze_df.writeTo(_table_name()).overwritePartitions()
                bronze_df.unpersist()

                row_count += csv_row_count
                logger.info("적재 완료: %s (%d행)", csv_path.name, csv_row_count)
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

    return row_count, failed, skipped


def run(input_files: list[str]) -> None:
    # Iceberg(warehouse_bucket)와 원본 랜딩(raw_bucket) 버킷 모두 Spark 세션 생성 전에
    # 존재해야 한다. LocalStack은 버킷을 자동으로 만들어주지 않아서, 이 호출이 없으면
    # CREATE TABLE 단계에서 "NoSuchBucket"으로 실패한다.
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-initial-load-rental-history")
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
        # non-zero exit -> Airflow가 배치 태스크 실패로 감지해 배치 전체를 재시도한다
        # (스킵은 실패로 취급하지 않음). 어떤 파일이 실패했는지는 위 로그로 특정할 수 있다.
        sys.exit(1)


def _resolve_input_files(argv: list[str] | None = None) -> list[str]:
    """entryPointArguments(--input-files-json)를 우선하고, 없으면 INPUT_FILES 환경변수로
    내려간다(#255) - 로컬 BashOperator 등 기존 호출부와의 하위호환용."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-files-json", default=None)
    args = parser.parse_args(argv)

    raw_input_files = args.input_files_json or os.getenv("INPUT_FILES")
    if not raw_input_files:
        logger.error(
            "사용법: python -m jobs.initial_load_rental_history "
            "--input-files-json '[\"./raw_downloads/2601.csv\"]' "
            "(또는 INPUT_FILES='[\"./raw_downloads/2601.csv\"]')"
        )
        sys.exit(1)
    return json.loads(raw_input_files)


if __name__ == "__main__":
    run(_resolve_input_files())
