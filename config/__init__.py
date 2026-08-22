"""
공통 환경설정

설계 원칙: 로컬 개발(LocalStack) <-> AWS 배포 전환 시 코드를 건드리지 않고
환경변수만 바꿔서 동작하게 한다.

    APP_ENV=local  -> LocalStack S3 + Hadoop Iceberg Catalog
    APP_ENV=aws    -> 실제 S3 + Glue Data Catalog

ingestion/staging/pipeline 세 서비스가 전부 이 값을 참조해야 해서
(ingestion/common 안에 두면 다른 서비스가 PYTHONPATH 트릭으로만 접근 가능했음)
최상위 config/ 패키지로 뺐다.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # ---- 실행 환경 ----
    env: str = os.getenv("APP_ENV", "local")  # local | aws

    # ---- Spark 로컬 실행 튜닝 여부 ----
    # env(local/aws)와는 독립된 축이다. env는 "S3 엔드포인트/자격증명을 LocalStack용으로
    # 쓰느냐"만 결정하고(spark_session.py 참고), 이 값은 "지금 이 프로세스가 자원이 작은
    # 로컬 머신에서 도느냐"만 결정한다. Glue 권한이 없어 실 S3 + Hadoop Catalog를 쓰면서도
    # 컨테이너는 여전히 로컬 macOS/CI 머신에서 도는 조합(APP_ENV=aws + 로컬 실행)이 실제로
    # 있어서 분리했다 - 예전에는 env=="local"에 로컬 메모리 튜닝(local[2], driver 6g 등)까지
    # 같이 묶여 있어서, 실 S3로 전환하면(APP_ENV=aws) 이 튜닝이 통째로 빠지고 반기 CSV
    # (최대 700MB대) 처리 중 OOM이 재현됐다.
    # 기본값은 APP_ENV=local이면 true, APP_ENV=aws면 false로 기존 동작과 동일하게 유지하되,
    # SPARK_LOCAL_EXECUTION을 명시하면 env와 무관하게 강제로 켜고 끌 수 있다.
    spark_local_execution: bool = os.getenv(
        "SPARK_LOCAL_EXECUTION", "true" if os.getenv("APP_ENV", "local") == "local" else "false"
    ).strip().lower() in ("1", "true", "yes")

    # ---- S3 / 객체 스토리지 ----
    # LocalStack 기본 포트는 4566. AWS에서는 이 값이 아예 안 쓰임(get_s3_client가 무시).
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    s3_region: str = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    s3_access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "test")
    s3_secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    raw_bucket: str = os.getenv("RAW_BUCKET", "ttareungyi-raw")
    warehouse_bucket: str = os.getenv("WAREHOUSE_BUCKET", "ttareungyi-warehouse")

    # ---- Iceberg 카탈로그 ----
    # hadoop: 객체 스토리지 경로 기반(별도 DB 불필요) / glue: AWS Glue Data Catalog /
    # jdbc: DB(Postgres)에 "테이블 -> 최신 metadata.json 위치" 포인터를 저장 - Spark
    # 없이도 pyiceberg 등으로 그 포인터를 직접 조회할 수 있게 하려고 도입한다
    # (hadoop catalog는 파일 규칙 기반이라 pyiceberg가 못 읽음 - spark_session.py 참고).
    iceberg_catalog_type: str = os.getenv("ICEBERG_CATALOG_TYPE", "hadoop")
    iceberg_catalog_name: str = os.getenv("ICEBERG_CATALOG_NAME", "bike_catalog")
    iceberg_warehouse_path: str = os.getenv(
        "ICEBERG_WAREHOUSE_PATH", f"s3a://{warehouse_bucket}/warehouse"
    )
    # jdbc 카탈로그 전용 - 데이터/메타데이터 파일은 그대로 iceberg_warehouse_path(S3)에
    # 남고, 이 DB에는 "테이블 이름 -> 최신 metadata.json 위치" 포인터만 저장된다.
    # docker-compose의 postgres 컨테이너 안에 Airflow 메타데이터 DB(POSTGRES_DB)와
    # 분리된 별도 DB(iceberg_catalog)를 쓴다 (docker/postgres-init/ 참고).
    iceberg_jdbc_catalog_uri: str = os.getenv(
        "ICEBERG_JDBC_CATALOG_URI", "jdbc:postgresql://postgres:5432/iceberg_catalog"
    )
    # 같은 postgres 컨테이너의 계정을 그대로 재사용 (DB만 분리) - 루트 .env의
    # POSTGRES_USER/PASSWORD와 항상 같아야 하므로 별도 하드코딩 기본값을 두지 않는다.
    iceberg_jdbc_catalog_user: str = os.getenv(
        "ICEBERG_JDBC_CATALOG_USER", os.getenv("POSTGRES_USER", "airflow")
    )
    iceberg_jdbc_catalog_password: str = os.getenv(
        "ICEBERG_JDBC_CATALOG_PASSWORD", os.getenv("POSTGRES_PASSWORD", "airflow")
    )

    # ---- 서울 열린데이터광장 Open API (실제 스펙 확인됨, 2026-08-11) ----
    # 서비스명(tbCycleRentData 등)은 데이터셋마다 고정값이라 env로 안 빼고
    # common/api_client.py에 상수로 박아둔다 - KEY/BASE_URL/TYPE/PAGE_SIZE만 환경설정.
    seoul_api_key: str = os.getenv("SEOUL_API_KEY", "sample")
    seoul_api_base_url: str = os.getenv("SEOUL_API_BASE_URL", "http://openapi.seoul.go.kr:8088")
    seoul_api_type: str = os.getenv("SEOUL_API_TYPE", "json")
    api_page_size: int = int(os.getenv("API_PAGE_SIZE", "1000"))  # 1회 최대 추정치

    # ---- 서울 열린데이터광장 파일 다운로드 (백필용, 반기/월별 CSV·XLSX) ----
    # 실제 다운로드 URL(nio_download.do)에는 seq(파일별 내부 일련번호)가 필요한데
    # 파일명에서 계산할 수 없다. seoul_data_list_base_url의 목록 페이지를 먼저 긁어서
    # {파일명: seq}를 알아낸 뒤에만 seoul_data_file_base_url로 실제 다운로드가 가능하다
    # (common/file_downloader.py 참고, 실측 확인 2026-08-19).
    seoul_data_list_base_url: str = os.getenv("SEOUL_DATA_LIST_BASE_URL", "https://data.seoul.go.kr/dataList")
    seoul_data_file_base_url: str = os.getenv(
        "SEOUL_DATA_FILE_BASE_URL", "https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do"
    )
    seoul_dataset_id_rental_history: str = os.getenv("SEOUL_DATASET_ID_RENTAL_HISTORY", "OA-15182")
    seoul_dataset_id_breakdown_report: str = os.getenv("SEOUL_DATASET_ID_BREAKDOWN_REPORT", "OA-15644")
    seoul_dataset_id_station_info: str = os.getenv("SEOUL_DATASET_ID_STATION_INFO", "OA-13252")

    # ---- 워터마크 저장 위치 ----
    # 소규모 메타데이터라 별도 DB 없이 S3에 json으로 저장
    watermark_key: str = os.getenv(
        "WATERMARK_KEY", "_meta/watermark/rental_history.json"
    )

    # ---- 백필 기준 ----
    backfill_start_date: str = os.getenv("BACKFILL_START_DATE", "2015-01-01")


SETTINGS = Settings()
