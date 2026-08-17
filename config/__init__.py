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

    # ---- S3 / 객체 스토리지 ----
    # LocalStack 기본 포트는 4566. AWS에서는 이 값이 아예 안 쓰임(get_s3_client가 무시).
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    s3_region: str = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    s3_access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "test")
    s3_secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    raw_bucket: str = os.getenv("RAW_BUCKET", "ttareungyi-raw")
    warehouse_bucket: str = os.getenv("WAREHOUSE_BUCKET", "ttareungyi-warehouse")

    # ---- Iceberg 카탈로그 ----
    # local: hadoop catalog (객체 스토리지 기반) / aws: glue
    iceberg_catalog_type: str = os.getenv("ICEBERG_CATALOG_TYPE", "hadoop")
    iceberg_catalog_name: str = os.getenv("ICEBERG_CATALOG_NAME", "bike_catalog")
    iceberg_warehouse_path: str = os.getenv(
        "ICEBERG_WAREHOUSE_PATH", f"s3a://{warehouse_bucket}/warehouse"
    )

    # ---- 서울 열린데이터광장 Open API (실제 스펙 확인됨, 2026-08-11) ----
    # 서비스명(tbCycleRentData 등)은 데이터셋마다 고정값이라 env로 안 빼고
    # common/api_client.py에 상수로 박아둔다 - KEY/BASE_URL/TYPE/PAGE_SIZE만 환경설정.
    seoul_api_key: str = os.getenv("SEOUL_API_KEY", "sample")
    seoul_api_base_url: str = os.getenv("SEOUL_API_BASE_URL", "http://openapi.seoul.go.kr:8088")
    seoul_api_type: str = os.getenv("SEOUL_API_TYPE", "json")
    api_page_size: int = int(os.getenv("API_PAGE_SIZE", "1000"))  # 1회 최대 추정치

    # ---- 서울 열린데이터광장 파일 다운로드 (백필용, 반기/월별 CSV·XLSX) ----
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
