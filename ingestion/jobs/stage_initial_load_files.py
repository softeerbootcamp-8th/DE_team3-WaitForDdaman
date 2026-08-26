"""
Bronze 초기 적재 - 로컬 입력 파일을 S3 초기 적재 스테이징 프리픽스로 배치 업로드한다 (#255)

jobs/list_input_files.py는 다운로드(+캐시)와 압축 해제까지만 하고 로컬 파일 경로 목록을
돌려준다. S3 업로드는 이 잡이 별도로 맡는다 - DAG(bronze_initial_load_all_sources_dag.py)
가 그 목록을 배치로 잘라(dag_common.chunk_list) Dynamic Task Mapping으로 배치마다 이
스크립트를 별도 프로세스로 실행한다. 완전히 빈 S3에서 ~40~47GB/114개 파일을 전부 올려야
하는 초기 상황을 다운로드 태스크 하나가 몇 시간 동안 순차로 처리하게 두지 않으면서도,
"파일 하나 = 태스크 하나"는 만들지 않는다 - 배치 단위로 dag_common.S3_STAGING_POOL이
허용하는 만큼만 동시에 돈다(EC2 t4g.large, 2vCPU/8GB를 로컬 MD5 해시+업로드로 과부하
시키지 않기 위함).

멱등성: 재실행/재시도 시 이미 목적지 key에 올라간 파일은 스킵한다
(common/s3_utils.reuse_or_upload_staging_file). 목적지 key가 예전(한글/공백 원본
파일명을 그대로 쓰던 #218 이전) 초기 적재에서 이미 올려져 있으면 서버사이드
CopyObject로 재사용하고, 그렇지 않을 때만 로컬 MD5 계산 후 업로드한다.

사용법:
    DATASET=rental_history \
    INPUT_FILES='["/opt/airflow/ingestion/data/rental_history/2601.csv"]' \
    python -m jobs.stage_initial_load_files
"""
import json
import logging
import os
import re
import sys
from pathlib import Path

import config
from common.s3_utils import ensure_bucket, reuse_or_upload_staging_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run(dataset: str, input_files: list[str]) -> list[str]:
    ensure_bucket(config.SETTINGS.raw_bucket)

    uris = []
    for raw_path in input_files:
        path = Path(raw_path)
        # 원본 파일명(한글/공백 포함 가능 - 서울 열린데이터광장 반기 파일 등)을 그대로
        # 쓰면, 이 URI가 나중에 EMR Serverless로 전달될 때 문제가 된다(jobs/list_input_files.py
        # 문서 참고) - 스테이징 업로드 시점에 ASCII로 안전한 이름으로 바꿔서 이 문제 자체를
        # 없앤다. legacy_key는 그 안전화 이전(#218 이전)에 실제로 쓰던 key 그대로다.
        # Hadoop/Spark는 이름이 '_' 또는 '.'로 시작하는 객체를 숨김 파일로
        # 취급해 spark.read.csv() 입력에서 제외할 수 있다. 한글 파일명을
        # underscore로 치환한 결과가 전부 '_'로 시작한 실제 장애가 있었으므로,
        # 항상 ASCII 접두사를 붙여 일반 입력 파일로 만든다.
        safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", path.name)
        safe_name = f"input_{safe_name.lstrip('._')}"
        key = f"raw/{dataset}/_initial_load_staging/{safe_name}"
        legacy_key = f"raw/{dataset}/_initial_load_staging/{path.name}"
        reuse_or_upload_staging_file(path, config.SETTINGS.raw_bucket, key, legacy_key)
        uris.append(f"s3://{config.SETTINGS.raw_bucket}/{key}")
    return uris


if __name__ == "__main__":
    dataset = os.getenv("DATASET", "")
    raw_input_files = os.getenv("INPUT_FILES")
    if not dataset or not raw_input_files:
        logger.error(
            "사용법: DATASET=rental_history "
            "INPUT_FILES='[\"./raw_downloads/2601.csv\"]' python -m jobs.stage_initial_load_files"
        )
        sys.exit(1)

    staged_uris = run(dataset, json.loads(raw_input_files))
    logger.info("스테이징 업로드 완료 %d개", len(staged_uris))
    print(json.dumps(staged_uris))
