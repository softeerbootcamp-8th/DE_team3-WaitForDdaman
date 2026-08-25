"""
Bronze 초기 적재 - 입력 파일 목록 나열 (없으면 다운로드 + 압축 해제까지 포함)

Dynamic Task Mapping에서 파일 단위 태스크를 만들기 전에, 먼저 원본 파일이 로컬에
없으면 열린데이터광장에서 받아 채워 넣는다(ensure_backfill_files). 서울시 공공데이터는
반기/월별 csv 6개 외에 연도별 zip(최대 12개월치)도 섞여 있는데, 여기서 압축을 미리
풀어 개별 월 단위 csv로 펼쳐서 목록을 만든다(expand_archives) - 안 그러면 zip 하나가
Dynamic Task Mapping에서 파일 1개로 잡혀서, 그 프로세스 하나가 여전히 최대 12개월치를
순회 처리하게 되어 "파일 하나=독립 JVM" 취지가 큰 연도 zip에서는 실현되지 않는다.

원본 zip은 지우지 않는다. 지우면 다음 실행 때 ensure_backfill_files가 "이미 있음"을
파일명으로만 판단하다가 그 zip을 다시 통째로 재다운로드하게 된다(실측 확인). zip은
그대로 두고, 압축 해제는 로컬 디스크 작업이라 매번 다시 해도 저렴하므로 재실행 시
같은 결과를 다시 만들어내게 둔다. 최종 목록에는 압축 해제된 개별 파일만 담고
원본 zip 경로 자체는 포함하지 않는다.

목록은 JSON 배열로 stdout 마지막 줄에 출력한다. BashOperator의 do_xcom_push는 stdout의
마지막 줄만 XCom으로 넘기므로, 로깅은 전부 logger(기본 stderr)로만 하고 print()는
이 한 줄에서만 쓴다.

set_watermark.py와 동일하게 DATASET 환경변수로 대상을 분기한다 - 잡 하나로 두
소스(rental_history/failure_report)를 모두 지원한다.

사용법:
    DATASET=rental_history INPUT_DIR=./raw_downloads INPUT_FILE_PATTERN="*" python -m jobs.list_input_files
    DATASET=failure_report INPUT_DIR=./raw_downloads INPUT_FILE_PATTERN="*2601*" python -m jobs.list_input_files
"""
import json
import logging
import os
import re
import sys
from pathlib import Path

import config
from common.file_downloader import ensure_backfill_files
from common.file_utils import expand_archives
from common.s3_utils import ensure_bucket, upload_file_if_changed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 데이터셋명 -> 열린데이터광장 데이터셋 ID. jobs/initial_load_*.py가 원래 각자 하드코딩해
# 부르던 것을 여기 한 곳으로 모았다.
_DATASET_IDS = {
    "rental_history": lambda: config.SETTINGS.seoul_dataset_id_rental_history,
    "failure_report": lambda: config.SETTINGS.seoul_dataset_id_breakdown_report,
}


def run(dataset: str, input_dir: str, file_pattern: str) -> list[str]:
    if dataset not in _DATASET_IDS:
        logger.error("알 수 없는 DATASET: %s (가능한 값: %s)", dataset, list(_DATASET_IDS.keys()))
        sys.exit(1)

    input_path = Path(input_dir)
    ensure_backfill_files(_DATASET_IDS[dataset](), input_path, file_pattern)

    raw_files = sorted(input_path.glob(file_pattern))
    input_files = sorted(expand_archives(raw_files, input_path))
    if not input_files:
        logger.error("입력 디렉토리에 파일이 없습니다: %s (패턴: %s)", input_dir, file_pattern)
        sys.exit(1)

    if config.SETTINGS.env == "aws":
        ensure_bucket(config.SETTINGS.raw_bucket)
        uris = []
        for path in input_files:
            # 원본 파일명(한글/공백 포함 가능 - 서울 열린데이터광장 반기 파일 등)을 그대로
            # 쓰면, 이 URI가 나중에 EMR Serverless의 sparkSubmitParameters(--conf
            # ...INPUT_FILE=<uri>)로 전달될 때 문제가 된다. dag_common.py가 shlex.quote()로
            # 감싸지만, EMR Serverless의 파서는 셸이 아니라서 그 따옴표 규칙을 지키지 않고
            # 공백에서 토큰을 잘라먹는다(실측: 2026-08-24, "서울시 공공자전거 고장신고
            # 내역_2015_2020.10.xlsx" 파일에서 INPUT_FILE이 드라이버에 전달되지 않음).
            # 스테이징 업로드 시점에 ASCII로 안전한 이름으로 바꿔서 이 문제 자체를 없앤다 -
            # 파일 내용은 이름과 무관하므로 안전하다.
            # deterministic key(파일명 기반) + upload_file_if_changed로 멱등하게 만든다 -
            # 재실행 시 스테이징에 같은 내용의 파일이 이미 있으면 재업로드하지 않고 그대로
            # 재사용하고, 로컬 원본이 바뀐 경우(재다운로드로 갱신된 경우)에만 덮어쓴다.
            safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", path.name)
            key = f"raw/{dataset}/_initial_load_staging/{safe_name}"
            upload_file_if_changed(path, config.SETTINGS.raw_bucket, key)
            uris.append(f"s3://{config.SETTINGS.raw_bucket}/{key}")
        return uris

    return [str(p) for p in input_files]


if __name__ == "__main__":
    files = run(
        os.getenv("DATASET", ""),
        os.getenv("INPUT_DIR", "./raw_downloads"),
        os.getenv("INPUT_FILE_PATTERN", "*"),
    )
    logger.info("발견된 입력 파일 %d개", len(files))
    print(json.dumps(files))
