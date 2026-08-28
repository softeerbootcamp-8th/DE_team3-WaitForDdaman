"""
Silver 워터마크 부트스트랩 - Bronze 초기 적재가 커버한 실제 범위에서 자동으로 계산한다.

왜 필요한가: transform_silver_rental_history.py는 자체 SILVER_RENTAL_HISTORY 워터마크로
"어디까지 처리했는지"를 관리한다. 이 워터마크가 없으면 config.BACKFILL_START_DATE(기본
2015-01-01)부터 시작하려 드는데, 실제 Bronze 데이터는 초기 적재가 커버하는 기간(예:
2026-06-01~)부터만 존재한다. 그 갭만큼 Silver가 존재하지도 않는 날짜를 헛되이 훑는다.

이 잡은 Bronze 테이블의 실제 MIN(partition)을 직접 읽어서 그 전날을 Silver 워터마크로
찍는다 - 사람이 날짜를 눈으로 세어 set_watermark.py에 넘기던 걸 없앤다.

⚠️ 원래는 초기 적재 직후 1회만 실행해야 하는 잡이었다 - daily_batch처럼 매일 도는 잡에
넣으면 정상 진행 중인 워터마크를 매번 (bronze MIN - 1일)로 되돌려버리기 때문이다.
initial_load_dag.py는 재트리거가 가능한 DAG(수동 1회성이지만 파일을
빠뜨렸을 때 등 다시 트리거할 수 있음)라, "1회만"을 사람이 지키는 것에만 의존할 수 없다 -
그래서 이미 워터마크가 설정돼 있으면(재실행으로 판단) 아무것도 안 하고 건너뛴다.
read_watermark()는 없으면 backfill_start_date로 폴백해버려서 "진짜 없음"과 "폴백값"을
구분 못 하므로, get_json()으로 키 존재 자체를 직접 확인한다.
initial_load_dag.py에서만 태스크로 연결한다.

Spark를 안 쓴다 (2026-08-22 변경) - MIN 하나 뽑는데 Spark JVM을 통째로 띄울 필요가
없다. pyiceberg로 매니페스트(파티션 메타데이터)만 읽어서 훨씬 가볍게 조회한다
(common/iceberg_catalog.py 참고) - ICEBERG_CATALOG_TYPE=jdbc 전용.

사용법:
    DATASET=rental_history python -m jobs.bootstrap_silver_watermark
"""
import logging
import os
import sys
from datetime import date, timedelta

import config
from common.iceberg_catalog import build_iceberg_catalog
from common.s3_utils import get_json
from common.watermark import write_watermark
from config.watermark_keys import SILVER_FAILURE_REPORT, SILVER_RENTAL_HISTORY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 데이터셋명 -> (Bronze 테이블, 파티션 컬럼, Silver 워터마크 키)
# bikeman_event은 여기 없다 - 파일 백필 대상이 아니라 "서비스 시작일" 기준이라
# 이 DAG(파일 백필) 범위 밖이다.
#
# failure_report는 #288부터 대상이다. 전량 재처리(overwrite_all) 시절엔 Silver 워터마크가
# Bronze 값의 미러라 부트스트랩할 게 없었지만, 확정 구간 증분으로 바뀐 뒤에는 하한이
# 필요하다 - 없으면 read_watermark가 backfill_start_date(기본 2015-01-01)로 폴백해
# 데이터가 없는 6년치를 구간에 넣는다.
DATASETS = {
    "rental_history": ("bronze.rental_history", "rent_date_partition", SILVER_RENTAL_HISTORY),
    "failure_report": ("bronze.failure_report", "reg_date_partition", SILVER_FAILURE_REPORT),
}


def run(dataset: str) -> None:
    if dataset not in DATASETS:
        print(f"알 수 없는 DATASET: {dataset} (가능한 값: {list(DATASETS.keys())})")
        sys.exit(1)

    table_name, partition_col, watermark_key = DATASETS[dataset]
    settings = config.SETTINGS

    # get_json()으로 키 존재 자체를 직접 본다 - read_watermark()는 없으면 backfill_start_date로
    # 폴백해서 "이미 설정됨"과 "설정된 적 없음"을 구분할 수 없다.
    if get_json(settings.raw_bucket, watermark_key) is not None:
        logger.info(
            "%s: Silver 워터마크가 이미 설정돼 있음 - 재실행으로 판단해 건너뜀 "
            "(최초 1회만 계산해야 하는 값이라 다시 트리거해도 덮어쓰지 않음)",
            watermark_key,
        )
        return

    catalog = build_iceberg_catalog()
    table = catalog.load_table(table_name)
    # table.inspect.partitions()는 매니페스트(파티션 메타데이터)만 읽는다 - 실제
    # 데이터 파일을 스캔하지 않아 Spark로 SELECT MIN(...)하는 것보다 훨씬 가볍다.
    parts = table.inspect.partitions().to_pandas()

    # rent_dt가 NULL인 원본 행은 파티션 컬럼이 빈 문자열("")로 떨어진다(_derive_date_partition의
    # concat_ws가 전부 NULL이면 NULL이 아니라 ""를 반환함). ""가 실제 날짜보다 사전식으로
    # 작아서 걸러내지 않으면 MIN이 그 값을 집어 "데이터 없음"으로 오판하게 된다
    # (실측: rental_history에서 malformed 행 2개로 인해 재현됨, 2026-08-20).
    min_date_str = None
    if not parts.empty:
        values = parts["partition"].apply(lambda p: p[partition_col])
        values = values[values.notna() & (values != "")]
        if not values.empty:
            min_date_str = values.min()

    if not min_date_str:
        logger.error("%s: Bronze 테이블에 데이터가 없음 - 백필을 먼저 실행하세요.", table_name)
        sys.exit(1)

    min_date = date.fromisoformat(min_date_str)
    watermark_date = min_date - timedelta(days=1)

    write_watermark(watermark_date, watermark_key=watermark_key)
    logger.info(
        "%s: Bronze 최초 날짜=%s -> Silver 워터마크=%s로 설정",
        dataset, min_date, watermark_date,
    )


if __name__ == "__main__":
    run(os.environ["DATASET"])
