"""
워터마크 수동 설정 DAG - 백필 완료 직후 1회 실행하는 유틸리티

백필이 커버한 마지막 날짜로 워터마크를 찍어야 daily_batch가 그 다음날부터 이어서 처리한다.
워터마크가 없으면 BACKFILL_START_DATE(기본 2015-01-01)부터 API로 다시 긁으려고 시도해서,
파일로 이미 채운 기간을 중복 처리하게 된다.

### 대상 데이터셋
- rental_history : Bronze 워터마크 (증분 기준 = RENT_DT)
- failure_report : Bronze 워터마크 (증분 기준 = REGDTTM)
- silver_rental_history : Silver 워터마크 (staging/jobs/transform_silver_rental_history.py)
- gold_dim_bike : Gold 워터마크 (pipeline/risk_model/jobs/build_dim_bike.py)
- bikeman_event  : 워터마크 있음 (증분 기준 = occurred_at). 백필이 아니라 서비스 시작일(6/30) 전날인 2026-06-29를 최초 1회 찍어야 함
- station_master : **해당 없음** - tbCycleStationInfo는 날짜 파라미터가 없고 매번 전체
                   스냅샷만 주므로 증분 기준이 될 컬럼 자체가 없다. 그래서 이 DAG의
                   선택지에 없다.

### 실행 방법
Airflow UI에서 "Trigger DAG w/ config"로 watermark_date / dataset을 지정해 실행한다.
데이터셋별로 워터마크를 다 찍어야 하면 dataset을 바꿔서 여러 번 트리거하면 된다.
데이터셋별로 워터마크를 다 찍어야 하면 dataset을 바꿔서 여러 번 트리거하면 된다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

INGESTION_DIR = "/opt/airflow/ingestion"
INGESTION_PYTHON = "python"


@dag(
    dag_id="set_watermark",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["utility", "watermark", "bronze"],
    params={
        "watermark_date": "2026-06-30",
        "dataset": "rental_history",  # rental_history | failure_report | silver_rental_history | gold_dim_bike
    },
    doc_md=__doc__,
)
def set_watermark():
    BashOperator(
        task_id="run_set_watermark",
        bash_command=(
            f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
            "WATERMARK_DATE='{{ params.watermark_date }}' "
            "DATASET='{{ params.dataset }}' "
            f"{INGESTION_PYTHON} -m jobs.set_watermark"
        ),
    )


set_watermark()
