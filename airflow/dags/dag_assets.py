"""
Asset(구 Dataset) 정의 모음 - Bronze/Silver DAG가 공통으로 import해서
이름 불일치로 인한 "조용히 안 트리거되는 사고"를 방지한다.

Airflow는 DAG 파일이 있는 폴더를 sys.path에 넣어서 파싱하므로,
airflow/dags/ 안의 다른 DAG 파일에서 `from dag_assets import ...`로
그냥 가져다 쓸 수 있다 (별도 패키징/설치 불필요).
"""
from airflow.sdk import Asset

# bikeman_event Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_bikeman_event 태스크가
# outlets로 이 Asset을 갱신하고, silver_bike_man_action_daily_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
BIKEMAN_EVENT_BRONZE = Asset("bikeman_event_bronze")
