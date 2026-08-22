"""
Asset(구 Dataset) 정의 모음 - Bronze/Silver DAG가 공통으로 import해서
이름 불일치로 인한 "조용히 안 트리거되는 사고"를 방지한다.

Airflow는 DAG 파일이 있는 폴더를 sys.path에 넣어서 파싱하므로,
airflow/dags/ 안의 다른 DAG 파일에서 `from dag_assets import ...`로
그냥 가져다 쓸 수 있다 (별도 패키징/설치 불필요).
"""
from airflow.sdk import Asset

# station_master Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_station_master 태스크가
# outlets로 이 Asset을 갱신하고, silver_station_master_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
STATION_MASTER_BRONZE = Asset("station_master_bronze")

# rental_history Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 rental_history TaskGroup 맨 끝
# publish_bronze_asset 태스크만 outlets로 이 Asset을 갱신하고,
# silver_rental_history_dag.py가 이 Asset의 갱신을 스케줄 트리거로 사용한다.
# 승격/확정 워터마크가 모두 끝난 뒤에만 발행돼야 하므로 앞 단계 태스크에는 절대 걸지 않는다
# (수동 catch-up DAG의 catchup_rental_history는 legacy 경로라 예외로 직접 발행한다).
RENTAL_HISTORY_BRONZE = Asset("rental_history_bronze")

# failure_report Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_failure_report 태스크가
# outlets로 이 Asset을 갱신하고, silver_failure_report_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
FAILURE_REPORT_BRONZE = Asset("failure_report_bronze")

# bikeman_event Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_bikeman_event 태스크가
# outlets로 이 Asset을 갱신하고, silver_bikeman_action_daily_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
BIKEMAN_EVENT_BRONZE = Asset("bikeman_event_bronze")

# station_active Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_station_active 태스크가
# outlets로 이 Asset을 갱신하고, silver_station_active_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
STATION_ACTIVE_BRONZE = Asset("station_active_bronze")
