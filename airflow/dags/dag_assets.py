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
# (historical reconciliation의 날짜별 복구 task는 자동 복구 전용이라 Asset을 직접 발행하지 않는다).
RENTAL_HISTORY_BRONZE = Asset("rental_history_bronze")

# rental_history Silver 승격이 성공적으로 끝났음을 나타내는 Asset.
# silver_rental_history_dag.py의 transform_silver_rental_history 태스크가 outlets로
# 이 Asset을 갱신하고, dq_rental_history_dag.py(#217)가 DQ 어써션 파이프라인의
# 트리거로 쓴다 - Silver 테이블(silver.rental_history)이 갱신된 뒤에 그 테이블을
# 대상으로 어써션을 돌려야 하므로 Bronze Asset이 아니라 이 Asset을 구독해야 한다.
RENTAL_HISTORY_SILVER = Asset("rental_history_silver")

# failure_report Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_failure_report 태스크가
# outlets로 이 Asset을 갱신하고, silver_failure_report_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
FAILURE_REPORT_BRONZE = Asset("failure_report_bronze")

# bikeman_event Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_bikeman_event 태스크가
# outlets로 이 Asset을 갱신하고, silver_bikeman_action_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
BIKEMAN_EVENT_BRONZE = Asset("bikeman_event_bronze")

# station_active Bronze 적재가 성공적으로 끝났음을 나타내는 Asset.
# bronze_daily_batch_all_sources_dag.py의 daily_batch_station_active 태스크가
# outlets로 이 Asset을 갱신하고, silver_station_active_dag.py가
# 이 Asset의 갱신을 스케줄 트리거로 사용한다.
STATION_ACTIVE_BRONZE = Asset("station_active_bronze")

# 아래 4개는 각 Silver 변환이 성공적으로 끝났음을 나타내는 Asset이다. RENTAL_HISTORY_SILVER와
# 같은 이유(dq_rental_history_dag.py 참고)로, Silver Bronze Asset이 아니라 이 Asset을
# 구독해야 그 실행 시점에 대상 Silver 테이블이 최신 상태다 - dq_silver_dag.py(Bronze 파일럿을
# rental_history 이외 4개 소스로 확장)가 트리거로 쓴다.
BIKEMAN_ACTION_SILVER = Asset("bikeman_action_silver")
STATION_MASTER_SILVER = Asset("station_master_silver")
STATION_ACTIVE_SILVER = Asset("station_active_silver")
FAILURE_REPORT_SILVER = Asset("failure_report_silver")

# 아래 6개는 각 Gold 빌드가 성공적으로 끝났음을 나타내는 Asset이다. Gold DAG(gold_dim_fact,
# gold_risk_decision)는 Bronze/Silver와 달리 Asset이 아니라 고정 스케줄 + PythonSensor
# 대기로 도는데, DQ 파이프라인(dq_gold_dag.py)만큼은 "그 Gold 테이블이 이번에 실제로
# 갱신된 시점"에 정확히 맞춰 돌아야 하므로 이 6개만 신규로 추가한다.
# gold.bike_last_action은 build_fact_station_inventory 태스크가 같이 쓰므로
# FACT_STATION_INVENTORY_GOLD를 공유한다(별도 Asset 불필요 - 항상 같은 시점에 갱신됨).
DIM_BIKE_GOLD = Asset("dim_bike_gold")
BIKE_LOCATION_GOLD = Asset("bike_location_gold")
STATION_ACTIVE_GOLD = Asset("station_active_gold")
FACT_STATION_INVENTORY_GOLD = Asset("fact_station_inventory_gold")
BIKE_FEATURES_DAILY_GOLD = Asset("bike_features_daily_gold")
FACT_BIKE_RISK_GOLD = Asset("fact_bike_risk_gold")
