"""
워터마크 키 상수 모음
"""

BRONZE_RENTAL_HISTORY = "_meta/watermark/rental_history.json"
BRONZE_FAILURE_REPORT = "_meta/watermark/failure_report.json"
BIKEMAN_EVENT = "_meta/watermark/bikeman_event.json"
SILVER_RENTAL_HISTORY = "_meta/watermark/silver_rental_history.json"
SILVER_FAILURE_REPORT = "_meta/watermark/silver_failure_report.json"
SILVER_BIKEMAN_ACTION = "_meta/watermark/silver_bikeman_action.json"
GOLD_DIM_BIKE = "_meta/watermark/gold_dim_bike.json"

# 데이터셋명 -> 워터마크 키. jobs/set_watermark.py(수동 워터마크 설정)와
# jobs/check_watermark_staleness.py(정체 감지)가 각자 별도 딕셔너리를 들고 있다가
# 서로 어긋나는 문제(Issue #191)가 있었다 - 이 딕셔너리 하나를 두 잡이 그대로
# import해서 쓰게 해서, 새 데이터셋을 추가할 때 여기 한 곳만 갱신하면 되게 한다.
# station_master/station_active는 날짜 파라미터 없는 "항상 전체 스냅샷" API라
# 증분 워터마크 개념이 없으므로 포함하지 않는다.
DATASET_WATERMARK_KEYS = {
    "rental_history": BRONZE_RENTAL_HISTORY,
    "failure_report": BRONZE_FAILURE_REPORT,
    "bikeman_event": BIKEMAN_EVENT,
    "silver_rental_history": SILVER_RENTAL_HISTORY,
    "silver_failure_report": SILVER_FAILURE_REPORT,
    "silver_bikeman_action": SILVER_BIKEMAN_ACTION,
    "gold_dim_bike": GOLD_DIM_BIKE,
}
