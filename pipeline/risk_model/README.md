# risk_model

위험도 Feature 생성

- 위험도 계산 결과 생성
- 우선순위 산정에 필요한 입력 데이터(dim_bike, bike_location, station_active, fact_station_inventory)는 이 폴더의 책임이 아니다 - `pipeline/collection_priority`가 그쪽을 담당하고, 여기는 그 결과를 받아 위험도를 추론하고 대여중단 여부를 결정하는 job만 둔다

## jobs

- `build_bike_features_daily.py`: `silver.rental_history` + `silver.failure_report` -> `gold.bike_features_daily` (하루치 통째로 재계산, OVERWRITE + PyDeequ 검증)
  - 컬럼: `snapshot_date`(파티션), `bike_id`, `trips`, `dist_km`, `instant_ret`, `fail_150d`, `days_since_fail`, `days_since_last_rent`, `trend_ratio`
  - 기준일 이전 14일 rolling window로 집계. 피처 로직은 `pipeline/train_risk_model/features.py`의 `build_features_for_inference()`를 그대로 호출한다(학습·추론 공유, train-serving skew 방지) - `dim_bike`처럼 누적 처리 아님, 워터마크 없음
  - `dag_gold_dim_fact` 산출물이 아니라 이 폴더가 직접 만든다 - 순수 추론 입력이라 risk_model 스코프
- `run_risk_scoring_model.py`: `pipeline/train_risk_model`의 `registry.get_champion()` + `train.score()`를 그대로 불러 쓰는 추론 라이브러리
  - champion은 `registry.json`에서 로드하고(`{model_root}/registry.json`), 승격 후보는 학습 쪽(`risk_model_train_dag.py`)에서 항상 `models.primary`(lgbm)로 고정돼 있어 여기서는 model_type 분기를 신경 쓸 필요가 없다
  - `risk_score`는 모델이 출력한 원본 확률(0~1)에 100을 곱한 값(0~100), `risk_grade`는 95/99 컷오프로 Normal/Warning/Critical 3등급 (이 컷오프 값은 champion의 확률 분포에 맞춰 재검증이 필요할 수 있음 - 재학습마다 드리프트 가능)
  - 독립 job이 아니라 `build_fact_bike_risk.py`가 라이브러리로 불러 쓴다
- `build_fact_bike_risk.py`: `gold.bike_features_daily` -> `gold.fact_bike_risk` (하루치 통째로 재계산, OVERWRITE + PyDeequ 검증)
  - 컬럼: `snapshot_date`(파티션), `bike_id`, `risk_score`, `risk_grade`, `model_version`
  - `bikeman_action` 최신 이벤트가 "수거"(미배치)인 자전거만 제외 - 대여중단 상태는 재고 상황에 따라 다시 바뀔 수 있어 매일 재포함
  - 날짜 범위를 누적 처리하지 않아 워터마크 없음, `SNAPSHOT_DATE` 환경변수(기본값 오늘)로 대상일 지정
- `build_fact_bike_decision.py`: `gold.fact_bike_risk` + `gold.fact_station_inventory` -> `gold.fact_bike_decision` (OVERWRITE + PyDeequ 검증)
  - 컬럼: `snapshot_date`(파티션), `bike_id`, `action`(`대여중단`/`보류` 2종 - `수거`는 이 job 스코프 밖)
  - 대여소별 `suspendable_bike_cnt = max(0, bike_cnt - target_bike_cnt)`, `warning_available_cnt = suspendable_bike_cnt - critical_cnt`
  - Critical은 무조건 대여중단, Warning은 대여소 내 `risk_score` 랭킹이 `warning_available_cnt` 이내일 때만 대여중단

## Airflow

- DAG: `dag_risk_decision` (`airflow/dags/gold_risk_decision_dag.py`)
  - `dag_gold_dim_fact` 완료 대기(`ExternalTaskSensor`) -> cold start 분기 -> `run_risk_scoring_model`(=`build_fact_bike_risk.py`) -> `build_fact_bike_decision`
  - `build_bike_features_daily`는 Silver만 있으면 되므로 `dag_gold_dim_fact` 대기와 무관하게 병렬로 실행, `run_risk_scoring_model` 직전에 합류
  - cold start 분기(`skip_filter_first_run`/`apply_lagged_filter`)는 실제로는 같은 필터 로직을 부른다 - `bikeman_action` 기반 필터가 이력 없는 최초 실행일에도 자연히 아무도 안 걸러서 별도 분기가 필요 없기 때문

## 로컬 실행

`run_risk_scoring_model.py`는 `gold.bike_features_daily` 없이도 단독으로 동작 확인 가능하다 (모델 로드 + 가짜 입력으로 스코어링) - 단, `registry.json`에 champion이 승격돼 있어야 한다(`risk_model_train` DAG을 `dry_run=false`로 먼저 실행):

```bash
cd pipeline/risk_model
python jobs/run_risk_scoring_model.py
```

`build_bike_features_daily.py`는 Silver(`rental_history`/`failure_report`)만 있으면 된다. `build_fact_bike_risk.py`/`build_fact_bike_decision.py`는 그 결과 + Gold 테이블(`bike_location`, `fact_station_inventory`)이 있어야 동작한다:

```bash
cd pipeline/risk_model
export PYTHONPATH=../..:../../ingestion:$PYTHONPATH
set -a && source ../../ingestion/.env && set +a
SNAPSHOT_DATE=2026-08-17 python -m jobs.build_bike_features_daily
SNAPSHOT_DATE=2026-08-17 python -m jobs.build_fact_bike_risk
SNAPSHOT_DATE=2026-08-17 python -m jobs.build_fact_bike_decision
```
