# risk_model — 위험도 모델 학습(재학습) 파이프라인

단일 DAG `risk_model_train` (수동 트리거). 아티팩트 저장까지가 범위이고, 서빙 반영은 `dag_risk_decision`(`pipeline/risk_model`)이 담당한다 — `run_risk_scoring_model.py`가 이 문서의 "모델 아티팩트 관리" 절 계약대로 `registry.get_champion()` + `train.score()`를 그대로 불러 쓴다.

```
resolve_anchors → validate_inputs → build_train_samples → assert_train_table
                → train_and_evaluate(학습·평가·게이트·저장) → report
```

## 파이프라인 구성 (DAG 태스크 흐름)

| 태스크 | 호출 모듈/함수 | 하는 일 |
|---|---|---|
| `resolve_anchors` | [samples.py](samples.py) `detect_label_ready_max`, `resolve_anchors` | 원천 최신일에서 라벨 확정 한계를 뽑고, 학습/홀드아웃 앵커 날짜 목록을 결정론적으로 산출 |
| `validate_inputs` | (DAG 내 인라인) | 라벨 확정 여부, 대여이력 하한, 고장신고 공백 월을 검사 — 실패하면 이후 태스크를 막음 |
| `build_train_samples` | [samples.py](samples.py) `write_samples` → [features.py](features.py) `build_samples` | 앵커별 피처+라벨을 계산해 `paths.train_sample` 파티션(`anchor_type=train`/`holdout`)에 dynamic overwrite. `paths.label_pos_new`(메인지표 분모)도 같이 적재 |
| `assert_train_table` | [train.py](train.py) `assert_quality` | 행수·앵커수·양성비율·결측률 게이트. 통과 못하면 학습 자체를 막음 |
| `train_and_evaluate` | [train.py](train.py) `train_candidates` → [evaluate.py](evaluate.py) `walk_forward`/`select_best`/`apply_gate` → [registry.py](registry.py) `save_run`/`promote` | `rule_trips`/`logreg`/`lgbm` 후보 학습 → 홀드아웃 walk-forward 평가 → best 선택 → champion 대비 게이트 판정 → (dry_run이 아니면) 아티팩트 저장 + champion 승격 |
| `report` | (DAG 내 인라인) | 실행 요약 로그. 게이트 미통과는 실패가 아니라 "HELD(champion 유지)"로 기록 |

DAG params: `as_of_end`(비우면 데이터에서 자동 산출), `rolling_months`, `skip_gate`(게이트 무시하고 강제 승격), `dry_run`(아티팩트 저장/승격 생략, 파이프라인만 검증).

피처/라벨 정의는 [features.py](features.py) 하나에만 있고 학습·추론이 공유한다 — 여기서 로직이 갈라지면 train-serving skew가 생긴다. 자세한 피처/라벨 설계 근거(왜 미대여 자전거를 후보군에서 빼는지, `exclude_recent_days`가 왜 필요한지 등)는 아래 "설계 결정과 근거" 절 참조.

## 모델 아티팩트 관리

### 저장 위치와 파일

`{paths.model_root}/{run_key}/` 아래에 세 파일이 쌓인다. `run_key`는 `as_of_end`(라벨이 확정된 최신일, `YYYYMMDD`)라서 같은 날짜로 재실행하면 같은 경로를 덮어쓴다 — append 없음, 멱등.

| 파일 | 내용 |
|---|---|
| `model.joblib` | `{scaler, model, features, model_type, feature_version, model_version, as_of_end, window_days, horizon_days, exclude_recent_days, train_anchors, trained_at}` — 학습된 모델 객체 자체와 그걸 재현하는 데 필요한 메타 전부 |
| `metrics.json` | 홀드아웃 지표(`capture_at_k`, `pr_auc` 등), 일별 curve, 게이트 판정 결과, 품질 리포트, 피처 중요도 |
| `feature_spec.json` | 피처 버전, `FEATURE_COLS` 목록, 앵커 계획 — 추론 쪽에서 "이 모델이 기대하는 입력이 뭔지" 확인하는 용도 |

실제 저장된 값 예시(로컬 LocalStack 스모크 실행, `run_key=20260701`):

```json
model_type: logreg
features: [trips, dist_km, instant_ret, fail_150d, days_since_fail, days_since_last_rent, trend_ratio]
feature_version: v1
as_of_end: 2026-07-01
window_days: 14 / horizon_days: 3 / exclude_recent_days: 30
```

### champion 포인터 (`registry.json`)

`{model_root}/registry.json` 하나가 전체 레지스트리다.

```json
{
  "champion": { "model_version": "...", "artifact_uri": "s3://.../model.joblib", "metrics": {...}, "gate": {...}, ... },
  "history": [ /* 최근 50개, model_version 기준 정렬 */ ]
}
```

- [registry.py](registry.py) `promote()`가 `gate.passed`일 때만 `champion`을 옮긴다. 게이트 미통과면 `history`에만 남고 champion은 그대로 — 실행 자체는 실패가 아니다(DAG는 성공, `report` 태스크가 "HELD"로 표시).
- 첫 실행(champion 없음)은 무조건 통과.
- `dry_run=True`면 `save_run`/`promote`를 아예 호출하지 않는다 — 아무것도 안 남는다.

### 추론(서빙) 쪽에서 쓰는 방법 — 계약

`pipeline/risk_model/jobs/run_risk_scoring_model.py`(`dag_risk_decision` DAG)가 이 계약대로 구현돼 있다:

1. `{model_root}/registry.json`을 읽어 `champion.artifact_uri`를 얻는다.
2. `joblib.load()`로 `model.joblib`을 로드한다 ([settings.py](settings.py) `read_bytes` + `io.BytesIO`를 쓰면 S3/로컬 모두 동일 코드로 처리됨).
3. 스코어링은 [train.py](train.py) `score(artifact, feat)`를 그대로 호출한다. champion은 항상 `models.primary` 타입으로 승격되므로(아래 "champion 승격 정책" 참고) 실제로는 그 타입 분기만 타지만, `score()` 자체는 `rule_trips`/`logreg`/`lgbm` 전부 지원한다.
4. 입력 피처는 반드시 [features.py](features.py) `build_features_for_inference()`로 만든다 — 학습과 동일 로직이라야 skew가 안 생긴다. `pipeline/risk_model/jobs/build_bike_features_daily.py`가 이걸 그대로 호출한다.
5. MLflow를 켠 경우(`mlflow.enabled: true`)에도 `artifact_uri`는 태그로만 남아 있으므로, tracking server가 죽어도 S3 직접 로드로 폴백 가능해야 한다.

### champion 승격 정책 — models.primary 고정

`train_and_evaluate` 태스크는 매 실행마다 `models.candidates`(`rule_trips`/`logreg`/`lgbm`) 전부를 학습·평가하지만, **승격 후보는 항상 `models.primary`(lgbm)로 고정**한다. `select_best()`가 고르는 "이번 평가 1등"은 리포트로만 로그에 남고 승격에는 안 쓰인다. 이유:

- `rule_trips`는 `score()`가 확률이 아니라 trips 개수를 그대로 반환한다 - 그게 승격되면 `risk_score`가 0~100 범위를 벗어나 추론 쪽 PyDeequ 검증이 깨진다. 애초에 "rule 대조군이 발표 근거가 된다"(위 표 참고)는 리포트용 설계다.
- `logreg`/`lgbm`처럼 둘 다 확률을 반환하는 타입끼리도 캘리브레이션(확률 분포)이 달라서, 재학습마다 champion 타입이 바뀌면 `risk_grade` 컷오프가 조용히 안 맞게 된다(에러 없이 등급만 이상해짐). 모델 아키텍처를 바꾸는 건 재학습이 자동으로 정할 일이 아니라 `models.primary`를 사람이 바꾸는 명시적 결정이어야 한다.

## 테스트 방법

### 1) 단위 테스트 — Spark 클러스터 불필요, 로직만 검증

[test_risk_model.py](test_risk_model.py)가 앵커 계산(gap=horizon 검증, 결정론성, 라벨 미확정 구간 거부)과 게이트 판정(하락 차단/허용, 동점시 primary 우선), `FEATURE_COLS` 계약을 검증한다. `pyspark`/`pyyaml`은 import 시점에 필요하지만 실제 Spark 세션은 띄우지 않는다.

```bash
docker exec airflow-scheduler bash -lc \
  "PYTHONPATH=/opt/airflow/pylib:/opt/airflow python -m pytest /opt/airflow/pipeline/train_risk_model/test_risk_model.py -q"
```

(`./pipeline:/opt/airflow/pipeline` 마운트로 이미 컨테이너 안에서 보인다 — 별도 마운트 불필요.)

### 2) 파이프라인 스모크 테스트 — 실제 DAG을 LocalStack Iceberg 데이터로 끝까지

사전 조건 확인:

```bash
docker exec airflow-scheduler bash -lc \
  "PYTHONPATH=/opt/airflow/pylib:/opt/airflow python /opt/airflow/scripts/check_silver_catalog.py --table rental_history"
```

`bike_catalog.silver.rental_history`/`failure_report`가 조회되면(행수 > 0) 준비 완료. 그다음 DAG 트리거:

```bash
docker exec airflow-scheduler airflow dags trigger risk_model_train --conf '{"dry_run": true}'
```

`dry_run: true`로 먼저 돌리면 배관만 확인하고 champion 레지스트리에는 손대지 않는다. 진행 확인:

```bash
docker exec airflow-scheduler airflow dags list-runs risk_model_train
docker exec airflow-scheduler airflow tasks states-for-dag-run risk_model_train "<run_id>"
```

태스크별 로그는 `airflow/logs/dag_id=risk_model_train/run_id=<run_id>/task_id=<task>/attempt=<n>.log`에 JSON lines로 쌓인다. `report` 태스크 로그의 `event` 필드에 최종 지표/게이트/아티팩트 경로가 찍힌다.

실제로 이렇게 검증된 실행(2026-08-17, 실데이터 rental_history 2026-06-01~07-01 / failure_report 2026-01-01~07-31, `config/risk_model.local.yaml` 완화 설정, holdout 3일):

| 후보 | capture@500 | std | min | PR-AUC |
|---|---|---|---|---|
| rule_trips | 3.58% | 0.389 | 3.28 | 0.032 |
| lgbm | 6.10% | 0.185 | 5.91 | 0.042 |
| **logreg** | **6.71%** | 0.181 | 6.52 | 0.044 |

홀드아웃이 3일뿐이라 (`risk_model.local.yaml`이 현재 backfill된 데이터 범위에 맞춘 완화값) 지표 자체는 참고용일 뿐 실운영 판단에 쓰지 않는다. rental_history가 6개월 이상 쌓이면 `config/risk_model.yaml`(운영값: holdout 60일, horizon 14일)로 재실행할 것.

## 로컬 실행 준비 (docker-compose.local.yml)

컨테이너에서 LocalStack Iceberg + risk_model 파이프라인을 돌리려면 다음이 다 갖춰져 있어야 한다. 하나라도 빠지면 조용히 멈추거나 엉뚱한 곳에 붙는다 — 실제로 전부 한 번씩 걸렸다.

| 항목 | 없으면 벌어지는 일 | 위치 |
|---|---|---|
| `.env`의 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`가 **빈 문자열이 아닌 값**(LocalStack은 `test`/`test`) | 코드의 `"test"` 기본값이 `os.getenv(KEY, "test")`인데 env가 "설정되어 있지만 빈 문자열"이면 기본값이 무시되고 빈 값이 그대로 들어가 인증 실패 | [.env](../../.env) |
| `S3_ENDPOINT=http://localstack:4566` | 기본값 `http://localhost:4566`을 그대로 쓰는데, 컨테이너 안에서 `localhost`는 컨테이너 자기 자신 — Iceberg 카탈로그 연결이 통째로 실패(타임아웃까지 몇 분 걸림) | `config/__init__.py`의 `Settings.s3_endpoint`, compose env로 주입 |
| `AWS_ENDPOINT_URL=http://localstack:4566` | `S3_ENDPOINT`와 별개로, [settings.py](settings.py)의 boto3 클라이언트(레지스트리 읽기/쓰기)는 이 변수를 본다 — 안 주면 레지스트리 접근이 실제 AWS로 나간다 | compose env |
| `RISK_MODEL_CONFIG=/opt/airflow/pylib/config/risk_model.local.yaml` | 안 주면 기본값(`risk_model.yaml`, 운영 스케일: rolling 24개월/holdout 60일)을 써서 지금 backfill된 1~2개월 데이터로는 앵커가 하나도 안 잡힘 | compose env |
| `config/risk_model.yaml`, `config/risk_model.local.yaml` 파일 자체 | [settings.py](settings.py) `DEFAULT_CONFIG`가 이 경로를 보는데 파일이 없으면 `FileNotFoundError` | `config/` (compose가 `./config`를 `/opt/airflow/pylib/config`에 마운트) |
| `lightgbm` (Python 패키지) | `train_candidates`가 `ModuleNotFoundError` | [airflow/requirements.txt](../../airflow/requirements.txt) |
| `libgomp1` (APT 패키지) | lightgbm은 설치돼도 import 시점에 `OSError: libgomp.so.1: cannot open shared object file` | [airflow/Dockerfile.local](../../airflow/Dockerfile.local) |

`airflow/requirements.txt`/`airflow/Dockerfile.local` 변경은 `docker compose build`로 이미지를 다시 만들어야 컨테이너에 반영된다. 급하게 확인만 하고 싶으면 살아있는 컨테이너에 바로 설치해도 되지만(`python -m pip install lightgbm`, `apt-get install libgomp1` — root 필요), 컨테이너를 재기동하면 사라진다.

원천 데이터가 6개월 이상 쌓이면 `RISK_MODEL_CONFIG`를 빼서(또는 `risk_model.yaml`로 바꿔서) 운영 설계값으로 다시 검증할 것 — `risk_model.local.yaml`의 `horizon_days: 3` 등은 라벨 정의 자체가 달라 지표를 운영 판단에 쓸 수 없다.

## 설계 결정과 근거

| 항목 | 결정 | 근거 |
|---|---|---|
| 트리거 | `schedule=None`, 수동 | 원천이 반기 공개라 고정 주기 재학습에 근거가 없다 |
| `run_key` | `as_of_end`(=라벨 확정 최신일) | `datetime.now()` 를 쓰지 않아 재실행이 멱등. 같은 키면 같은 경로 덮어쓰기 |
| 학습 윈도우 | rolling 24개월, 7일 간격 앵커 | 계절성 2주기 확보 + 낡은 분포 배제 |
| 홀드아웃 | 60일, 일별 앵커, gap 14일 | 실운영과 같은 "매일 아침 리스트" 빈도. gap 은 라벨창 누수 차단 |
| 후보 | rule / logreg / lgbm 동시 학습, 아티팩트는 1개 | 모델 차이만 비교. rule 대조군이 발표 근거가 된다 |
| 게이트 | champion 대비 −2%p 초과 하락 시 승격 보류 | DAG 실패가 아니라 보류. 아티팩트와 지표는 남긴다 |
| 학습 위치 | 피처=Spark, 학습=단일 노드 | 표본 규모에서 분산 학습은 오버헤드만 늘린다 |
| 피처 로직 | `features.py` 하나를 학습·추론이 공유 | train-serving skew 차단. 갈라지면 코드 리뷰에서 막는다 |
| 성능 | 일 집계 1회 + 앵커 broadcast join | 노트북은 앵커 N개면 대여이력을 N번 스캔했다. 창14/간격7이면 fan-out 2배로 제한 |

### 정책 두 가지 (앞선 논의 결론)

**`exclude_recent_days=30` — 학습·평가·추론 전부 적용.** 직전 30일에 이미 신고된 자전거는
재신고 확률이 구조적으로 높아 라벨을 오염시키고, 추론에서 제외하는 집단을 학습에 넣으면
분포가 어긋난다. 샘플 테이블에는 `excluded` 플래그로 **행을 남기고** 학습 직전에만 필터하므로,
30일 → 14/60일 실험 때 피처를 다시 만들 필요가 없다.

**후보군 = 창 내 대여 1건 이상.** 14일 미대여를 고장 신호로 쓸 수 없다. 인과가
고장→신고→수거→미대여 순서라 미대여는 원인이 아니라 결과이고, 신고 없이 수거된 자전거는
향후 14일 신고도 없어 **라벨 0** 이 된다. 그대로 넣으면 모델이 "미대여 = 안전" 을 학습한다.
대신 `dim_bike` 대조로 **미대여 대수를 일별 메트릭으로 기록**해 수거 적체 신호로 쓰고,
따맨 수거·배치 이벤트가 쌓인 뒤 v2 에서 "정비 중" 마스킹 후 재검토한다.

## 노트북과 달라진 점

1. **앵커별 재계산 → 일 집계 + broadcast join.** 결과는 같고 스캔 횟수만 줄었다.
2. **`dur_h` 는 진단용 컬럼으로만.** 모델 입력은 7개 고정(`FEATURE_COLS`).
3. **메인지표 분모를 별도 테이블(`fact_bike_label_pos_new`)로 물리화.** 노트북의 분모는
   피처 테이블에 없는 자전거까지 포함하므로, 피처 테이블만 보면 지표가 실제보다 좋게 나온다.
4. **`bike_class`(새싹/일반) 메타 컬럼 추가.** 모델 입력은 아니고 지표를 차종별로 쪼개보기 위한 것.
5. **`n_jobs: 1`.** LightGBM 이 스레드 수에 따라 결과가 흔들리지 않게 고정.
6. **이용시간 컬럼 파생.** silver.rental_history 에 이용시간이 없으면 `반납 - 대여` 로 만든다
   (`sources.rental_columns.dur_min: null`). silver 에 컬럼이 있으면 이름만 채우면 된다.

## 미결정 / 확인 필요

- `sources.assume_silver_clean` 기본 **false** — 이상치 필터(속도 45km/h, 거리 50km, 0분 5km)를
  이 파이프라인에서 재적용한다. silver DAG 이 이미 같은 필터를 걸었다면 **true** 로 바꿔야
  이중 정의가 사라진다.
- `run.fault_lookback_days` 기본 **null**(전체 이력) — 노트북과 동일한 `days_since_fail` 값.
  느리면 400 정도로 두되, 그 경우 "400일 이전 고장"은 `9999`(미고장)와 구분되지 않는다.
- silver 컬럼명 매핑 — 아키텍처 SVG 기준으로 채웠다. 실제 컬럼명이 다르면 config 만 수정.
- EMR 전환 시점 — `build_train_samples` 태스크만 `EmrAddStepsOperator` 로 바꾸면 된다.
  `samples.py` 에 `--anchor-plan` CLI 를 이미 붙여뒀다.
- MLflow — `mlflow.enabled: false`. 켤 경우 compose에 tracking server(백엔드=기존 postgres 별 DB,
  아티팩트=S3) 추가가 필요하고, joblib 은 S3 직접 저장 + 경로를 태그로 남겨 폴백을 유지한다.
- 추론 — `dag_risk_decision`(`pipeline/risk_model`)이 위 계약대로 연결돼 있음. 다만 이 환경엔 아직 `registry.json`에 champion이 없어서(`risk_model_train`을 `dry_run=false`로 실제 실행해 승격시킨 적 없음) end-to-end 동작은 미검증 상태 — 실제 학습 실행 후 확인 필요.
