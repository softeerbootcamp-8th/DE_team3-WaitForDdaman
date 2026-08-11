# WaitForDdaman Backend (FastAPI)

따릉이 재배치 운영 콘솔의 API 서버. `softeer/project/prototype`의 정적 프로토타입(HTML + `data/snapshot.json` fetch)을
FastAPI 기반 REST API로 옮긴 것입니다.

## 구조

```
backend/
├── app/
│   ├── main.py          # FastAPI 앱, 라우터 등록, CORS
│   ├── schemas.py        # Pydantic 응답/요청 모델
│   ├── state.py          # 오늘 하루치 콘솔 상태(메모리) — source/dest 리스트, capacity, worklog
│   └── routers/
│       ├── snapshot.py   # GET /api/meta, GET /api/map
│       ├── bikes.py      # GET /api/bikes, POST /api/bikes/transfer, PATCH /api/capacity
│       └── worklog.py    # POST /api/worklog/confirm, GET /api/worklog
├── scripts/
│   └── build_snapshot.py # 오프라인 데이터 파이프라인 (pandas/sklearn) — snapshot.json 생성
├── data/                  # snapshot.json, worklog.json (git에는 커밋하지 않음)
└── requirements.txt
```

## API 서버 실행

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

`data/snapshot.json`이 이미 있어야 합니다 (이 저장소에는 프로토타입에서 생성해둔 스냅샷이
`backend/data/snapshot.json`으로 포함되어 있습니다). 앱 시작 시 이 파일을 한 번 읽어 메모리에
올린 뒤, 이후의 이동/확정/capacity 변경은 모두 메모리 + `data/worklog.json`으로 관리합니다
(`app/state.py`). 서버를 재시작하면 스냅샷 기준으로 상태가 초기화됩니다.

## 주요 엔드포인트

| Method | Path | 설명 |
|---|---|---|
| GET | `/api/meta` | 오늘 KPI·capacity·tier/action 집계 |
| GET | `/api/map` | 메인 탭 지도(자치구 경계 + 대여소 위험도) 데이터 |
| GET | `/api/bikes` | 수거후보 Pool(source) / 오늘 확정 대상(dest) 리스트 |
| POST | `/api/bikes/transfer` | `{ids, fromList}` — 선택한 자전거를 source↔dest 사이로 이동 |
| PATCH | `/api/capacity` | `{max}` — capacity 변경, risk_score 내림차순으로 재분할 |
| POST | `/api/worklog/confirm` | 오늘의 source/dest 상태를 작업이력으로 기록 |
| GET | `/api/worklog` | 누적된 작업이력 전체 (내보내기용) |

## 스냅샷 데이터 재생성 (오프라인 파이프라인)

`scripts/build_snapshot.py`는 원천 CSV/XLSX/GeoJSON(용량이 커서 이 저장소에는 포함하지 않음,
`softeer/project/p1`, `softeer/project/p2/data`를 참조)을 읽어 risk_score 모델을 학습하고
`backend/data/snapshot.json`을 다시 만듭니다. 데이터가 바뀌었을 때만 실행하면 됩니다 — API
서버 자체는 pandas/sklearn 없이 JSON만 읽습니다.

```bash
cd backend
python3 -m venv .venv-pipeline   # API용 venv와 분리 권장 (무거운 의존성)
source .venv-pipeline/bin/activate
pip install -r scripts/requirements.txt
# 기본 경로(../../../project/p1, ../../../project/p2/data)가 다르면 환경변수로 오버라이드
P1_DATA_DIR=/path/to/project/p1 P2_DATA_DIR=/path/to/project/p2/data \
  python3 scripts/build_snapshot.py
```
