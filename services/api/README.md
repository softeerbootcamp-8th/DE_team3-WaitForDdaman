# WaitForDdaman Backend (FastAPI)

따릉이 재배치 운영 콘솔의 API 서버. Airflow 파이프라인이 매일 Postgres에 채워두는
`dim_district` / `station_daily` / `bike_risk_daily`를 읽어 REST API로 내려준다.

## 구조

```
api/
├── app/
│   ├── main.py          # FastAPI 앱, 라우터 등록, CORS
│   ├── schemas.py        # Pydantic 응답 모델
│   ├── db.py              # SQLAlchemy 엔진 (DATABASE_URL)
│   ├── state.py           # 최신 snapshot_date 기준 조회 계층 (읽기 전용)
│   └── routers/
│       ├── snapshot.py   # GET /api/meta, GET /api/map
│       └── bikes.py      # GET /api/bikes
├── scripts/
│   └── build_snapshot.py # 원천 CSV/XLSX로부터 risk_score를 계산하는 오프라인 파이프라인 참고 구현
│                          # (Postgres 적재는 Airflow DAG이 담당 — 이 리포에는 아직 없음)
└── requirements.txt
```

## API 서버 실행

```bash
cd services/api
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
DATABASE_URL=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow \
  uvicorn app.main:app --reload --port 8000
```

`DATABASE_URL`을 지정하지 않으면 `docker-compose.yml`의 postgres 서비스 기본 자격증명
(`airflow`/`airflow`/`airflow`, 호스트 `postgres`)을 그대로 사용한다. 도커 밖에서 로컬로 띄울 때는
호스트를 `localhost`로 바꿔서 오버라이드해야 한다.

매 요청마다 그 시점 최신 `snapshot_date`를 다시 조회한다 — 서버가 상태를 메모리에 들고
있지 않으므로, 파이프라인이 새 스냅샷을 넣으면 재시작 없이 바로 반영된다.

## 주요 엔드포인트

| Method | Path | 설명 |
|---|---|---|
| GET | `/api/meta` | 최신 snapshot_date, capacity 기본값 |
| GET | `/api/map` | 메인 지도(자치구 경계 + 대여소 위험도) 데이터 |
| GET | `/api/bikes` | 수거 후보 Pool — action이 `대여중단`/`수거`인 자전거 (source/dest) |

## 스냅샷 데이터 파이프라인

`scripts/build_snapshot.py`는 원천 CSV/XLSX/GeoJSON으로부터 risk_score를 계산하던 예전
오프라인 파이프라인이다. 지금은 그 결과를 Postgres에 적재하는 Airflow DAG으로 옮기는 것이
목표이고, 이 스크립트는 그 로직을 옮길 때 참고용으로만 남겨둔 것 — 현재 API는 이 스크립트의
출력(JSON)을 더 이상 읽지 않는다.
