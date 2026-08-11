"""
따릉이 재배치 운영 콘솔 - 오프라인 데이터 파이프라인

원천 데이터 (모두 읽기 전용, 이 저장소에는 커밋하지 않고 상대경로로 참조):
  - <P1_DATA_DIR>/서울특별시 공공자전거 대여이력 정보_2606.csv   (자전거 단위 상세 대여이력)
  - <P2_DATA_DIR>/서울시_공공자전거_고장신고_내역_*.csv           (고장신고 이력)
  - <P1_DATA_DIR>/공공자전거_대여소_정보_2606.xlsx                (대여소 마스터: 위치, 거치대수)
  - <P1_DATA_DIR>/서울_자치구_경계.geojson                        (지도용 자치구 경계)

원천 데이터 위치는 이 저장소 밖(softeer/project/p1, softeer/project/p2/data)에 있으므로
환경변수 P1_DATA_DIR / P2_DATA_DIR로 오버라이드할 수 있다. 기본값은 이 스크립트를
softeer/waitforddaman/backend/scripts 안에 둔 상태에서 softeer/project/{p1,p2/data}를 가리킨다.

이 스크립트가 만들어내는 것:
  - backend/data/snapshot.json : FastAPI가 시작 시 읽어서 API로 서빙하는 오늘자 콘솔 데이터
    (백엔드 코드는 이 파일이 pandas/sklearn 없이도 그냥 JSON으로 읽히기만 하면 되므로,
    이 파이프라인은 API 서버와 별도로 - 데이터가 바뀔 때만 - 오프라인으로 재실행한다)

핵심 가정 (원본 프로토타입 README에서 그대로 가져옴):
  - "오늘" = 대여이력 데이터의 마지막 날짜(2026-06-30).
  - risk_score는 누적 이동거리 / 누적 이동시간 / 노후화정도 3요소를 입력변수로 쓴 로지스틱
    회귀다. "노후화정도"에 대응하는 원천 데이터(도입일, 생애 누적사용시간)가 없어서, 과거
    고장신고 이력을 지수감쇠(exponential decay)로 압축한 aging_proxy로 대체한다.
  - 자전거의 "현재 위치"는 6월 중 마지막 반납 대여소로 역산한다.
  - "작업이력(수거/대여중단)" 로그는 이 시스템이 새로 만들어내는 것이라, 이 스크립트는
    오늘자 제안값(source/dest 초기 분할)만 계산한다. 실제 확정/이동은 FastAPI 백엔드가
    메모리 상태 + backend/data/worklog.json으로 관리한다 (app/state.py 참고).
"""
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# 0. 설정값 (튜닝 포인트를 한곳에 모음)
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SCRIPTS_DIR.parent
REPO_ROOT = BACKEND_DIR.parent

DEFAULT_P1 = REPO_ROOT.parent / "project" / "p1"
DEFAULT_P2_DATA = REPO_ROOT.parent / "project" / "p2" / "data"

P1_DATA = Path(os.environ.get("P1_DATA_DIR", DEFAULT_P1))
P2_DATA = Path(os.environ.get("P2_DATA_DIR", DEFAULT_P2_DATA))
OUT_DIR = BACKEND_DIR / "data"

TODAY = pd.Timestamp("2026-06-30")               # 대여이력 데이터의 마지막 날 = "오늘"
AGING_HALFLIFE_DAYS = 90                          # 노후화 지수감쇠 반감기
STATION_HEALTHY_CUTOFF = 0.70                     # 정상자전거 비율 70% 컷오프
CRITICAL_QUANTILE = 0.99                          # risk_score 상위 1% = Critical
RISK_QUANTILE = 0.95                              # risk_score 상위 1~5% = Risk
DEFAULT_CAPACITY = 700                            # 정비소 하루 처리 capacity 기본값 (API에서 수정 가능)
                                                   # 근거: 검증 노트북 9절 "정비 26.4만건/년" ÷ 365일 ≈ 723건/일
SEASON_MULTIPLIER = {                             # 기존 노트북 3절 분석과 동일한 계절 보정 배수
    "봄": 1.05, "여름": 0.93, "가을": 1.10, "겨울": 0.92,
}
CURRENT_SEASON = "여름"

FAIL_FILES = [
    "서울시_공공자전거_고장신고_내역_2501-2506.csv",
    "서울시_공공자전거_고장신고_내역_2507-2512.csv",
    "서울시_공공자전거_고장신고_내역_2601-2606.csv",
]
RENT_FILE = "서울특별시_공공자전거_대여이력_정보_2606.csv"
STATION_FILE = "공공자전거_대여소_정보_2606.xlsx"
DISTRICT_GEOJSON_FILE = "서울_자치구_경계.geojson"

MAP_TARGET_WIDTH = 880   # 메인페이지 지도 SVG 폭(px) — 위경도를 이 좌표계로 투영
MAP_PAD = 24


# ---------------------------------------------------------------------------
# 1. 원천 데이터 로드
# ---------------------------------------------------------------------------
def load_rent():
    usecols = ["자전거번호", "대여일시", "대여 대여소번호", "대여 대여소명",
               "반납일시", "반납대여소번호", "반납대여소명", "이용시간(분)", "이용거리(M)"]
    dtypes = {"이용시간(분)": "int32", "이용거리(M)": "float32"}
    df = pd.read_csv(P2_DATA / RENT_FILE, encoding="cp949", usecols=usecols, dtype=dtypes,
                      parse_dates=["대여일시", "반납일시"])
    df = df.dropna(subset=["자전거번호"])
    df["자전거번호"] = df["자전거번호"].astype("category")

    # 노트북과 동일한 이상치 제거: 비현실적 속도(>45km/h) 또는 0분인데 5km 넘는 트립
    sub = df[df["이용시간(분)"] > 0].copy()
    speed_kmh = (sub["이용거리(M)"] / 1000) / (sub["이용시간(분)"] / 60)
    bad_idx = sub.index[(speed_kmh > 45) | (sub["이용거리(M)"] > 50_000)]
    zero_dur_big = df.index[(df["이용시간(분)"] == 0) & (df["이용거리(M)"] > 5_000)]
    clean = df.drop(index=bad_idx.union(zero_dur_big))
    return clean


def load_fail():
    dfs = [pd.read_csv(P2_DATA / f, encoding="cp949") for f in FAIL_FILES]
    fail = pd.concat(dfs, ignore_index=True)
    fail["등록일시"] = pd.to_datetime(fail["등록일시"])
    fail["구분"] = fail["구분"].str.strip()
    return fail


def load_stations():
    raw = pd.read_excel(P1_DATA / STATION_FILE, header=None, skiprows=4)
    raw = raw.iloc[:, :10]
    raw.columns = ["대여소번호", "대여소명", "자치구", "상세주소", "위도", "경도",
                   "설치시기", "LCD거치대수", "QR거치대수", "운영방식"]
    raw = raw.dropna(subset=["대여소번호"])
    raw["대여소번호"] = raw["대여소번호"].astype(int)
    raw["대여소명"] = raw["대여소명"].str.strip()
    raw["총거치대수"] = raw["LCD거치대수"].fillna(0) + raw["QR거치대수"].fillna(0)
    return raw.drop_duplicates(subset=["대여소번호"], keep="last").set_index("대여소번호")


def load_district_geojson():
    with open(P1_DATA / DISTRICT_GEOJSON_FILE, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 지도용 위경도 → SVG 좌표 투영 (등장방형도법 + 위도 보정, 외부 지도 라이브러리/타일 서버 없이
# 서울시 자치구 경계 geojson만으로 완전히 오프라인에서 그린다)
# ---------------------------------------------------------------------------
def build_projector(geojson, extra_lats, extra_lngs):
    lats, lngs = list(extra_lats), list(extra_lngs)
    for feat in geojson["features"]:
        for ring in feat["geometry"]["coordinates"]:
            for lng, lat in ring:
                lats.append(lat)
                lngs.append(lng)
    lat_min, lat_max = min(lats), max(lats)
    lng_min, lng_max = min(lngs), max(lngs)
    cos_lat_mid = np.cos(np.radians((lat_min + lat_max) / 2))
    lng_span_adj = (lng_max - lng_min) * cos_lat_mid
    lat_span = lat_max - lat_min
    inner_w = MAP_TARGET_WIDTH - 2 * MAP_PAD
    scale = inner_w / lng_span_adj
    height = int(lat_span * scale + 2 * MAP_PAD)

    def project(lat, lng):
        x = (lng - lng_min) * cos_lat_mid * scale + MAP_PAD
        y = (lat_max - lat) * scale + MAP_PAD
        return round(x, 1), round(y, 1)

    return project, (MAP_TARGET_WIDTH, height)


def build_map_data(geojson, project, viewbox, st_stats, stations_master):
    districts = []
    for feat in geojson["features"]:
        ring = feat["geometry"]["coordinates"][0]
        pts = [project(lat, lng) for lng, lat in ring]
        path = "M" + " L".join(f"{x},{y}" for x, y in pts) + " Z"
        cx = round(sum(p[0] for p in pts) / len(pts), 1)
        cy = round(sum(p[1] for p in pts) / len(pts), 1)
        districts.append({"name": feat["properties"]["name"], "path": path, "cx": cx, "cy": cy})

    stations = []
    risk_count = st_stats["bike_count"] - st_stats["normal_count"]
    for st_id, row in st_stats.iterrows():
        if st_id not in stations_master.index:
            continue
        sm = stations_master.loc[st_id]
        if pd.isna(sm["위도"]) or pd.isna(sm["경도"]):
            continue
        x, y = project(float(sm["위도"]), float(sm["경도"]))
        stations.append({
            "id": int(st_id),
            "name": sm["대여소명"],
            "gu": sm["자치구"],
            "x": x, "y": y,
            "bikeCount": int(row["bike_count"]),
            "riskCount": int(risk_count.loc[st_id]),
            "healthyRatio": float(round(row["healthy_ratio"] * 100, 1)),
            "urgency": row["urgency"],
        })
    return {"viewBox": list(viewbox), "districts": districts, "stations": stations}


# ---------------------------------------------------------------------------
# 2. 자전거 단위 특징(feature) 계산 — 사용자가 지정한 3요소: 누적거리 / 누적시간 / 노후화정도
# ---------------------------------------------------------------------------
def build_bike_features(rent: pd.DataFrame, fail: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    """as_of 시점까지의 데이터만으로 자전거별 특징을 계산한다 (백테스트에도 재사용)."""
    active = rent[rent["대여일시"] <= as_of]

    usage = active.groupby("자전거번호", observed=True).agg(
        trips=("대여일시", "count"),
        dist_m=("이용거리(M)", "sum"),
        dur_min=("이용시간(분)", "sum"),
    )
    usage["dist_km"] = usage["dist_m"] / 1000
    usage["dur_h"] = usage["dur_min"] / 60

    # 노후화정도(aging_proxy): 과거 고장신고를 지수감쇠로 압축 — 최근/빈번한 고장일수록 값이 큼
    prior_fail = fail[fail["등록일시"] <= as_of].copy()
    prior_fail["age_days"] = (as_of - prior_fail["등록일시"]).dt.days
    prior_fail["decay"] = np.exp(-prior_fail["age_days"] / AGING_HALFLIFE_DAYS)
    aging = prior_fail.groupby("자전거번호")["decay"].sum().rename("aging_proxy")
    prior_count = prior_fail.groupby("자전거번호").size().rename("prior_fail_count")
    last_fail = prior_fail.groupby("자전거번호")["등록일시"].max().rename("last_fail_date")

    feat = usage.join(aging).join(prior_count).join(last_fail)
    feat["aging_proxy"] = feat["aging_proxy"].fillna(0.0)
    feat["prior_fail_count"] = feat["prior_fail_count"].fillna(0).astype(int)
    feat["days_since_last_fail"] = (as_of - feat["last_fail_date"]).dt.days.fillna(999).astype(int)
    return feat


def current_station(rent: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    """as_of 이전 마지막 반납 기록으로 자전거의 '현재' 대여소를 역산한다."""
    returned = rent[(rent["반납일시"] <= as_of) & rent["반납대여소번호"].notna()].sort_values("반납일시")
    last = returned.groupby("자전거번호", observed=True).tail(1)
    return last.set_index("자전거번호")[["반납대여소번호", "반납대여소명"]].rename(
        columns={"반납대여소번호": "station_id", "반납대여소명": "station_name"})


# ---------------------------------------------------------------------------
# 3. risk_score 모델 (거리 / 시간 / 노후화정도 3변수 로지스틱 회귀)
# ---------------------------------------------------------------------------
RISK_FEATURES = ["dist_km", "dur_h", "aging_proxy"]


def fit_risk_model(feat: pd.DataFrame, label: pd.Series):
    scaler = StandardScaler().fit(feat[RISK_FEATURES].values)
    Xs = scaler.transform(feat[RISK_FEATURES].values)
    model = LogisticRegression(max_iter=1000).fit(Xs, label.values)
    return scaler, model


def score(feat: pd.DataFrame, scaler, model) -> pd.Series:
    Xs = scaler.transform(feat[RISK_FEATURES].values)
    proba = model.predict_proba(Xs)[:, 1]
    return pd.Series(proba, index=feat.index) * SEASON_MULTIPLIER[CURRENT_SEASON]


def tier_from_score(risk_score_0_100: pd.Series) -> pd.Series:
    q_critical = risk_score_0_100.quantile(CRITICAL_QUANTILE)
    q_risk = risk_score_0_100.quantile(RISK_QUANTILE)
    return pd.cut(
        risk_score_0_100, bins=[-1, q_risk, q_critical, 101],
        labels=["Normal", "Risk", "Critical"],
    )


# ---------------------------------------------------------------------------
# 4. 대여소 시급도
# ---------------------------------------------------------------------------
def station_urgency(bikes: pd.DataFrame) -> pd.DataFrame:
    g = bikes.groupby("station_id")
    stats = g.agg(bike_count=("tier", "size"),
                   normal_count=("tier", lambda s: (s == "Normal").sum()))
    stats["healthy_ratio"] = stats["normal_count"] / stats["bike_count"]
    stats["urgency"] = np.where(stats["healthy_ratio"] >= STATION_HEALTHY_CUTOFF, "여유있음", "부족함")
    return stats


# ---------------------------------------------------------------------------
# 5. 결정 플로우 (플로우차트 그대로)
# ---------------------------------------------------------------------------
def decide_action(row) -> str:
    if row["tier"] == "Normal":
        return "조치없음"
    if row["tier"] == "Critical":
        return "대여중단"
    # Risk
    return "대여중단" if row["urgency"] == "여유있음" else "조치보류"


# ---------------------------------------------------------------------------
# 6. 일별 백테스트 — KPI 카드용
#
# 이해관계자 인터뷰 표의 "배차" 담당자 KPI를 그대로 씀:
#   "신규고장(직전 30일 신고 이력 없음) 중, 신고접수 전에 목록에 올라 있었던 비율 (지금 0%)"
# 즉 어떤 날 새로 신고된(=최근 30일간 이 자전거로 다른 신고가 없었던) 고장 중,
# 그 신고가 들어오기 전날 시점에 이미 우리 위험목록(Critical/Risk)에 있었던 비율.
#
# 한계: 이 값을 구하려면 신고 시점 이전의 risk_score(누적거리/시간 포함)가 필요한데,
# 대여이력 상세데이터가 6월 한 달뿐이라 이 백테스트도 6월 안에서만 유효하다. 6월 1일
# 신고를 평가할 때 기준일(5/31)은 대여이력 범위 밖이라 항상 "포착 못함(0%)"으로 나온다 —
# 버그가 아니라 "그 이전 데이터가 없다"는 사실 그대로다.
# ---------------------------------------------------------------------------
def daily_capture_rate(rent, fail, scaler, model, days: pd.DatetimeIndex) -> dict:
    fail_sorted = fail.sort_values("등록일시")
    result = {}
    for d in days:
        d_start = d.normalize()
        d_end = d_start + pd.Timedelta(days=1)
        today_reports = fail_sorted[(fail_sorted["등록일시"] >= d_start) & (fail_sorted["등록일시"] < d_end)]
        key = d.strftime("%Y-%m-%d")
        if today_reports.empty:
            result[key] = None
            continue

        lookback_start = d_start - pd.Timedelta(days=30)
        prior_window = fail_sorted[(fail_sorted["등록일시"] >= lookback_start) & (fail_sorted["등록일시"] < d_start)]
        had_recent_report = set(prior_window["자전거번호"])
        new_fail_bikes = [b for b in today_reports["자전거번호"].unique() if b not in had_recent_report]
        if not new_fail_bikes:
            result[key] = None
            continue

        feat_prev = build_bike_features(rent, fail, d_start - pd.Timedelta(days=1))
        if feat_prev.empty:
            result[key] = 0.0
            continue
        tier_prev = tier_from_score(score(feat_prev, scaler, model) * 100)
        flagged = set(feat_prev.index[tier_prev != "Normal"])

        caught = sum(1 for b in new_fail_bikes if b in flagged)
        result[key] = round(caught / len(new_fail_bikes) * 100, 1)
    return result


# ---------------------------------------------------------------------------
# 7. 사람이 읽을 수 있는 "선정 근거" 문구
# ---------------------------------------------------------------------------
def reason_text(row) -> str:
    parts = []
    parts.append(f"6월 누적 이동거리 {row['dist_km']:.0f}km")
    parts.append(f"누적 이용시간 {row['dur_h']:.0f}시간")
    if row["prior_fail_count"] > 0:
        parts.append(f"과거 고장신고 {int(row['prior_fail_count'])}회(최근 {int(row['days_since_last_fail'])}일 전)")
    else:
        parts.append("과거 고장신고 이력 없음")
    return " · ".join(parts)


def main():
    OUT_DIR.mkdir(exist_ok=True)

    rent = load_rent()
    fail = load_fail()
    stations_master = load_stations()

    # --- 오늘(=6/30) 기준 전체 파이프라인 ---
    feat = build_bike_features(rent, fail, TODAY)
    # 모델 라벨: "이번 달(6월) 안에 고장신고가 있었는가" — 기존 검증 노트북과 동일한 타깃
    fail_june_bikes = set(fail.loc[(fail["등록일시"] >= "2026-06-01") & (fail["등록일시"] <= TODAY), "자전거번호"])
    label = feat.index.to_series(index=feat.index).isin(fail_june_bikes)

    scaler, model = fit_risk_model(feat, label)
    feat["risk_score"] = (score(feat, scaler, model) * 100).round(1)
    feat["tier"] = tier_from_score(feat["risk_score"])

    loc = current_station(rent, TODAY)
    bikes = feat.join(loc, how="inner")  # 6월 중 반납 기록이 없는(=한번도 안 돌아온) 자전거는 위치 불명 → 제외

    st_stats = station_urgency(bikes)
    bikes = bikes.join(st_stats[["healthy_ratio", "urgency"]], on="station_id")
    bikes["urgency"] = bikes["urgency"].fillna("정보없음")

    # --- 메인페이지 지도(히트맵)용 데이터: 자치구 경계 + 대여소별 위험 자전거 수 ---
    geojson = load_district_geojson()
    station_lats = stations_master["위도"].dropna().tolist()
    station_lngs = stations_master["경도"].dropna().tolist()
    project, viewbox = build_projector(geojson, station_lats, station_lngs)
    map_data = build_map_data(geojson, project, viewbox, st_stats, stations_master)

    bikes["action"] = bikes.apply(decide_action, axis=1)
    bikes["reason"] = bikes.apply(reason_text, axis=1)

    pool = bikes[bikes["action"] == "대여중단"].sort_values("risk_score", ascending=False)

    print(f"활성 자전거: {len(bikes):,}대")
    print(bikes["tier"].value_counts())
    print(f"수거후보 Pool: {len(pool):,}대")
    print(bikes["action"].value_counts())

    # --- capacity 적용: 우선순위 상위 N대는 '오늘 확정', 나머지는 대기 ---
    capacity = min(DEFAULT_CAPACITY, len(pool))
    dest_ids = pool.index[:capacity]
    source_ids = pool.index[capacity:]

    # 자전거별 고장이력을 미리 그룹핑해두고 재사용 (수천 번 반복 필터링 방지)
    fail_by_bike = {
        bike_id: g.sort_values("등록일시", ascending=False)
        for bike_id, g in fail.groupby("자전거번호")
    }

    def bike_record(bike_id):
        r = pool.loc[bike_id]
        st_id = int(r["station_id"])
        sm = stations_master.loc[st_id] if st_id in stations_master.index else None
        station_name = sm["대여소명"] if sm is not None else r.get("station_name", "알수없음")
        gu = sm["자치구"] if sm is not None else ""
        hist_df = fail_by_bike.get(bike_id)
        history = []
        if hist_df is not None:
            top5 = hist_df.head(5)
            history = [f"{cat} ({dt.strftime('%y/%m/%d')})"
                       for dt, cat in zip(top5["등록일시"], top5["구분"])]
        return {
            "id": bike_id,
            "station": station_name,
            "gu": gu,
            "stationUrgency": r["urgency"],
            "healthyRatio": None if pd.isna(r["healthy_ratio"]) else float(round(r["healthy_ratio"] * 100, 1)),
            "tier": str(r["tier"]),
            "score": float(r["risk_score"]),
            "reason": r["reason"],
            "distKm": round(float(r["dist_km"]), 1),
            "durH": float(round(r["dur_h"], 1)),
            "priorFailCount": int(r["prior_fail_count"]),
            "daysSinceLastFail": int(r["days_since_last_fail"]),
            "history": history,
        }

    dest_list = [bike_record(b) for b in dest_ids]
    source_list = [bike_record(b) for b in source_ids]

    # --- KPI 백테스트: "신규고장 사전포착률" 오늘 / 어제 / 6월 평균 ---
    backtest_days = pd.date_range("2026-06-01", TODAY, freq="D")
    capture = daily_capture_rate(rent, fail, scaler, model, backtest_days)
    today_key = TODAY.strftime("%Y-%m-%d")
    yesterday_key = (TODAY - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    valid_days = [v for v in capture.values() if v is not None]
    kpi = {
        "today": float(capture[today_key]) if capture[today_key] is not None else None,
        "yesterday": float(capture[yesterday_key]) if capture[yesterday_key] is not None else None,
        "monthly": float(round(np.mean(valid_days), 1)) if valid_days else None,
    }

    snapshot = {
        "generatedAt": TODAY.strftime("%Y-%m-%d 08:42"),
        "capacity": {"used": int(len(dest_list)), "max": int(DEFAULT_CAPACITY)},
        "kpi": kpi,
        "poolSize": int(len(pool)),
        "tierCounts": {str(k): int(v) for k, v in bikes["tier"].value_counts().items()},
        "actionCounts": {str(k): int(v) for k, v in bikes["action"].value_counts().items()},
        "source": source_list,
        "dest": dest_list,
        "dailyCaptureRate": {k: (float(v) if v is not None else None) for k, v in capture.items()},
        "map": map_data,
    }

    with open(OUT_DIR / "snapshot.json", "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)

    print(f"\nsnapshot.json 저장 완료 → {OUT_DIR / 'snapshot.json'}")
    print(f"capacity={capacity}, dest(오늘 확정 예정)={len(dest_list)}, source(대기)={len(source_list)}")
    print(f"KPI: {kpi}")
    print(f"지도 대여소 수: {len(map_data['stations']):,} (viewBox={map_data['viewBox']})")


if __name__ == "__main__":
    main()
