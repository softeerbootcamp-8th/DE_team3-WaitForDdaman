"""app/state.py 의 DB에 의존하지 않는 부분 검증.

조회 함수들(get_bikes 등)은 raw SQL이라 Postgres 없이는 못 돌린다. 여기서는
행 -> API 응답 모양 변환과 OperationState 위임 계약만 본다.
"""
from __future__ import annotations

import pytest

from services.api.app import state as state_mod
from services.api.app.state import OperationState, _to_bike


def _row(**overrides) -> dict:
    base = {
        "bike_id": "SPB-12345",
        "station_name": "여의나루역 1번출구",
        "district": "영등포구",
        "region": "강남",
        "station_urgency": "높음",
        "healthy_ratio": 0.83,
        "risk_grade": "Critical",
        "risk_score": 0.91,
        "dist_km": 512.4,
        "aging": 3.2,
        "fail_history": ["체인", "브레이크"],
    }
    base.update(overrides)
    return base


# ── _to_bike: 프론트가 의존하는 두 개의 기본값 ──────────────────────────


@pytest.mark.parametrize("falsy", [None, ""])
def test_station_urgency가_비면_정보없음으로_채운다(falsy):
    assert _to_bike(_row(station_urgency=falsy))["station_urgency"] == "정보없음"


def test_station_urgency가_있으면_그대로_둔다():
    assert _to_bike(_row(station_urgency="낮음"))["station_urgency"] == "낮음"


@pytest.mark.parametrize("falsy", [None, []])
def test_fail_history가_비면_빈_리스트다(falsy):
    # None을 그대로 흘리면 스키마(list[str])에서 터진다.
    assert _to_bike(_row(fail_history=falsy))["fail_history"] == []


def test_fail_history가_있으면_그대로_둔다():
    assert _to_bike(_row(fail_history=["안장"]))["fail_history"] == ["안장"]


def test_healthy_ratio는_None을_보존한다():
    # 스키마가 Optional[float]이라 0.0으로 뭉개면 "미측정"과 "0%"가 구분되지 않는다.
    assert _to_bike(_row(healthy_ratio=None))["healthy_ratio"] is None


def test_healthy_ratio_0은_None으로_바뀌지_않는다():
    assert _to_bike(_row(healthy_ratio=0.0))["healthy_ratio"] == 0.0


def test_to_bike는_스키마가_요구하는_키를_모두_만든다():
    from services.api.app.schemas import Bike

    assert set(_to_bike(_row())) == set(Bike.model_fields)


# ── OperationState: 라우터가 기대하는 property/메서드 구분 ───────────────


def test_라우터가_괄호없이_읽는_것은_property다():
    # snapshot.py는 state.meta / state.map_data, actions.py는
    # state.latest_confirmation / state.confirmed_bikes 를 괄호 없이 읽는다.
    # 메서드로 바뀌면 응답에 bound method가 실려 스키마 검증에서 터진다.
    for name in ("meta", "map_data", "latest_confirmation", "confirmed_bikes"):
        assert isinstance(getattr(OperationState, name), property), f"{name}이 property가 아니다"


def test_라우터가_호출하는_것은_메서드다():
    # bikes.py는 state.bikes(), actions.py는 state.confirm_collection(...) 을 호출한다.
    for name in ("bikes", "confirm_collection"):
        attr = getattr(OperationState, name)
        assert callable(attr) and not isinstance(attr, property), f"{name}이 호출 가능하지 않다"


def test_property가_모듈_함수로_위임한다(monkeypatch):
    monkeypatch.setattr(state_mod, "get_meta", lambda: {"sentinel": "meta"})
    monkeypatch.setattr(state_mod, "get_map_data", lambda: {"sentinel": "map"})
    monkeypatch.setattr(state_mod, "get_latest_confirmation", lambda: {"sentinel": "conf"})
    monkeypatch.setattr(state_mod, "get_confirmed_bikes", lambda: {"sentinel": "bikes"})

    s = OperationState()
    assert s.meta == {"sentinel": "meta"}
    assert s.map_data == {"sentinel": "map"}
    assert s.latest_confirmation == {"sentinel": "conf"}
    assert s.confirmed_bikes == {"sentinel": "bikes"}


def test_메서드가_모듈_함수로_위임한다(monkeypatch):
    monkeypatch.setattr(state_mod, "get_bikes", lambda: (["src"], ["dst"]))
    captured = {}

    def fake_confirm(bike_ids):
        captured["ids"] = bike_ids
        return {"confirmed": len(bike_ids)}

    monkeypatch.setattr(state_mod, "confirm_collection", fake_confirm)

    s = OperationState()
    assert s.bikes() == (["src"], ["dst"])
    assert s.confirm_collection(["A", "B"]) == {"confirmed": 2}
    assert captured["ids"] == ["A", "B"]


# ── 회귀 방지: Normal 등급 제외 (#95 -> #104) ────────────────────────────


def test_수거_후보_쿼리가_Normal_등급을_제외한다():
    """#95에서 mart_bike_risk_daily의 action 컬럼이 사라진 뒤 필터가 빠져,
    Normal 32,922대가 수거 후보 Pool에 섞여 "총 대여중단 대수"가 34,586대로
    잡히는 문제가 있었다(#104).

    실제 쿼리 결과는 Postgres 없이 검증할 수 없으므로(이슈 #128 참고),
    필터 조건이 SQL에서 사라지지 않았는지만 소스 레벨로 잠근다.
    """
    import inspect

    src = inspect.getsource(state_mod.get_bikes)
    assert "risk_grade <> :no_action_grade" in src, (
        "get_bikes에서 Normal 등급 제외 조건이 사라졌다 (#95 회귀)"
    )
    assert state_mod.NO_ACTION_GRADE == "Normal"
