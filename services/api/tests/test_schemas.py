"""app/schemas.py 응답 계약 검증. 프론트가 이 모양에 맞춰 그린다."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from services.api.app.schemas import Bike, MapData, SnapshotMeta, Station


def _bike(**overrides) -> dict:
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
        "fail_history": ["체인"],
    }
    base.update(overrides)
    return base


def test_유효한_자전거는_통과한다():
    assert Bike(**_bike()).risk_grade == "Critical"


@pytest.mark.parametrize("region", ["강남", "강북"])
def test_허용된_지역만_받는다(region):
    assert Bike(**_bike(region=region)).region == region


@pytest.mark.parametrize("bad", ["강서", "서울", "", "GANGNAM"])
def test_알_수_없는_지역은_거부한다(bad):
    with pytest.raises(ValidationError):
        Bike(**_bike(region=bad))


@pytest.mark.parametrize("tier", ["Normal", "Warning", "Critical"])
def test_허용된_위험등급만_받는다(tier):
    assert Bike(**_bike(risk_grade=tier)).risk_grade == tier


@pytest.mark.parametrize("bad", ["critical", "High", "위험", ""])
def test_알_수_없는_위험등급은_거부한다(bad):
    # 파이프라인이 등급 문자열을 바꾸면 여기서 먼저 터져야 한다.
    with pytest.raises(ValidationError):
        Bike(**_bike(risk_grade=bad))


def test_healthy_ratio는_None을_허용한다():
    assert Bike(**_bike(healthy_ratio=None)).healthy_ratio is None


def test_aging은_None을_허용한다():
    assert Bike(**_bike(aging=None)).aging is None


def test_dist_km은_None을_허용한다():
    assert Bike(**_bike(dist_km=None)).dist_km is None


def test_필수_필드가_없으면_거부한다():
    payload = _bike()
    del payload["risk_score"]
    with pytest.raises(ValidationError):
        Bike(**payload)


def test_fail_history는_문자열_리스트다():
    with pytest.raises(ValidationError):
        Bike(**_bike(fail_history="체인"))  # 문자열 하나를 넘기면 안 된다


def test_지도_응답_모양():
    data = MapData(
        view_box=[880.0, 640.0],
        districts=[{"name": "영등포구", "path": "M0,0L1,1Z", "cx": 1.0, "cy": 2.0}],
        stations=[
            {
                "station_id": "ST-1",
                "station_name": "여의나루역 1번출구",
                "district": "영등포구",
                "region": "강남",
                "x": 1.0,
                "y": 2.0,
                "hold_num": 10,
                "bike_cnt": 7,
                "risk_cnt": 2,
                "healthy_ratio": 0.71,
                "urgency": "높음",
            }
        ],
    )
    assert data.stations[0].station_id == "ST-1"
    assert len(data.view_box) == 2


def test_대여소도_지역_Literal을_공유한다():
    with pytest.raises(ValidationError):
        Station(
            station_id="ST-1",
            station_name="X",
            district="영등포구",
            region="강서",
            x=0.0,
            y=0.0,
            hold_num=0,
            bike_cnt=0,
            risk_cnt=0,
            healthy_ratio=0.0,
            urgency="낮음",
        )


def test_스냅샷_메타_모양():
    meta = SnapshotMeta(snapshot_date="2026-08-21", capacity={"max": 700})
    assert meta.capacity.max == 700
