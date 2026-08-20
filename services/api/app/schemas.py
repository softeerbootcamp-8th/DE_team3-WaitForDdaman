from typing import Literal, Optional

from pydantic import BaseModel

Region = Literal["강남", "강북"]
Tier = Literal["Normal", "Warning", "Critical"]


class Bike(BaseModel):
    bike_id: str
    station_name: str
    district: str
    region: Region
    station_urgency: str
    healthy_ratio: Optional[float]
    risk_grade: Tier
    risk_score: float
    dist_km: float
    aging: float
    fail_history: list[str]


class BikeLists(BaseModel):
    source: list[Bike]
    dest: list[Bike]


class Capacity(BaseModel):
    max: int


class SnapshotMeta(BaseModel):
    snapshot_date: str
    capacity: Capacity


class District(BaseModel):
    name: str
    path: str
    cx: float
    cy: float


class Station(BaseModel):
    station_id: str
    station_name: str
    district: str
    region: Region
    x: float
    y: float
    hold_num: int
    bike_cnt: int
    risk_cnt: int
    healthy_ratio: float
    urgency: str


class MapData(BaseModel):
    view_box: list[float]
    districts: list[District]
    stations: list[Station]


class ConfirmRequest(BaseModel):
    bike_ids: list[str]


class ConfirmResponse(BaseModel):
    snapshot_date: str
    confirmed: int
    # 마지막 확정 시각. 아직 확정한 적이 없으면 None (confirmed도 0)
    actioned_at: Optional[str]
