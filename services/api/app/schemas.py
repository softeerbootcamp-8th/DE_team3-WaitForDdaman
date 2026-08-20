from typing import Literal, Optional

from pydantic import BaseModel

Region = Literal["강남", "강북"]
Tier = Literal["Normal", "Warning", "Critical"]


class Bike(BaseModel):
    bikeId: str
    stationName: str
    district: str
    region: Region
    stationUrgency: str
    healthyRatio: Optional[float]
    riskGrade: Tier
    riskScore: float
    reason: str
    distKm: float
    durH: float
    aging: float
    failHistory: list[str]


class BikeLists(BaseModel):
    source: list[Bike]
    dest: list[Bike]


class Capacity(BaseModel):
    max: int


class SnapshotMeta(BaseModel):
    snapshotDate: str
    capacity: Capacity


class District(BaseModel):
    name: str
    path: str
    cx: float
    cy: float


class Station(BaseModel):
    stationId: str
    stationName: str
    district: str
    region: Region
    x: float
    y: float
    holdNum: int
    bikeCnt: int
    riskCnt: int
    healthyRatio: float
    urgency: str


class MapData(BaseModel):
    viewBox: list[float]
    districts: list[District]
    stations: list[Station]
