from typing import Literal, Optional

from pydantic import BaseModel

Region = Literal["강남", "강북"]
Tier = Literal["Normal", "Warning", "Critical"]


class Bike(BaseModel):
    id: str
    station: Optional[str] = None
    district: Optional[str] = None
    region: Optional[Region] = None
    stationUrgency: str
    healthyRatio: Optional[float]
    tier: Tier
    score: float
    reason: Optional[str] = None
    distKm: float
    durH: Optional[float] = None
    aging: float
    history: list[str]


class BikeLists(BaseModel):
    source: list[Bike]
    dest: list[Bike]


class Capacity(BaseModel):
    max: int


class SnapshotMeta(BaseModel):
    generatedAt: str
    capacity: Capacity


class District(BaseModel):
    name: str
    path: str
    cx: float
    cy: float


class Station(BaseModel):
    id: int
    name: str
    district: str
    region: Region
    x: float
    y: float
    holdNum: int
    bikeCount: int
    riskCount: int
    healthyRatio: float
    urgency: str


class MapData(BaseModel):
    viewBox: list[float]
    districts: list[District]
    stations: list[Station]
