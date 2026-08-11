from typing import Literal, Optional

from pydantic import BaseModel


class Bike(BaseModel):
    id: str
    station: str
    gu: str
    stationUrgency: str
    healthyRatio: Optional[float]
    tier: str
    score: float
    reason: str
    distKm: float
    durH: float
    priorFailCount: int
    daysSinceLastFail: int
    history: list[str]


class BikeLists(BaseModel):
    source: list[Bike]
    dest: list[Bike]


class Capacity(BaseModel):
    used: int
    max: int


class Kpi(BaseModel):
    today: Optional[float]
    yesterday: Optional[float]
    monthly: Optional[float]


class SnapshotMeta(BaseModel):
    generatedAt: str
    capacity: Capacity
    kpi: Kpi
    poolSize: int
    tierCounts: dict[str, int]
    actionCounts: dict[str, int]


class District(BaseModel):
    name: str
    path: str
    cx: float
    cy: float


class Station(BaseModel):
    id: int
    name: str
    gu: str
    x: float
    y: float
    bikeCount: int
    riskCount: int
    healthyRatio: float
    urgency: str


class MapData(BaseModel):
    viewBox: list[float]
    districts: list[District]
    stations: list[Station]


class TransferRequest(BaseModel):
    ids: list[str]
    fromList: Literal["source", "dest"]


class CapacityUpdateRequest(BaseModel):
    max: int


class WorklogEntry(BaseModel):
    date: str
    bikeId: str
    station: str
    action: str
    tier: str
    score: float
    confirmedAt: str


class ConfirmResponse(BaseModel):
    recorded: int
    destCount: int
    sourceCount: int
