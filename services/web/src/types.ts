export type Tier = "Normal" | "Warning" | "Critical";
export type Urgency = "여유있음" | "부족함" | "정보없음";

export type Side = "강남" | "강북";
export type RegionFilter = { kind: "all" } | { kind: "side"; side: Side } | { kind: "gu"; name: string };

export interface Bike {
  bikeId: string;
  stationName: string;
  district: string;
  region: Side;
  stationUrgency: Urgency;
  healthyRatio: number | null;
  riskGrade: Tier;
  riskScore: number;
  distKm: number;
  aging: number;
  failHistory: string[];
}

export interface Capacity {
  max: number;
}

export interface SnapshotMeta {
  snapshotDate: string;
  capacity: Capacity;
}

export interface District {
  name: string;
  path: string;
  cx: number;
  cy: number;
}

export interface MapStation {
  stationId: string;
  stationName: string;
  district: string;
  region: Side;
  x: number;
  y: number;
  holdNum: number;
  bikeCnt: number;
  riskCnt: number;
  healthyRatio: number;
  urgency: string;
}

export interface MapData {
  viewBox: [number, number];
  districts: District[];
  stations: MapStation[];
}

export interface BikeLists {
  source: Bike[];
  dest: Bike[];
}
