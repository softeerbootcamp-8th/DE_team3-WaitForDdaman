export type Tier = "Normal" | "Warning" | "Critical";
export type Urgency = "여유있음" | "부족함" | "정보없음";

export type Side = "강남" | "강북";
export type RegionFilter = { kind: "all" } | { kind: "side"; side: Side } | { kind: "district"; name: string };

export interface Bike {
  id: string;
  station: string;
  district: string;
  region: Side;
  stationUrgency: Urgency;
  healthyRatio: number | null;
  tier: Tier;
  score: number;
  reason: string | null;
  distKm: number;
  durH: number | null;
  aging: number;
  history: string[];
}

export interface Capacity {
  max: number;
}

export interface SnapshotMeta {
  generatedAt: string;
  capacity: Capacity;
}

export interface District {
  name: string;
  path: string;
  cx: number;
  cy: number;
}

export interface MapStation {
  id: number;
  name: string;
  district: string;
  region: Side;
  x: number;
  y: number;
  holdNum: number;
  bikeCount: number;
  riskCount: number;
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
