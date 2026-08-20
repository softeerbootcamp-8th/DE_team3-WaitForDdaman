export type Tier = "Normal" | "Warning" | "Critical";
export type Urgency = "여유있음" | "부족함" | "정보없음";

export type Side = "강남" | "강북";
export type RegionFilter = { kind: "all" } | { kind: "side"; side: Side } | { kind: "gu"; name: string };

export interface Bike {
  bike_id: string;
  station_name: string;
  district: string;
  region: Side;
  station_urgency: Urgency;
  healthy_ratio: number | null;
  risk_grade: Tier;
  risk_score: number;
  dist_km: number;
  aging: number;
  fail_history: string[];
}

export interface Capacity {
  max: number;
}

export interface SnapshotMeta {
  snapshot_date: string;
  capacity: Capacity;
}

export interface District {
  name: string;
  path: string;
  cx: number;
  cy: number;
}

export interface MapStation {
  station_id: string;
  station_name: string;
  district: string;
  region: Side;
  x: number;
  y: number;
  hold_num: number;
  bike_cnt: number;
  risk_cnt: number;
  healthy_ratio: number;
  urgency: string;
}

export interface MapData {
  view_box: [number, number];
  districts: District[];
  stations: MapStation[];
}

export interface BikeLists {
  source: Bike[];
  dest: Bike[];
}
