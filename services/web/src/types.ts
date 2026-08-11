export type Tier = "Normal" | "Risk" | "Critical";
export type Urgency = "여유있음" | "부족함" | "정보없음";
export type ListName = "source" | "dest";

export interface Bike {
  id: string;
  station: string;
  gu: string;
  stationUrgency: Urgency;
  healthyRatio: number | null;
  tier: Tier;
  score: number;
  reason: string;
  distKm: number;
  durH: number;
  priorFailCount: number;
  daysSinceLastFail: number;
  history: string[];
}

export interface Capacity {
  used: number;
  max: number;
}

export interface Kpi {
  today: number | null;
  yesterday: number | null;
  monthly: number | null;
}

export interface SnapshotMeta {
  generatedAt: string;
  capacity: Capacity;
  kpi: Kpi;
  poolSize: number;
  tierCounts: Record<string, number>;
  actionCounts: Record<string, number>;
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
  gu: string;
  x: number;
  y: number;
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

export interface ConfirmResult {
  recorded: number;
  destCount: number;
  sourceCount: number;
}

export interface WorklogEntry {
  date: string;
  bikeId: string;
  station: string;
  action: string;
  tier: string;
  score: number;
  confirmedAt: string;
}
