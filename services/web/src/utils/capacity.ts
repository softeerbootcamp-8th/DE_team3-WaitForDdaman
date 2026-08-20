import type { Bike, RegionFilter } from "../types";
import { matchesRegion } from "./regions";

export function evenSplit(total: number, keys: string[]): Record<string, number> {
  const per = keys.length > 0 ? total / keys.length : 0;
  const result: Record<string, number> = {};
  keys.forEach((key) => (result[key] = per));
  return result;
}

export function classifyPool(
  bikes: Bike[],
  filter: RegionFilter,
  capacity: number,
): { dest: Bike[]; source: Bike[] } {
  const inRegion = bikes.filter((b) => matchesRegion(b, filter));
  const sorted = [...inRegion].sort((a, b) => b.riskScore - a.riskScore);
  const n = Math.max(0, Math.min(sorted.length, Math.round(capacity)));
  return { dest: sorted.slice(0, n), source: sorted.slice(n) };
}

// dest 개수만 필요한 경우 classifyPool처럼 정렬할 필요 없이 capacity로 그냥 자르면 된다.
export function countInCapacity(bikes: Bike[], filter: RegionFilter, capacity: number): number {
  const inRegionCount = bikes.reduce((sum, b) => sum + (matchesRegion(b, filter) ? 1 : 0), 0);
  return Math.max(0, Math.min(inRegionCount, Math.round(capacity)));
}
