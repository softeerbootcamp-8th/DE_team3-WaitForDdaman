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
  const sorted = [...inRegion].sort((a, b) => b.risk_score - a.risk_score);
  const n = Math.max(0, Math.min(sorted.length, Math.round(capacity)));
  return { dest: sorted.slice(0, n), source: sorted.slice(n) };
}

// 확정할 수거 목록. 구별 capacity가 유일한 상태(useCapacity)이고 전체/지역 값은 그 합계라,
// "조절한 최종값"을 지키는 집합은 구별로 그 구의 capacity만큼 뽑아 합친 것이다.
// 전역 top-N과는 개수는 같아도 구성이 다르다 - 전역 정렬은 구 경계를 무시해서 구별 capacity를
// 위반한다. 화면의 검색/등급 필터는 탐색용이라 여기 반영하지 않는다(검색어를 넣은 채로 눌러
// 몇 대만 확정되는 사고를 막는다).
export function collectConfirmTargets(
  bikes: Bike[],
  districtNames: string[],
  capacityFor: (filter: RegionFilter) => number,
): Bike[] {
  return districtNames.flatMap((gu) => {
    const filter: RegionFilter = { kind: "gu", name: gu };
    return classifyPool(bikes, filter, capacityFor(filter)).dest;
  });
}

// dest 개수만 필요한 경우 classifyPool처럼 정렬할 필요 없이 capacity로 그냥 자르면 된다.
export function countInCapacity(bikes: Bike[], filter: RegionFilter, capacity: number): number {
  const inRegionCount = bikes.reduce((sum, b) => sum + (matchesRegion(b, filter) ? 1 : 0), 0);
  return Math.max(0, Math.min(inRegionCount, Math.round(capacity)));
}
