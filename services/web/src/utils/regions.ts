import type { MapStation, RegionFilter, Side } from "../types";

export const SIDES: readonly Side[] = ["강남", "강북"];

// 구-지역(강남/강북) 대응은 station_daily.region에서 그대로 나온다 — dim_district에는
// region이 없어서, 지도 위 구 강조표시(gu 이름만 아는 경우)에 쓸 매핑을 대여소 목록에서 파생시킨다.
export function buildGuSideMap(stations: MapStation[]): Record<string, Side> {
  const map: Record<string, Side> = {};
  stations.forEach((s) => {
    if (!(s.district in map)) map[s.district] = s.region;
  });
  return map;
}

export function matchesRegion(entity: { district: string; region: Side }, filter: RegionFilter): boolean {
  if (filter.kind === "all") return true;
  if (filter.kind === "side") return entity.region === filter.side;
  return entity.district === filter.name;
}

// 지도에서 구를 강조 표시할지 여부 (구 이름만 알 때 — DistrictMap의 highlight 콜백용).
// "전체" 필터에서는 특정 구를 강조하지 않는다.
export function isDistrictActive(gu: string, filter: RegionFilter, guToSide: Record<string, Side>): boolean {
  if (filter.kind === "all") return false;
  if (filter.kind === "side") return guToSide[gu] === filter.side;
  return gu === filter.name;
}

export function regionLabel(filter: RegionFilter): string {
  if (filter.kind === "all") return "전체";
  if (filter.kind === "side") return filter.side;
  return filter.name;
}

export function totalBikeCount(stations: MapStation[], filter: RegionFilter): number {
  return stations.filter((s) => matchesRegion(s, filter)).reduce((sum, s) => sum + s.bike_cnt, 0);
}
