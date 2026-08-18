import { useCallback, useMemo, useState } from "react";
import type { RegionFilter, Side } from "../types";
import { evenSplit } from "../utils/capacity";

export interface UseCapacityResult {
  overall: number;
  setOverall: (n: number) => void;

  getSideCapacity: (side: Side) => number;
  setSideCapacity: (side: Side, n: number) => void;

  getDistrictCapacity: (district: string) => number;
  setDistrictCapacity: (district: string, n: number) => void;

  capacityFor: (filter: RegionFilter) => number;
}

function clamp(n: number): number {
  return Number.isFinite(n) ? Math.max(0, n) : 0;
}

// 대상 그룹(keys)의 현재 합을 target에 맞춰 비례적으로 재분배한다.
// 상위(전체/지역) capacity를 바꾸면 그 안의 구들이 같은 비율로 늘거나 줄어서,
// 구별 capacity를 바꾸면 그 구가 속한 지역·전체 합계가 자동으로 따라온다 (파생값).
function scaleToTarget(values: Record<string, number>, keys: string[], target: number): Record<string, number> {
  const current = keys.reduce((sum, key) => sum + (values[key] ?? 0), 0);
  const next = { ...values };
  if (current > 0) {
    const factor = target / current;
    keys.forEach((key) => (next[key] = (values[key] ?? 0) * factor));
  } else {
    const per = keys.length > 0 ? target / keys.length : 0;
    keys.forEach((key) => (next[key] = per));
  }
  return next;
}

export function useCapacity(
  districtNames: string[],
  districtToSide: Record<string, Side>,
  initialOverall: number,
): UseCapacityResult {
  // 구별 capacity가 유일한 상태(source of truth)다. 지역/전체 capacity는 여기서 합산해 파생시킨다.
  const [districtCapacities, setDistrictCapacities] = useState<Record<string, number>>(() =>
    evenSplit(clamp(initialOverall), districtNames),
  );

  const setDistrictCapacity = useCallback(
    (district: string, n: number) => setDistrictCapacities((prev) => ({ ...prev, [district]: clamp(n) })),
    [],
  );
  const getDistrictCapacity = useCallback(
    (district: string) => districtCapacities[district] ?? 0,
    [districtCapacities],
  );

  const districtNamesBySide = useCallback(
    (side: Side) => districtNames.filter((district) => districtToSide[district] === side),
    [districtNames, districtToSide],
  );

  const setSideCapacity = useCallback(
    (side: Side, n: number) => {
      const keys = districtNamesBySide(side);
      setDistrictCapacities((prev) => scaleToTarget(prev, keys, clamp(n)));
    },
    [districtNamesBySide],
  );
  const getSideCapacity = useCallback(
    (side: Side) => districtNamesBySide(side).reduce((sum, district) => sum + (districtCapacities[district] ?? 0), 0),
    [districtNamesBySide, districtCapacities],
  );

  const setOverall = useCallback(
    (n: number) => setDistrictCapacities((prev) => scaleToTarget(prev, districtNames, clamp(n))),
    [districtNames],
  );
  const overall = useMemo(
    () => districtNames.reduce((sum, district) => sum + (districtCapacities[district] ?? 0), 0),
    [districtNames, districtCapacities],
  );

  const capacityFor = useCallback(
    (filter: RegionFilter) => {
      if (filter.kind === "all") return overall;
      if (filter.kind === "side") return getSideCapacity(filter.side);
      return getDistrictCapacity(filter.name);
    },
    [overall, getSideCapacity, getDistrictCapacity],
  );

  return useMemo(
    () => ({
      overall, setOverall, getSideCapacity, setSideCapacity,
      getDistrictCapacity, setDistrictCapacity, capacityFor,
    }),
    [overall, setOverall, getSideCapacity, setSideCapacity, getDistrictCapacity, setDistrictCapacity, capacityFor],
  );
}
