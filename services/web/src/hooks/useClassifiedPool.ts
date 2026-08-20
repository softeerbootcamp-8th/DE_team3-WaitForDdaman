import { useMemo } from "react";
import type { UseCapacityResult } from "./useCapacity";
import type { Bike, RegionFilter } from "../types";
import { classifyPool } from "../utils/capacity";

export function useClassifiedPool(pool: Bike[], filter: RegionFilter, capacity: UseCapacityResult) {
  const capacityValue = capacity.capacityFor(filter);
  return useMemo(() => classifyPool(pool, filter, capacityValue), [pool, filter, capacityValue]);
}
