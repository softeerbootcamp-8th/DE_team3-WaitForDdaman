import type { UseCapacityResult } from "../hooks/useCapacity";
import type { Bike, RegionFilter } from "../types";
import { countInCapacity } from "../utils/capacity";
import { SIDES } from "../utils/regions";

interface CapacityPanelProps {
  pool: Bike[];
  filter: RegionFilter;
  capacity: UseCapacityResult;
}

interface CapacityRowProps {
  label: string;
  used: number;
  max: number;
  onChange: (n: number) => void;
}

function CapacityRow({ label, used, max, onChange }: CapacityRowProps) {
  return (
    <div className="capacity-row">
      <div className="capacity-row-label">{label}</div>
      <div className="capacity-row-value">
        <span>{used.toLocaleString()} /</span>
        <input
          type="number"
          min={0}
          value={Math.round(max)}
          onChange={(e) => onChange(Number(e.target.value))}
        />
        <span>대</span>
      </div>
    </div>
  );
}

export function CapacityPanel({ pool, filter, capacity }: CapacityPanelProps) {
  const overallUsed = countInCapacity(pool, { kind: "all" }, capacity.overall);

  return (
    <div className="capacity-panel">
      <div className="list-head">
        <h2>Capacity</h2>
      </div>
      <div className="capacity-rows">
        <CapacityRow label="전체" used={overallUsed} max={capacity.overall} onChange={capacity.setOverall} />
        {SIDES.map((side) => {
          const max = capacity.getSideCapacity(side);
          const used = countInCapacity(pool, { kind: "side", side }, max);
          return (
            <CapacityRow key={side} label={side} used={used} max={max} onChange={(n) => capacity.setSideCapacity(side, n)} />
          );
        })}
        {filter.kind === "district" && (
          <CapacityRow
            label={filter.name}
            used={countInCapacity(pool, filter, capacity.getDistrictCapacity(filter.name))}
            max={capacity.getDistrictCapacity(filter.name)}
            onChange={(n) => capacity.setDistrictCapacity(filter.name, n)}
          />
        )}
      </div>
    </div>
  );
}
