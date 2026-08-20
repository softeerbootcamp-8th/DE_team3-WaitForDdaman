import type { RegionFilter, Side } from "../types";
import { SIDES } from "../utils/regions";

interface RegionFilterBarProps {
  filter: RegionFilter;
  onChange: (filter: RegionFilter) => void;
  districtNames: string[];
}

export function RegionFilterBar({ filter, onChange, districtNames }: RegionFilterBarProps) {
  const sortedNames = [...districtNames].sort((a, b) => a.localeCompare(b));

  function handleSideClick(side: Side) {
    onChange(filter.kind === "side" && filter.side === side ? { kind: "all" } : { kind: "side", side });
  }

  return (
    <div className="region-filter-bar">
      <button className={`region-btn${filter.kind === "all" ? " active" : ""}`} onClick={() => onChange({ kind: "all" })}>
        전체
      </button>
      {SIDES.map((side) => (
        <button
          key={side}
          className={`region-btn${filter.kind === "side" && filter.side === side ? " active" : ""}`}
          onClick={() => handleSideClick(side)}
        >
          {side}
        </button>
      ))}
      <select
        className="region-gu-select"
        value={filter.kind === "gu" ? filter.name : ""}
        onChange={(e) => (e.target.value ? onChange({ kind: "gu", name: e.target.value }) : onChange({ kind: "all" }))}
      >
        <option value="">구 선택…</option>
        {sortedNames.map((name) => (
          <option key={name} value={name}>
            {name}
          </option>
        ))}
      </select>
    </div>
  );
}
