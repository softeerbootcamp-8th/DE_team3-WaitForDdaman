import type { UseCapacityResult } from "../hooks/useCapacity";
import type { Bike, RegionFilter } from "../types";
import { countInCapacity } from "../utils/capacity";
import { SIDES } from "../utils/regions";

export type ConfirmState =
  | { kind: "idle" }
  | { kind: "saving" }
  | { kind: "done"; count: number }
  | { kind: "error" };

interface CapacityPanelProps {
  pool: Bike[];
  filter: RegionFilter;
  capacity: UseCapacityResult;
  /** 확정 대상 대수 = 전체 기준 수거 대상(헤더의 총 수거대수와 같은 값) */
  confirmCount: number;
  confirmState: ConfirmState;
  onConfirm: () => void;
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

export function CapacityPanel({
  pool,
  filter,
  capacity,
  confirmCount,
  confirmState,
  onConfirm,
}: CapacityPanelProps) {
  const overallUsed = countInCapacity(pool, { kind: "all" }, capacity.overall);

  return (
    <div className="capacity-panel">
      <div className="list-head">
        <h2>Capacity</h2>
        {/* 확정은 지역 필터와 무관하게 전체 capacity 기준이라, 필터에 딸린 목록 헤더가 아니라
            그 값을 조절하는 이 패널에 둔다. */}
        <div className="list-head-actions">
          {confirmState.kind === "done" && (
            <span className="confirm-note">{confirmState.count.toLocaleString()}대 확정 기록됨</span>
          )}
          {confirmState.kind === "error" && (
            <span className="confirm-note error">확정 실패, 다시 시도해 주세요</span>
          )}
          <button
            className="confirm-btn"
            disabled={confirmState.kind === "saving" || confirmCount === 0}
            onClick={onConfirm}
            title="전체 capacity 기준 수거 대상을 확정합니다 (지역 필터·검색과 무관)"
          >
            {confirmState.kind === "saving" ? "확정 중…" : `확인 (${confirmCount.toLocaleString()}대)`}
          </button>
        </div>
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
        {filter.kind === "gu" && (
          <CapacityRow
            label={filter.name}
            used={countInCapacity(pool, filter, capacity.getGuCapacity(filter.name))}
            max={capacity.getGuCapacity(filter.name)}
            onChange={(n) => capacity.setGuCapacity(filter.name, n)}
          />
        )}
      </div>
    </div>
  );
}
