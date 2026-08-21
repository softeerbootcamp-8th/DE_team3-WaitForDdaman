import type { UseCapacityResult } from "../hooks/useCapacity";
import type { Bike, ConfirmResponse, RegionFilter } from "../types";
import { countInCapacity } from "../utils/capacity";
import { fmtStamp } from "../utils/format";
import { SIDES } from "../utils/regions";

interface CapacityPanelProps {
  pool: Bike[];
  filter: RegionFilter;
  capacity: UseCapacityResult;
  /** 확정 대상 대수 = 전체 기준 수거 대상(헤더의 총 수거대수와 같은 값) */
  confirmCount: number;
  /** 서버에 저장된 확정 내역 (없으면 null) */
  confirmed: ConfirmResponse | null;
  submitState: "idle" | "saving" | "error";
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
  confirmed,
  submitState,
  onConfirm,
}: CapacityPanelProps) {
  const overallUsed = countInCapacity(pool, { kind: "all" }, capacity.overall);
  // 확정해둔 대수와 지금 화면의 대상 대수가 다르면, 저장된 게 최신이 아니라는 뜻이다.
  const stale = confirmed !== null && confirmed.confirmed !== confirmCount;

  return (
    <div className="capacity-panel">
      <div className="list-head">
        <h2>Capacity</h2>
        {/* 확정은 지역 필터와 무관하게 전체 capacity 기준이라, 필터에 딸린 목록 헤더가 아니라
            그 값을 조절하는 이 패널에 둔다. */}
        <div className="list-head-actions">
          {submitState === "error" ? (
            <span className="confirm-note error">확정 실패, 다시 시도해 주세요</span>
          ) : (
            confirmed?.actioned_at && (
              <span className={`confirm-note${stale ? " stale" : ""}`}>
                {confirmed.confirmed.toLocaleString()}대 확정됨 · {fmtStamp(confirmed.actioned_at)}
                {stale && ` (현재 ${confirmCount.toLocaleString()}대와 다름)`}
              </span>
            )
          )}
          <button
            className="confirm-btn"
            disabled={submitState === "saving" || confirmCount === 0}
            onClick={onConfirm}
            title="전체 capacity 기준 수거 대상을 확정합니다 (지역 필터·검색과 무관)"
          >
            {submitState === "saving" ? "확정 중…" : `확인 (${confirmCount.toLocaleString()}대)`}
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
