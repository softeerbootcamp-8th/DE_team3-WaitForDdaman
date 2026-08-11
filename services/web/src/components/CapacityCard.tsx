import type { Capacity } from "../types";

interface CapacityCardProps {
  capacity: Capacity;
  onChange: (max: number) => void;
}

export function CapacityCard({ capacity, onChange }: CapacityCardProps) {
  function handleClick() {
    const next = window.prompt("오늘 정비소 Capacity(대)를 입력하세요", String(capacity.max));
    if (!next) return;
    const n = parseInt(next, 10);
    if (Number.isNaN(n)) return;
    onChange(Math.max(0, n));
  }

  return (
    <div className="capacity-card" onClick={handleClick} title="클릭해서 오늘 정비소 Capacity를 조정하세요">
      <div className="k-label">
        오늘 정비소 Capacity <span className="edit-pill">✎ 수정 가능</span>
      </div>
      <div className="k-value">
        {capacity.used.toLocaleString()} / {capacity.max.toLocaleString()}대
      </div>
    </div>
  );
}
