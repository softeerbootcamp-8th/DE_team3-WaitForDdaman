import { useEffect, useRef, useState } from "react";

interface ControlsProps {
  query: string;
  onQueryChange: (v: string) => void;
  tiers: Set<string>;
  onTiersChange: (tiers: Set<string>) => void;
  urgencies: Set<string>;
  onUrgenciesChange: (urgencies: Set<string>) => void;
}

function toggleValue(set: Set<string>, value: string): Set<string> {
  const next = new Set(set);
  if (next.has(value)) next.delete(value);
  else next.add(value);
  return next;
}

export function Controls({
  query,
  onQueryChange,
  tiers,
  onTiersChange,
  urgencies,
  onUrgenciesChange,
}: ControlsProps) {
  const [open, setOpen] = useState(false);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, []);

  return (
    <div className="controls">
      <div className="search-box">
        <svg viewBox="0 0 24 24" fill="none" strokeWidth={2} strokeLinecap="round">
          <circle cx="11" cy="11" r="7" />
          <path d="M21 21l-4.3-4.3" />
        </svg>
        <input
          type="text"
          placeholder="자전거ID, 대여소명으로 검색"
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
        />
      </div>
      <div
        className="filter-box"
        ref={boxRef}
        onClick={(e) => {
          if ((e.target as HTMLElement).closest(".filter-pop")) {
            e.stopPropagation();
            return;
          }
          setOpen((v) => !v);
        }}
      >
        <svg viewBox="0 0 24 24" fill="none" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
          <path d="M4 5h16M7 12h10M10 19h4" />
        </svg>
        필터링
        <div className={`filter-pop${open ? " open" : ""}`}>
          <div className="grp-title">위험 등급</div>
          <label>
            <input
              type="checkbox"
              checked={tiers.has("Critical")}
              onChange={() => onTiersChange(toggleValue(tiers, "Critical"))}
            />{" "}
            Critical
          </label>
          <label>
            <input
              type="checkbox"
              checked={tiers.has("Warning")}
              onChange={() => onTiersChange(toggleValue(tiers, "Warning"))}
            />{" "}
            Warning
          </label>
          <div className="grp-title">정상 자전거 거치율</div>
          <label>
            <input
              type="checkbox"
              checked={urgencies.has("여유있음")}
              onChange={() => onUrgenciesChange(toggleValue(urgencies, "여유있음"))}
            />{" "}
            여유있음
          </label>
          <label>
            <input
              type="checkbox"
              checked={urgencies.has("부족함")}
              onChange={() => onUrgenciesChange(toggleValue(urgencies, "부족함"))}
            />{" "}
            부족함
          </label>
        </div>
      </div>
    </div>
  );
}
