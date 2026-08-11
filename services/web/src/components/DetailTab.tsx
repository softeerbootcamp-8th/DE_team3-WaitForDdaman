import { useMemo, useState } from "react";
import type { Bike, BikeLists, ListName, SnapshotMeta } from "../types";
import { BikeTable } from "./BikeTable";
import { CapacityCard } from "./CapacityCard";
import { Controls } from "./Controls";
import { DetailPanel } from "./DetailPanel";
import { KpiRow } from "./KpiRow";

interface DetailTabProps {
  meta: SnapshotMeta;
  bikes: BikeLists;
  onTransfer: (ids: string[], fromList: ListName) => void;
  onCapacityChange: (max: number) => void;
  onConfirm: () => void;
}

function passesFilter(bike: Bike, query: string, tiers: Set<string>, urgencies: Set<string>): boolean {
  if (tiers.size && !tiers.has(bike.tier)) return false;
  if (urgencies.size && bike.stationUrgency !== "정보없음" && !urgencies.has(bike.stationUrgency)) return false;
  if (query) {
    const hay = (bike.id + " " + bike.station + " " + bike.gu).toLowerCase();
    if (!hay.includes(query)) return false;
  }
  return true;
}

export function DetailTab({ meta, bikes, onTransfer, onCapacityChange, onConfirm }: DetailTabProps) {
  const [query, setQuery] = useState("");
  const [tiers, setTiers] = useState<Set<string>>(new Set(["Critical", "Risk"]));
  const [urgencies, setUrgencies] = useState<Set<string>>(new Set(["여유있음", "부족함"]));
  const [selected, setSelected] = useState<{ source: Set<string>; dest: Set<string> }>({
    source: new Set(),
    dest: new Set(),
  });
  const [activeDetailId, setActiveDetailId] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);

  const byId = useMemo(() => {
    const map = new Map<string, Bike>();
    bikes.dest.concat(bikes.source).forEach((b) => map.set(b.id, b));
    return map;
  }, [bikes]);

  const normalizedQuery = query.trim().toLowerCase();
  const filteredSource = useMemo(
    () => bikes.source.filter((b) => passesFilter(b, normalizedQuery, tiers, urgencies)),
    [bikes.source, normalizedQuery, tiers, urgencies],
  );
  const filteredDest = useMemo(
    () => bikes.dest.filter((b) => passesFilter(b, normalizedQuery, tiers, urgencies)),
    [bikes.dest, normalizedQuery, tiers, urgencies],
  );

  const activeBike = activeDetailId ? (byId.get(activeDetailId) ?? null) : null;

  function toggleSelect(listName: ListName, id: string) {
    setSelected((prev) => {
      const next = new Set(prev[listName]);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return { ...prev, [listName]: next };
    });
  }

  function moveSelected(fromList: ListName) {
    const ids = Array.from(selected[fromList]);
    if (ids.length === 0) return;
    onTransfer(ids, fromList);
    setSelected((prev) => ({ ...prev, [fromList]: new Set<string>() }));
  }

  function handleConfirm() {
    onConfirm();
    setSelected({ source: new Set(), dest: new Set() });
    setConfirming(true);
    window.setTimeout(() => setConfirming(false), 1400);
  }

  const poolTotal = bikes.source.length + bikes.dest.length;
  const poolNote =
    `수거후보 Pool 전체 ${poolTotal.toLocaleString()}대 ` +
    `(Critical ${(meta.tierCounts.Critical ?? 0).toLocaleString()} · Risk ${(meta.tierCounts.Risk ?? 0).toLocaleString()}) ` +
    `— Normal ${(meta.tierCounts.Normal ?? 0).toLocaleString()}대는 조치없음으로 제외됨`;

  const toDestDisabled = selected.source.size === 0;
  const toSourceDisabled = selected.dest.size === 0;
  const confirmDisabled = bikes.dest.length === 0 && bikes.source.length === 0;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>수거 우선순위 콘솔</h1>
          <div className="sub">위험도(risk_score) 기반 수거 후보 선정 · 대여소 시급도 반영 · Capacity 내 확정</div>
        </div>
        <div className="updated">데이터 기준 {meta.generatedAt} · risk_score 파이프라인 1회 산출</div>
      </div>

      <div className="kpi-row">
        <KpiRow kpi={meta.kpi} />
        <CapacityCard capacity={meta.capacity} onChange={onCapacityChange} />
      </div>

      <Controls
        query={query}
        onQueryChange={setQuery}
        tiers={tiers}
        onTiersChange={setTiers}
        urgencies={urgencies}
        onUrgenciesChange={setUrgencies}
      />
      <div className="filter-note">{poolNote}</div>

      <div className="workspace">
        <div className="list-panel">
          <div className="list-head">
            <h2>수거 후보 Pool (대여중단)</h2>
            <span className="count">{filteredSource.length.toLocaleString()}건</span>
          </div>
          <div className="list-cap-note">Capacity 초과분 · 대여중단 유지, 다음날 재평가</div>
          <div className="list-scroll">
            <BikeTable
              listName="source"
              bikes={filteredSource}
              selected={selected.source}
              onToggleSelect={(id) => toggleSelect("source", id)}
              activeDetailId={activeDetailId}
              onRowClick={(bike) => setActiveDetailId(bike.id)}
            />
          </div>
        </div>

        <div className="transfer">
          <button
            className="xfer-btn"
            disabled={toDestDisabled}
            onClick={() => moveSelected("source")}
            title="선택 항목을 오늘 확정 대상으로 이동"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2.2} strokeLinecap="round" strokeLinejoin="round">
              <path d="M5 12h14M13 6l6 6-6 6" />
            </svg>
          </button>
          <button
            className="xfer-btn"
            disabled={toSourceDisabled}
            onClick={() => moveSelected("dest")}
            title="선택 항목을 대기 Pool로 되돌리기"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2.2} strokeLinecap="round" strokeLinejoin="round">
              <path d="M19 12H5M11 18l-6-6 6-6" />
            </svg>
          </button>
          <button className="confirm-btn" disabled={confirmDisabled} onClick={handleConfirm}>
            {confirming ? "확정됨 ✓" : "확정"}
          </button>
        </div>

        <div className="list-panel dest">
          <div className="list-head">
            <h2>오늘 수거 확정 대상</h2>
            <span className="count">{filteredDest.length.toLocaleString()}건</span>
          </div>
          <div className="list-cap-note">확정 시 작업이력(수거)으로 기록됩니다</div>
          <div className="list-scroll">
            <BikeTable
              listName="dest"
              bikes={filteredDest}
              selected={selected.dest}
              onToggleSelect={(id) => toggleSelect("dest", id)}
              activeDetailId={activeDetailId}
              onRowClick={(bike) => setActiveDetailId(bike.id)}
            />
          </div>
        </div>

        <div className="detail-panel">
          <DetailPanel bike={activeBike} />
        </div>
      </div>
    </>
  );
}
