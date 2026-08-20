import { useMemo, useState } from "react";
import { BikeTable } from "../components/BikeTable";
import { CapacityPanel } from "../components/CapacityPanel";
import { Controls } from "../components/Controls";
import { DetailPanel } from "../components/DetailPanel";
import { DistrictMap } from "../components/DistrictMap";
import { RegionFilterBar } from "../components/RegionFilterBar";
import type { UseCapacityResult } from "../hooks/useCapacity";
import { useClassifiedPool } from "../hooks/useClassifiedPool";
import type { Bike, MapData, RegionFilter, Side } from "../types";
import { isDistrictActive, matchesRegion, regionLabel } from "../utils/regions";

interface DetailPageProps {
  mapData: MapData;
  districtNames: string[];
  guToSide: Record<string, Side>;
  pool: Bike[];
  regionFilter: RegionFilter;
  onRegionFilterChange: (filter: RegionFilter) => void;
  capacity: UseCapacityResult;
}

function passesFilter(bike: Bike, query: string, tiers: Set<string>, urgencies: Set<string>): boolean {
  if (tiers.size && !tiers.has(bike.risk_grade)) return false;
  if (urgencies.size && bike.station_urgency !== "정보없음" && !urgencies.has(bike.station_urgency)) return false;
  if (query) {
    const hay = (bike.bike_id + " " + bike.station_name + " " + bike.district).toLowerCase();
    if (!hay.includes(query)) return false;
  }
  return true;
}

export function DetailPage({
  mapData,
  districtNames,
  guToSide,
  pool,
  regionFilter,
  onRegionFilterChange,
  capacity,
}: DetailPageProps) {
  const [query, setQuery] = useState("");
  const [tiers, setTiers] = useState<Set<string>>(new Set(["Critical", "Warning"]));
  const [urgencies, setUrgencies] = useState<Set<string>>(new Set(["여유있음", "부족함"]));
  const [activeDetailId, setActiveDetailId] = useState<string | null>(null);

  const { dest, source } = useClassifiedPool(pool, regionFilter, capacity);

  const byId = useMemo(() => {
    const map = new Map<string, Bike>();
    dest.concat(source).forEach((b) => map.set(b.bike_id, b));
    return map;
  }, [dest, source]);

  const normalizedQuery = query.trim().toLowerCase();
  const filteredDest = useMemo(
    () => dest.filter((b) => passesFilter(b, normalizedQuery, tiers, urgencies)),
    [dest, normalizedQuery, tiers, urgencies],
  );
  const filteredSource = useMemo(
    () => source.filter((b) => passesFilter(b, normalizedQuery, tiers, urgencies)),
    [source, normalizedQuery, tiers, urgencies],
  );

  const activeBike = activeDetailId ? (byId.get(activeDetailId) ?? null) : null;

  const regionPool = useMemo(() => pool.filter((b) => matchesRegion(b, regionFilter)), [pool, regionFilter]);
  const { critCount, warningCount } = useMemo(
    () =>
      regionPool.reduce(
        (acc, b) => {
          if (b.risk_grade === "Critical") acc.critCount++;
          else if (b.risk_grade === "Warning") acc.warningCount++;
          return acc;
        },
        { critCount: 0, warningCount: 0 },
      ),
    [regionPool],
  );
  const poolNote = `${regionLabel(regionFilter)} 수거후보 Pool ${regionPool.length.toLocaleString()}대 (Critical ${critCount.toLocaleString()} · Warning ${warningCount.toLocaleString()})`;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>구별 수거 현황</h1>
        </div>
      </div>

      <div className="detail-region-layout">
        <RegionFilterBar filter={regionFilter} onChange={onRegionFilterChange} districtNames={districtNames} />
        <DistrictMap
          viewBox={mapData.view_box}
          districts={mapData.districts}
          variant="mini"
          highlight={(gu) => isDistrictActive(gu, regionFilter, guToSide)}
          onSelectDistrict={(gu) => onRegionFilterChange({ kind: "gu", name: gu })}
        />
      </div>

      <CapacityPanel pool={pool} filter={regionFilter} capacity={capacity} />

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
            <h2>대여중단</h2>
            <span className="count">{filteredSource.length.toLocaleString()}건</span>
          </div>
          <div className="list-cap-note">Capacity 초과분 · 대여중단 유지, 다음날 재평가</div>
          <div className="list-scroll">
            <BikeTable bikes={filteredSource} activeDetailId={activeDetailId} onRowClick={(bike) => setActiveDetailId(bike.bike_id)} />
          </div>
        </div>

        <div className="list-panel dest">
          <div className="list-head">
            <h2>수거 대상</h2>
            <span className="count">{filteredDest.length.toLocaleString()}건</span>
          </div>
          <div className="list-cap-note">Capacity 내 우선 수거 대상</div>
          <div className="list-scroll">
            <BikeTable bikes={filteredDest} activeDetailId={activeDetailId} onRowClick={(bike) => setActiveDetailId(bike.bike_id)} />
          </div>
        </div>

        <div className="detail-panel">
          <DetailPanel bike={activeBike} />
        </div>
      </div>
    </>
  );
}
