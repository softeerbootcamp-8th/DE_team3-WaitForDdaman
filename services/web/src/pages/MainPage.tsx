import { useMemo } from "react";
import { DistrictMap } from "../components/DistrictMap";
import { DistrictStatsCard } from "../components/DistrictStatsCard";
import { RegionFilterBar } from "../components/RegionFilterBar";
import type { UseCapacityResult } from "../hooks/useCapacity";
import { useClassifiedPool } from "../hooks/useClassifiedPool";
import type { Bike, MapData, RegionFilter, Side } from "../types";
import { isDistrictActive, regionLabel, totalBikeCount } from "../utils/regions";

interface MainPageProps {
  mapData: MapData;
  districtNames: string[];
  guToSide: Record<string, Side>;
  pool: Bike[];
  generatedAt?: string;
  regionFilter: RegionFilter;
  onRegionFilterChange: (filter: RegionFilter) => void;
  onOpenDetail: () => void;
  capacity: UseCapacityResult;
}

export function MainPage({
  mapData,
  districtNames,
  guToSide,
  pool,
  generatedAt,
  regionFilter,
  onRegionFilterChange,
  onOpenDetail,
  capacity,
}: MainPageProps) {
  const riskStations = useMemo(() => mapData.stations.filter((s) => s.riskCount > 0), [mapData]);
  const top10 = useMemo(
    () => [...riskStations].sort((a, b) => b.riskCount - a.riskCount).slice(0, 10),
    [riskStations],
  );

  const { dest, source } = useClassifiedPool(pool, regionFilter, capacity);

  function handleDistrictClick(gu: string) {
    if (regionFilter.kind === "gu" && regionFilter.name === gu) {
      onOpenDetail();
    } else {
      onRegionFilterChange({ kind: "gu", name: gu });
    }
  }

  return (
    <>
      <div className="page-head">
        <div>
          <h1>대여소 현황</h1>
          <div className="sub">지역(강남/강북) 또는 구를 클릭하면 해당 지역의 자전거 현황을 확인할 수 있습니다</div>
        </div>
      </div>

      <RegionFilterBar filter={regionFilter} onChange={onRegionFilterChange} districtNames={districtNames} />

      <div className="map-layout">
        <DistrictMap
          viewBox={mapData.viewBox}
          districts={mapData.districts}
          stations={mapData.stations}
          variant="full"
          highlight={(gu) => isDistrictActive(gu, regionFilter, guToSide)}
          onSelectDistrict={handleDistrictClick}
        />
        <div className="map-side">
          <div className="map-legend">
            <div className="legend-title">위험 자전거 밀집도</div>
            <div className="legend-gradient" />
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span>적음</span>
              <span>많음</span>
            </div>
          </div>

          {regionFilter.kind !== "all" && (
            <DistrictStatsCard
              label={regionLabel(regionFilter)}
              totalCount={totalBikeCount(mapData.stations, regionFilter)}
              destCount={dest.length}
              sourceCount={source.length}
              onOpenDetail={onOpenDetail}
            />
          )}

          <div className="top-station-panel">
            <div className="list-head">
              <h2>위험 대여소 TOP 10</h2>
            </div>
            <ol className="top-station-list">
              {top10.map((s, i) => (
                <li key={s.id} onClick={() => handleDistrictClick(s.gu)}>
                  <span className="rank">{i + 1}</span>
                  <span className="st-name">
                    {s.name.trim()}
                    <br />
                    <span className="st-gu">{s.gu}</span>
                  </span>
                  <span className="st-count">{s.riskCount}대</span>
                </li>
              ))}
            </ol>
          </div>
        </div>
      </div>
    </>
  );
}
