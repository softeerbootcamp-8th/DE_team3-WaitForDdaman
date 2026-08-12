import { useEffect, useMemo, useState } from "react";
import { api } from "./api";
import { SummaryRow } from "./components/SummaryRow";
import { TopBar } from "./components/TopBar";
import { useCapacity } from "./hooks/useCapacity";
import { useClassifiedPool } from "./hooks/useClassifiedPool";
import { DetailPage } from "./pages/DetailPage";
import { MainPage } from "./pages/MainPage";
import type { Bike, BikeLists, MapData, RegionFilter, SnapshotMeta } from "./types";
import { buildGuSideMap, totalBikeCount } from "./utils/regions";

type View = "main" | "detail";
const ALL_FILTER: RegionFilter = { kind: "all" };

function useSnapshotData() {
  const [meta, setMeta] = useState<SnapshotMeta | null>(null);
  const [mapData, setMapData] = useState<MapData | null>(null);
  const [bikes, setBikes] = useState<BikeLists>({ source: [], dest: [] });
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const [metaRes, mapRes, bikesRes] = await Promise.all([api.getMeta(), api.getMap(), api.getBikes()]);
        setMeta(metaRes);
        setMapData(mapRes);
        setBikes(bikesRes);
      } catch (e) {
        setLoadError(e instanceof Error ? e.message : String(e));
      }
    })();
  }, []);

  const pool = useMemo(() => [...bikes.source, ...bikes.dest], [bikes]);

  return { meta, mapData, pool, loadError };
}

export default function App() {
  const { meta, mapData, pool, loadError } = useSnapshotData();

  return (
    <>
      <TopBar generatedAt={meta?.generatedAt} />
      <div className="page">
        {meta && mapData ? <Dashboard meta={meta} mapData={mapData} pool={pool} /> : <div className="updated">로딩 중…</div>}
      </div>
      {loadError && (
        <div style={{ position: "fixed", bottom: 8, left: 8, color: "var(--danger)", fontSize: 12 }}>
          데이터를 불러오지 못했습니다: {loadError}
        </div>
      )}
    </>
  );
}

interface DashboardProps {
  meta: SnapshotMeta;
  mapData: MapData;
  pool: Bike[];
}

function Dashboard({ meta, mapData, pool }: DashboardProps) {
  const [view, setView] = useState<View>("main");
  const [regionFilter, setRegionFilter] = useState<RegionFilter>({ kind: "all" });
  const districtNames = useMemo(() => mapData.districts.map((d) => d.name), [mapData]);
  const guToSide = useMemo(() => buildGuSideMap(mapData.stations), [mapData]);
  const capacity = useCapacity(districtNames, guToSide, meta.capacity.max);

  const { dest, source } = useClassifiedPool(pool, ALL_FILTER, capacity);
  const totalBikes = useMemo(() => totalBikeCount(mapData.stations, ALL_FILTER), [mapData]);

  return (
    <>
      <div className="summary-row">
        <SummaryRow totalBikes={totalBikes} totalDest={dest.length} totalSource={source.length} overallCapacity={capacity.overall} />
      </div>

      <div className="tab-bar">
        <button className={`tab-btn${view === "main" ? " active" : ""}`} onClick={() => setView("main")}>
          메인
        </button>
        <button className={`tab-btn${view === "detail" ? " active" : ""}`} onClick={() => setView("detail")}>
          상세
        </button>
      </div>

      {view === "main" ? (
        <MainPage
          mapData={mapData}
          districtNames={districtNames}
          guToSide={guToSide}
          pool={pool}
          generatedAt={meta.generatedAt}
          regionFilter={regionFilter}
          onRegionFilterChange={setRegionFilter}
          onOpenDetail={() => setView("detail")}
          capacity={capacity}
        />
      ) : (
        <DetailPage
          meta={meta}
          mapData={mapData}
          districtNames={districtNames}
          guToSide={guToSide}
          pool={pool}
          regionFilter={regionFilter}
          onRegionFilterChange={setRegionFilter}
          capacity={capacity}
        />
      )}
    </>
  );
}
