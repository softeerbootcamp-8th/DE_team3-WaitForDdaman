import { useEffect, useMemo, useState } from "react";
import { api } from "./api";
import { SummaryRow } from "./components/SummaryRow";
import { TopBar } from "./components/TopBar";
import { useCapacity } from "./hooks/useCapacity";
import { useClassifiedPool } from "./hooks/useClassifiedPool";
import { DetailPage } from "./pages/DetailPage";
import { ConfirmedPage } from "./pages/ConfirmedPage";
import { MainPage } from "./pages/MainPage";
import { FieldPage } from "./pages/FieldPage";
import type { Bike, BikeLists, MapData, RegionFilter, SnapshotMeta } from "./types";
import { ALL_FILTER, buildGuSideMap, totalBikeCount } from "./utils/regions";

type View = "main" | "detail" | "confirmed" | "field";
const VIEW_ORDER: View[] = ["main", "detail", "confirmed", "field"];
const VIEW_LABEL: Record<View, string> = { main: "메인", detail: "상세", confirmed: "확정 내역", field: "기사님 화면" };

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
      <TopBar />
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

  const viewIndex = VIEW_ORDER.indexOf(view);
  const nextView = VIEW_ORDER[viewIndex + 1];

  return (
    <>
      <div className="summary-row">
        <SummaryRow totalBikes={totalBikes} totalDest={dest.length} totalSource={source.length} overallCapacity={capacity.overall} />
      </div>

      <div className="tab-bar">
        {VIEW_ORDER.map((v) => (
          <button key={v} className={`tab-btn${view === v ? " active" : ""}`} onClick={() => setView(v)}>
            {VIEW_LABEL[v]}
          </button>
        ))}
      </div>

      {view === "main" && (
        <MainPage
          mapData={mapData}
          districtNames={districtNames}
          guToSide={guToSide}
          pool={pool}
          regionFilter={regionFilter}
          onRegionFilterChange={setRegionFilter}
          onOpenDetail={() => setView("detail")}
          capacity={capacity}
        />
      )}
      {view === "detail" && (
        <DetailPage
          mapData={mapData}
          districtNames={districtNames}
          guToSide={guToSide}
          pool={pool}
          regionFilter={regionFilter}
          onRegionFilterChange={setRegionFilter}
          capacity={capacity}
        />
      )}
      {view === "confirmed" && <ConfirmedPage />}
      {view === "field" && <FieldPage />}

      {nextView && (
        <button className="next-step-btn" onClick={() => setView(nextView)}>
          <img className="next-step-mascot" src="/img/hamster-badge.png" alt="" />
          <span className="next-step-tag">시연</span>
          다음 단계로: {VIEW_LABEL[nextView]} →
        </button>
      )}
    </>
  );
}
