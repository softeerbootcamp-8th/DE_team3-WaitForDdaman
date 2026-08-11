import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { api } from "./api";
import { DetailTab } from "./components/DetailTab";
import { MainMapTab } from "./components/MainMapTab";
import { Toast } from "./components/Toast";
import { TopBar } from "./components/TopBar";
import type { BikeLists, ListName, MapData, SnapshotMeta } from "./types";

type Tab = "main" | "detail";

export default function App() {
  const [tab, setTab] = useState<Tab>("main");
  const [meta, setMeta] = useState<SnapshotMeta | null>(null);
  const [mapData, setMapData] = useState<MapData | null>(null);
  const [bikes, setBikes] = useState<BikeLists>({ source: [], dest: [] });
  const [loadError, setLoadError] = useState<string | null>(null);
  const [toast, setToast] = useState<ReactNode | null>(null);
  const toastTimer = useRef<number | undefined>(undefined);

  const showToast = useCallback((node: ReactNode) => {
    setToast(node);
    window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 3200);
  }, []);

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

  const handleTransfer = useCallback(async (ids: string[], fromList: ListName) => {
    const updated = await api.transfer(ids, fromList);
    setBikes(updated);
    setMeta((prev) => (prev ? { ...prev, capacity: { ...prev.capacity, used: updated.dest.length } } : prev));
  }, []);

  const handleCapacityChange = useCallback(async (max: number) => {
    const updatedMeta = await api.setCapacity(max);
    const updatedBikes = await api.getBikes();
    setMeta(updatedMeta);
    setBikes(updatedBikes);
  }, []);

  const handleExport = useCallback(async () => {
    const log = await api.getWorklog();
    const blob = new Blob([JSON.stringify(log, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "따맨_작업이력.json";
    a.click();
    URL.revokeObjectURL(url);
  }, []);

  const handleConfirm = useCallback(async () => {
    const result = await api.confirm();
    showToast(
      <>
        오늘 작업이력 {result.recorded.toLocaleString()}건 기록됨 (수거 {result.destCount.toLocaleString()}건){" "}
        <a onClick={handleExport}>내보내기</a>
      </>,
    );
  }, [showToast, handleExport]);

  return (
    <>
      <TopBar />
      <div className="page">
        <div className="tab-bar">
          <button className={`tab-btn${tab === "main" ? " active" : ""}`} onClick={() => setTab("main")}>
            메인
          </button>
          <button className={`tab-btn${tab === "detail" ? " active" : ""}`} onClick={() => setTab("detail")}>
            상세
          </button>
        </div>

        <div className={`tab-panel${tab === "main" ? " active" : ""}`}>
          {mapData ? <MainMapTab mapData={mapData} generatedAt={meta?.generatedAt} /> : <div className="updated">로딩 중…</div>}
        </div>

        <div className={`tab-panel${tab === "detail" ? " active" : ""}`}>
          {meta ? (
            <DetailTab
              meta={meta}
              bikes={bikes}
              onTransfer={handleTransfer}
              onCapacityChange={handleCapacityChange}
              onConfirm={handleConfirm}
            />
          ) : (
            <div className="updated">로딩 중…</div>
          )}
        </div>
      </div>

      <Toast message={toast} visible={toast !== null} />

      {loadError && (
        <div style={{ position: "fixed", bottom: 8, left: 8, color: "var(--danger)", fontSize: 12 }}>
          데이터를 불러오지 못했습니다: {loadError}
        </div>
      )}
    </>
  );
}
