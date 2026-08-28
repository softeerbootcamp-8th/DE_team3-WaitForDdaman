import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { BikeTable } from "../components/BikeTable";
import { DetailPanel } from "../components/DetailPanel";
import type { ConfirmedBikes } from "../types";
import { fmtStamp } from "../utils/format";

export function ConfirmedPage() {
  // 가장 최근 확정 배치만 보여주므로 날짜 필터나 페이지네이션이 필요 없다.
  const [data, setData] = useState<ConfirmedBikes | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [activeDetailId, setActiveDetailId] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    api
      .getConfirmedBikes()
      .then((d) => {
        if (alive) setData(d);
      })
      .catch((e) => {
        if (alive) setLoadError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      alive = false;
    };
  }, []);

  const activeBike = useMemo(
    () => data?.bikes.find((b) => b.bike_id === activeDetailId) ?? null,
    [data, activeDetailId],
  );

  return (
    <>
      <div className="page-head">
        <div>
          <h1>확정 내역</h1>
          <div className="sub">가장 최근에 확정한 수거 목록입니다</div>
        </div>
        {data?.actioned_at && (
          <div className="updated">
            {data.snapshot_date} 스냅샷 · {fmtStamp(data.actioned_at)} 확정
          </div>
        )}
      </div>

      {loadError && <div className="filter-note">확정 내역을 불러오지 못했습니다: {loadError}</div>}

      {!loadError && data === null && <div className="updated">로딩 중…</div>}

      {data !== null && data.confirmed === 0 && (
        <div className="list-panel">
          <div className="list-cap-note">
            아직 확정한 내역이 없습니다. 상세 탭의 Capacity 패널에서 확인 버튼을 누르면 여기에 표시됩니다.
          </div>
        </div>
      )}

      {data !== null && data.confirmed > 0 && (
        <div className="confirmed-layout">
          <div className="list-panel dest">
            <div className="list-head">
              <h2>수거 확정</h2>
              <span className="count">{data.confirmed.toLocaleString()}건</span>
            </div>
            <div className="list-cap-note">확정 시점의 capacity 기준으로 선정된 목록</div>
            <div className="list-scroll">
              <BikeTable
                bikes={data.bikes}
                activeDetailId={activeDetailId}
                onRowClick={(bike) => setActiveDetailId(bike.bike_id)}
              />
            </div>
          </div>

          <div className={`detail-panel${activeBike ? " active" : ""}`}>
            <DetailPanel bike={activeBike} onClose={() => setActiveDetailId(null)} />
          </div>
        </div>
      )}
    </>
  );
}
