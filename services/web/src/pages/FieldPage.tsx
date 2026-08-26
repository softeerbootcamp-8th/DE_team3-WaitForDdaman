import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import type { Bike, ConfirmedBikes } from "../types";

// 시연용 하드코딩: 실제로는 인증된 기사님 계정 + 배차 시스템에서 내려와야 하는 값이다.
const FIELD_TECHNICIAN = { name: "김민준", district: "영등포구" };

function groupByStation(bikes: Bike[]): Map<string, Bike[]> {
  const map = new Map<string, Bike[]>();
  for (const bike of bikes) {
    const list = map.get(bike.station_name) ?? [];
    list.push(bike);
    map.set(bike.station_name, list);
  }
  return map;
}

export function FieldPage() {
  const [data, setData] = useState<ConfirmedBikes | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

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

  const myBikes = useMemo(
    () => (data?.bikes ?? []).filter((b) => b.district === FIELD_TECHNICIAN.district),
    [data],
  );
  const stations = useMemo(() => groupByStation(myBikes), [myBikes]);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>{FIELD_TECHNICIAN.name} 기사님 · {FIELD_TECHNICIAN.district} 담당</h1>
          <div className="sub">오늘 확정된 수거 목록입니다. 대여소 순서대로 방문해 자전거를 수거해 주세요.</div>
        </div>
        {data?.actioned_at && <div className="updated">{data.snapshot_date} 스냅샷 기준</div>}
      </div>

      {loadError && <div className="filter-note">수거 목록을 불러오지 못했습니다: {loadError}</div>}

      {!loadError && data === null && <div className="updated">로딩 중…</div>}

      {data !== null && myBikes.length === 0 && (
        <div className="list-panel">
          <div className="list-cap-note">
            아직 {FIELD_TECHNICIAN.district}에 확정된 수거 내역이 없습니다. 상세 탭에서 먼저 확정해 주세요.
          </div>
        </div>
      )}

      {stations.size > 0 && (
        <div className="field-station-grid">
          {[...stations.entries()].map(([stationName, bikes]) => (
            <div className="field-card" key={stationName}>
              <div className="field-card-head">
                <h2>{stationName.trim()}</h2>
                <span className="count">{bikes.length}대 수거</span>
              </div>
              <ul className="field-bike-list">
                {bikes.map((b) => (
                  <li key={b.bike_id}>
                    <span className="bike-id">{b.bike_id}</span>
                    <span className={`tier-pill ${b.risk_grade}`}>{b.risk_grade}</span>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      )}
    </>
  );
}
