import type { Bike } from "../types";
import { fmtPct, rateClass } from "../utils/format";

const RENDER_LIMIT = 200; // 리스트당 최대 렌더링 행 수 (성능 보호, 검색/필터로 좁혀서 탐색)

interface BikeTableProps {
  bikes: Bike[];
  activeDetailId: string | null;
  onRowClick: (bike: Bike) => void;
}

export function BikeTable({ bikes, activeDetailId, onRowClick }: BikeTableProps) {
  const shown = bikes.slice(0, RENDER_LIMIT);

  return (
    <table className="roster">
      <thead>
        <tr>
          <th>자전거ID</th>
          <th>대여소</th>
          <th>정상 자전거 거치율</th>
        </tr>
      </thead>
      <tbody>
        {bikes.length === 0 && (
          <tr className="empty-row">
            <td colSpan={3}>표시할 자전거가 없습니다.</td>
          </tr>
        )}
        {shown.map((bike) => {
          const pct = bike.healthyRatio;
          const rc = rateClass(pct);
          const pctLabel = pct === null ? "정보없음" : fmtPct(pct);
          const barWidth = pct === null ? 0 : pct;
          return (
            <tr
              key={bike.id}
              className={bike.id === activeDetailId ? "active" : undefined}
              onClick={() => onRowClick(bike)}
            >
              <td className="bike-id">
                {bike.id}
                <span className={`tier-pill ${bike.tier}`}>{bike.tier}</span>
              </td>
              <td>{bike.station ?? "정보없음"}</td>
              <td className={`rate ${rc}`}>
                {pctLabel}
                <span className="rate-bar">
                  <i style={{ width: `${barWidth}%` }} />
                </span>
              </td>
            </tr>
          );
        })}
        {bikes.length > RENDER_LIMIT && (
          <tr className="more-row">
            <td colSpan={3}>
              {bikes.length.toLocaleString()}건 중 상위 {RENDER_LIMIT}건 표시 중 · 검색/필터로 좁혀보세요
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}
