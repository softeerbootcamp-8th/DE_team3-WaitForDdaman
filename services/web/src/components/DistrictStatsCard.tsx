interface DistrictStatsCardProps {
  label: string;
  totalCount: number;
  destCount: number;
  sourceCount: number;
  onOpenDetail: () => void;
}

export function DistrictStatsCard({ label, totalCount, destCount, sourceCount, onOpenDetail }: DistrictStatsCardProps) {
  return (
    <div className="district-stats-card">
      <div className="list-head">
        <h2>{label}</h2>
        <button className="detail-link-btn" onClick={onOpenDetail}>
          상세보기 →
        </button>
      </div>
      <div className="district-stats-grid">
        <div className="stat-box">
          <div className="s-label">전체 자전거</div>
          <div className="s-value">{totalCount.toLocaleString()}대</div>
        </div>
        <div className="stat-box">
          <div className="s-label">수거 대상</div>
          <div className="s-value">{destCount.toLocaleString()}대</div>
        </div>
        <div className="stat-box">
          <div className="s-label">대여중단</div>
          <div className="s-value">{sourceCount.toLocaleString()}대</div>
        </div>
      </div>
    </div>
  );
}
