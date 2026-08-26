interface SummaryRowProps {
  totalBikes: number;
  totalDest: number;
  totalSource: number;
  overallCapacity: number;
}

export function SummaryRow({ totalBikes, totalDest, totalSource, overallCapacity }: SummaryRowProps) {
  return (
    <>
      <div className="summary-card">
        <div className="s-label">총 자전거대수</div>
        <div className="s-value">{totalBikes.toLocaleString()}대</div>
      </div>
      <div className="summary-card">
        <div className="s-label">총 수거대수</div>
        <div className="s-value">{totalDest.toLocaleString()}대</div>
      </div>
      <div className="summary-card">
        <div className="s-label">총 대여중단 대수</div>
        <div className="s-value">{totalSource.toLocaleString()}대</div>
      </div>
      <div className="summary-card">
        <div className="s-label">전체 수용량</div>
        <div className="s-value">{Math.round(overallCapacity).toLocaleString()}대</div>
      </div>
    </>
  );
}
