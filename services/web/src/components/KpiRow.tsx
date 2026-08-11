import type { Kpi } from "../types";
import { fmtPctOrNA } from "../utils/format";

export function KpiRow({ kpi }: { kpi: Kpi }) {
  let deltaText = "";
  let deltaClass = "";
  if (kpi.today !== null && kpi.yesterday !== null) {
    const d = kpi.today - kpi.yesterday;
    deltaText = (d >= 0 ? "전일比 +" : "전일比 ") + d.toFixed(1) + "%p";
    deltaClass = d >= 0 ? "up" : "down";
  }

  return (
    <>
      <div className="kpi-card today">
        <div className="k-label">오늘 신규고장 사전포착률</div>
        <div className="k-value">{fmtPctOrNA(kpi.today)}</div>
        <div className={`k-delta${deltaClass ? " " + deltaClass : ""}`}>{deltaText}</div>
      </div>
      <div className="kpi-card">
        <div className="k-label">어제 사전포착률</div>
        <div className="k-value">{fmtPctOrNA(kpi.yesterday)}</div>
        <div className="k-delta">신고 접수 전, 신규고장을 목록이 먼저 잡아낸 비율</div>
      </div>
      <div className="kpi-card">
        <div className="k-label">6월 평균 사전포착률</div>
        <div className="k-value">{fmtPctOrNA(kpi.monthly)}</div>
        <div className="k-delta">6월 1일~30일 일별 평균 (1~2일은 대여이력 없어 0%)</div>
      </div>
    </>
  );
}
