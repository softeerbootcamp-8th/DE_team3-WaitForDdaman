import type { Bike } from "../types";
import { bikeImageFor, FALLBACK_BIKE_IMG } from "../utils/bikeImage";

interface DetailPanelProps {
  bike: Bike | null;
  onClose?: () => void;
}

export function DetailPanel({ bike, onClose }: DetailPanelProps) {
  if (!bike) {
    return (
      <div className="detail-empty">
        <svg viewBox="0 0 24 24" fill="none" strokeWidth={1.6} strokeLinecap="round" strokeLinejoin="round">
          <rect x="3" y="7" width="18" height="13" rx="2" />
          <path d="M8 7V5a2 2 0 012-2h4a2 2 0 012 2v2" />
        </svg>
        <div>
          리스트에서 자전거를 선택하면
          <br />
          상세 정보가 여기에 확대 표시됩니다.
        </div>
      </div>
    );
  }

  return (
    <div className="detail-content-active">
      <div className="detail-panel-top">
        <div className="detail-panel-title">자전거 상세 점검</div>
        {onClose && (
          <button className="detail-close-btn" onClick={onClose} title="닫기 (ESC)">
            ✕
          </button>
        )}
      </div>

      <div className="detail-img-wrap">
        <img
          className="detail-img"
          src={bikeImageFor(bike.bike_id)}
          onError={(e) => {
            (e.target as HTMLImageElement).src = FALLBACK_BIKE_IMG;
          }}
          alt={`${bike.bike_id} 참고 이미지`}
        />
        <span className={`badge-type ${bike.risk_grade} badge-floating`}>
          <span className="badge-pulse-dot" />
          {bike.risk_grade}
        </span>
      </div>

      <div className="detail-body">
        <div className="detail-id-row">
          <span className="bike-id">{bike.bike_id}</span>
        </div>
        <div className="detail-station">
          <span className="station-icon">📍</span> {bike.station_name} · <span className="gu-tag">{bike.district}</span>
        </div>

        <div className="detail-grid">
          <div className="stat-box">
            <div className="s-label">누적 이동거리</div>
            <div className="s-value">{bike.dist_km != null ? `${bike.dist_km.toLocaleString()}km` : "정보없음"}</div>
          </div>
          <div className="stat-box">
            <div className="s-label">노후화</div>
            <div className="s-value">{bike.aging != null ? `${bike.aging.toLocaleString()}년` : "정보없음"}</div>
          </div>
        </div>

        <div className="score-block">
          <div className="score-main">
            <div className="score-num">
              {bike.risk_score}
              <sub>점</sub>
            </div>
            <div className="score-label">AI 고장 위험도</div>
          </div>
          <div className="score-bar-track">
            <div
              className={`score-bar-fill ${bike.risk_grade}`}
              style={{ width: `${Math.min(100, Math.max(0, bike.risk_score))}%` }}
            />
          </div>
        </div>

        <div className="station-block">
          {bike.station_urgency === "정보없음" ? (
            "이 대여소의 정상자전거 비율 데이터가 부족합니다"
          ) : (
            <>
              대여소 정상 거치율 <b>{bike.healthy_ratio}%</b> → <b>{bike.station_urgency}</b>
            </>
          )}
        </div>

        <div className="history-label">최근 고장신고 이력 ({bike.fail_history.length}건)</div>
        <div className="history-list">
          {bike.fail_history.length ? (
            bike.fail_history.map((h, i) => (
              <div className="history-item" key={i}>
                <span className="dot" />
                {h}
              </div>
            ))
          ) : (
            <div className="history-item none">최근 접수된 고장신고 없음 (정상)</div>
          )}
        </div>
      </div>
    </div>
  );
}