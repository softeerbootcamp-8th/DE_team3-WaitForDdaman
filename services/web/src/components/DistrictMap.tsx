import { useEffect, useMemo, useRef, useState } from "react";
import type { District, MapStation } from "../types";

interface DistrictMapProps {
  viewBox: [number, number];
  districts: District[];
  stations?: MapStation[];
  variant: "full" | "mini";
  highlight: (guName: string) => boolean;
  onSelectDistrict?: (guName: string) => void;
  showHeatmap?: boolean;
  showStationDots?: boolean;
  highlightStation?: MapStation | null;
}

interface HoverInfo {
  station: MapStation;
  left: number;
  top: number;
}

export function DistrictMap({
  viewBox,
  districts,
  stations,
  variant,
  highlight,
  onSelectDistrict,
  showHeatmap = variant === "full",
  showStationDots = variant === "full",
  highlightStation,
}: DistrictMapProps) {
  const mapCardRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [hover, setHover] = useState<HoverInfo | null>(null);
  const [w, h] = viewBox;

  const riskStations = useMemo(() => stations?.filter((s) => s.risk_cnt > 0) ?? [], [stations]);
  const maxRisk = useMemo(() => Math.max(1, ...riskStations.map((s) => s.risk_cnt)), [riskStations]);

  // 캔버스 radial-gradient 히트맵은 선언적 React로 표현할 수 없어 useEffect로 직접 그린다.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !showHeatmap) return;
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.clearRect(0, 0, w, h);
    ctx.globalCompositeOperation = "lighter";
    riskStations.forEach((s) => {
      const t = s.risk_cnt / maxRisk;
      const alphaFactor = Math.pow(t, 2.2);
      const radius = 6 + 20 * Math.sqrt(t);
      const grad = ctx.createRadialGradient(s.x, s.y, 0, s.x, s.y, radius);
      grad.addColorStop(0, `rgba(198,79,46,${(0.6 * alphaFactor).toFixed(3)})`);
      grad.addColorStop(0.6, `rgba(198,79,46,${(0.3 * alphaFactor).toFixed(3)})`);
      grad.addColorStop(1, "rgba(198,79,46,0)");
      ctx.fillStyle = grad;
      ctx.beginPath();
      ctx.arc(s.x, s.y, radius, 0, Math.PI * 2);
      ctx.fill();
    });
  }, [riskStations, maxRisk, w, h, showHeatmap]);

  function handleDotMove(s: MapStation, e: React.MouseEvent) {
    const rect = mapCardRef.current?.getBoundingClientRect();
    if (!rect) return;
    setHover({ station: s, left: e.clientX - rect.left + 12, top: e.clientY - rect.top + 8 });
  }

  return (
    <div className={`map-card${variant === "mini" ? " mini" : ""}`} ref={mapCardRef} style={{ aspectRatio: `${w} / ${h}` }}>
      <svg viewBox={`0 0 ${w} ${h}`}>
        {districts.map((d) => (
          <path
            key={d.name}
            className={`district${highlight(d.name) ? " selected" : ""}`}
            d={d.path}
            onClick={onSelectDistrict ? () => onSelectDistrict(d.name) : undefined}
            style={onSelectDistrict ? { cursor: "pointer" } : undefined}
          >
            <title>{d.name}</title>
          </path>
        ))}
        {variant === "full" &&
          districts.map((d) => (
            <text key={`${d.name}-label`} className="district-label" x={d.cx} y={d.cy}>
              {d.name}
            </text>
          ))}
        {showStationDots &&
          riskStations.map((s) => {
            const r = 1.8 + 3.2 * Math.sqrt(s.risk_cnt / maxRisk);
            return (
              <circle
                key={s.station_id}
                className="station-dot"
                cx={s.x}
                cy={s.y}
                r={r}
                onMouseMove={(e) => handleDotMove(s, e)}
                onMouseLeave={() => setHover(null)}
              />
            );
          })}
        {highlightStation && (
          <g className="station-pulse-group" transform={`translate(${highlightStation.x}, ${highlightStation.y})`}>
            <circle className="station-pulse-ring-outer" r="14" />
            <circle className="station-pulse-ring-inner" r="7" />
            <circle className="station-pulse-core" r="3.5" />
            <text className="station-pulse-label" x="0" y="-7">
              {highlightStation.station_name}
            </text>
          </g>
        )}
      </svg>
      {showHeatmap && <canvas ref={canvasRef} />}
      {hover && (
        <div className="station-tooltip show" style={{ left: hover.left, top: hover.top }}>
          <b>{hover.station.station_name}</b>
          <br />
          위험 자전거 {hover.station.risk_cnt}대 / 전체 {hover.station.bike_cnt}대 (거치대 {hover.station.hold_num}대) ·
          정상비율 {hover.station.healthy_ratio}% ({hover.station.urgency})
        </div>
      )}
    </div>
  );
}
