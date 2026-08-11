import { useEffect, useMemo, useRef } from "react";
import type { MapData } from "../types";

interface MainMapTabProps {
  mapData: MapData;
  generatedAt?: string;
}

export function MainMapTab({ mapData, generatedAt }: MainMapTabProps) {
  const mapCardRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);

  const [w, h] = mapData.viewBox;

  const riskStations = useMemo(() => mapData.stations.filter((s) => s.riskCount > 0), [mapData]);

  const top10 = useMemo(
    () => [...riskStations].sort((a, b) => b.riskCount - a.riskCount).slice(0, 10),
    [riskStations],
  );

  // 지도는 외부 지도 라이브러리/타일 서버 없이 완전히 오프라인으로 그린다: 자치구 경계는
  // SVG path 문자열로, 위험 밀집도는 canvas radial gradient 누적(lighter)으로 그려서
  // 겹칠수록 밝고 진해지는 히트맵을 만든다. React 렌더 트리 밖에서 매번 다시 그려야 하므로
  // 원본 프로토타입과 동일하게 ref로 DOM에 직접 그린다.
  useEffect(() => {
    const svg = svgRef.current;
    const canvas = canvasRef.current;
    const mapCard = mapCardRef.current;
    const tooltip = tooltipRef.current;
    if (!svg || !canvas || !mapCard || !tooltip) return;

    const districtPaths = mapData.districts
      .map((d) => `<path class="district" d="${d.path}"><title>${d.name}</title></path>`)
      .join("");
    const districtLabels = mapData.districts
      .map((d) => `<text class="district-label" x="${d.cx}" y="${d.cy}">${d.name}</text>`)
      .join("");
    const maxRisk = Math.max(1, ...riskStations.map((s) => s.riskCount));
    const dots = riskStations
      .map((s) => {
        const r = 1.8 + 3.2 * Math.sqrt(s.riskCount / maxRisk);
        return `<circle class="station-dot" data-id="${s.id}" cx="${s.x}" cy="${s.y}" r="${r.toFixed(1)}"></circle>`;
      })
      .join("");
    svg.innerHTML = districtPaths + districtLabels + dots;

    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext("2d");
    if (ctx) {
      ctx.clearRect(0, 0, w, h);
      ctx.globalCompositeOperation = "lighter";
      // 대여소 84%가 위험 자전거를 1대라도 갖고 있어서(중간값 3대) 선형/제곱근 스케일로는
      // 지도 전체가 겹쳐 하얗게 뜬다. 개수가 많은 상위 대여소만 도드라지도록 감쇠를 가파르게 준다.
      riskStations.forEach((s) => {
        const t = s.riskCount / maxRisk;
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
    }

    const stationsById = new Map(riskStations.map((s) => [String(s.id), s]));
    const dotEls = svg.querySelectorAll<SVGCircleElement>(".station-dot");
    const cleanups: Array<() => void> = [];
    dotEls.forEach((dot) => {
      const s = stationsById.get(dot.dataset.id ?? "");
      if (!s) return;
      const move = (e: MouseEvent) => {
        const rect = mapCard.getBoundingClientRect();
        tooltip.innerHTML = `<b>${s.name}</b><br>위험 자전거 ${s.riskCount}대 / 전체 ${s.bikeCount}대 · 정상비율 ${s.healthyRatio}% (${s.urgency})`;
        tooltip.style.left = `${e.clientX - rect.left + 12}px`;
        tooltip.style.top = `${e.clientY - rect.top + 8}px`;
        tooltip.classList.add("show");
      };
      const leave = () => tooltip.classList.remove("show");
      dot.addEventListener("mousemove", move);
      dot.addEventListener("mouseleave", leave);
      cleanups.push(() => {
        dot.removeEventListener("mousemove", move);
        dot.removeEventListener("mouseleave", leave);
      });
    });

    return () => cleanups.forEach((fn) => fn());
  }, [mapData, riskStations, w, h]);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>대여소 위험도 지도</h1>
          <div className="sub">
            대여소별 위험(Critical/Risk) 자전거 수를 히트맵으로 표시 — 진하고 밝을수록 위험 자전거가 몰려있는 대여소
          </div>
        </div>
        <div className="updated">{generatedAt ? `데이터 기준 ${generatedAt}` : "로딩 중…"}</div>
      </div>

      <div className="map-layout">
        <div className="map-card" ref={mapCardRef} style={{ aspectRatio: `${w} / ${h}` }}>
          <svg ref={svgRef} viewBox={`0 0 ${w} ${h}`} />
          <canvas ref={canvasRef} />
          <div className="station-tooltip" ref={tooltipRef} />
        </div>
        <div className="map-side">
          <div className="map-legend">
            <div className="legend-title">위험 자전거 밀집도</div>
            <div className="legend-gradient" />
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span>적음</span>
              <span>많음</span>
            </div>
          </div>
          <div className="top-station-panel">
            <div className="list-head">
              <h2>위험 대여소 TOP 10</h2>
            </div>
            <ol className="top-station-list">
              {top10.map((s, i) => (
                <li key={s.id}>
                  <span className="rank">{i + 1}</span>
                  <span className="st-name">
                    {s.name.trim()}
                    <br />
                    <span className="st-gu">{s.gu}</span>
                  </span>
                  <span className="st-count">{s.riskCount}대</span>
                </li>
              ))}
            </ol>
          </div>
        </div>
      </div>
    </>
  );
}
