export function fmtPct(n: number): string {
  return n.toFixed(1) + "%";
}

export function rateClass(pct: number | null): "" | "low" | "high" {
  if (pct === null) return "";
  return pct < 70 ? "low" : "high";
}

// 확정 시각 표시용. 서버는 타임존 없는 ISO 문자열("2026-08-20T09:28:46.099201")을 주므로
// Date로 파싱해 브라우저 타임존으로 밀리지 않게 문자열에서 바로 잘라 쓴다.
export function fmtStamp(iso: string): string {
  return iso.slice(5, 16).replace("T", " ");
}
