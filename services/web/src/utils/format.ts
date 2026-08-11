export function fmtPct(n: number): string {
  return n.toFixed(1) + "%";
}

export function fmtPctOrNA(v: number | null | undefined): string {
  return v === null || v === undefined ? "평가 대상 없음" : fmtPct(v);
}

export function rateClass(pct: number | null): "" | "low" | "high" {
  if (pct === null) return "";
  return pct < 70 ? "low" : "high";
}
