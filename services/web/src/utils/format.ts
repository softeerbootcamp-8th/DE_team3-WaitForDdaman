export function fmtPct(n: number): string {
  return n.toFixed(1) + "%";
}

export function rateClass(pct: number | null): "" | "low" | "high" {
  if (pct === null) return "";
  return pct < 70 ? "low" : "high";
}
