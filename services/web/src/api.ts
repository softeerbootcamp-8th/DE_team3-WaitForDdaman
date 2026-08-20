import type { BikeLists, ConfirmResponse, MapData, SnapshotMeta } from "./types";

const BASE = "/api";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    throw new Error(`${path} 요청 실패 (${res.status})`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  getMeta: () => request<SnapshotMeta>("/meta"),
  getMap: () => request<MapData>("/map"),
  getBikes: () => request<BikeLists>("/bikes"),
  getConfirmation: () => request<ConfirmResponse>("/actions/confirm"),
  // headers를 init으로 넘기면 위 Content-Type이 덮이므로 body만 전달한다.
  confirmCollection: (bike_ids: string[]) =>
    request<ConfirmResponse>("/actions/confirm", { method: "POST", body: JSON.stringify({ bike_ids }) }),
};
