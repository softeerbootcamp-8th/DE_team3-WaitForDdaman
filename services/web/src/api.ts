import type { BikeLists, MapData, SnapshotMeta } from "./types";

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
};
