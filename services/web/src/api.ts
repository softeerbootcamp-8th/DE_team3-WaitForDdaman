import type {
  BikeLists,
  ConfirmResult,
  ListName,
  MapData,
  SnapshotMeta,
  WorklogEntry,
} from "./types";

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
  transfer: (ids: string[], fromList: ListName) =>
    request<BikeLists>("/bikes/transfer", {
      method: "POST",
      body: JSON.stringify({ ids, fromList }),
    }),
  setCapacity: (max: number) =>
    request<SnapshotMeta>("/capacity", {
      method: "PATCH",
      body: JSON.stringify({ max }),
    }),
  confirm: () => request<ConfirmResult>("/worklog/confirm", { method: "POST" }),
  getWorklog: () => request<WorklogEntry[]>("/worklog"),
};
