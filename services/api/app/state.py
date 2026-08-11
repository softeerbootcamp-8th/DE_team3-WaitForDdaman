"""오늘 하루치 수거 우선순위 콘솔 상태를 메모리에 들고 있는 단일 진실 소스.

원본 프로토타입(index.html)은 fetch한 snapshot.json을 브라우저 전역 변수에 두고
확정 로그만 localStorage에 남겼다. 이제 그 상태를 백엔드가 갖고,
프론트는 API를 통해서만 읽고 바꾼다.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Literal

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SNAPSHOT_PATH = DATA_DIR / "snapshot.json"
WORKLOG_PATH = DATA_DIR / "worklog.json"


class OperationState:
    def __init__(self, snapshot_path: Path = SNAPSHOT_PATH):
        self._lock = Lock()
        self._raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
        self._by_id: dict[str, dict] = {
            b["id"]: b for b in self._raw["source"] + self._raw["dest"]
        }
        self._source_ids: list[str] = [b["id"] for b in self._raw["source"]]
        self._dest_ids: list[str] = [b["id"] for b in self._raw["dest"]]
        self._capacity_max: int = self._raw["capacity"]["max"]

    @property
    def meta(self) -> dict:
        return {
            "generatedAt": self._raw["generatedAt"],
            "capacity": {"used": len(self._dest_ids), "max": self._capacity_max},
            "kpi": self._raw["kpi"],
            "poolSize": self._raw["poolSize"],
            "tierCounts": self._raw["tierCounts"],
            "actionCounts": self._raw["actionCounts"],
        }

    @property
    def map_data(self) -> dict:
        return self._raw["map"]

    def bikes(self) -> tuple[list[dict], list[dict]]:
        return (
            [self._by_id[i] for i in self._source_ids],
            [self._by_id[i] for i in self._dest_ids],
        )

    def transfer(self, ids: list[str], from_list: Literal["source", "dest"]) -> tuple[list[dict], list[dict]]:
        with self._lock:
            src = self._source_ids if from_list == "source" else self._dest_ids
            dst = self._dest_ids if from_list == "source" else self._source_ids
            moving = set(ids) & set(src)
            if moving:
                src[:] = [i for i in src if i not in moving]
                # 원래 순서(점수 내림차순)를 최대한 보존하며 이동분을 뒤에 붙인다
                dst.extend(i for i in ids if i in moving)
            return self.bikes()

    def set_capacity(self, max_capacity: int) -> None:
        with self._lock:
            self._capacity_max = max(0, max_capacity)
            pool = sorted(
                self._source_ids + self._dest_ids,
                key=lambda i: self._by_id[i]["score"],
                reverse=True,
            )
            self._dest_ids = pool[: self._capacity_max]
            self._source_ids = pool[self._capacity_max:]

    def confirm_today(self) -> dict:
        with self._lock:
            today = self._raw["generatedAt"][:10]
            stamp = datetime.now(timezone.utc).isoformat()
            entries = []
            for bike_id in self._dest_ids:
                b = self._by_id[bike_id]
                entries.append({
                    "date": today, "bikeId": b["id"], "station": b["station"],
                    "action": "수거", "tier": b["tier"], "score": b["score"], "confirmedAt": stamp,
                })
            for bike_id in self._source_ids:
                b = self._by_id[bike_id]
                entries.append({
                    "date": today, "bikeId": b["id"], "station": b["station"],
                    "action": "대여중단_유지", "tier": b["tier"], "score": b["score"], "confirmedAt": stamp,
                })

            log = self.worklog()
            log.extend(entries)
            WORKLOG_PATH.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
            return {
                "recorded": len(entries),
                "destCount": len(self._dest_ids),
                "sourceCount": len(self._source_ids),
            }

    def worklog(self) -> list[dict]:
        if not WORKLOG_PATH.exists():
            return []
        return json.loads(WORKLOG_PATH.read_text(encoding="utf-8"))


state = OperationState()
