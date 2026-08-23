"""deploy_returned_bikes의 Lambda 진입점 - generate_collect_events.py와 동일 패턴 (#186)."""
from typing import Any

from ._secrets import load_bikeman_db_secret

load_bikeman_db_secret()

from deploy_returned_bikes import run  # noqa: E402 (자격증명을 채운 뒤에 import해야 함)


def handler(event: dict, context: Any) -> dict:
    event = event or {}
    written = run(event["snapshot_date"])
    return {"statusCode": 200, "written": written}
