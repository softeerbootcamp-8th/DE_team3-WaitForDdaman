"""generate_collect_events의 Lambda 진입점 (#186).

핸들러는 얇게 유지한다 - 실제 로직은 pipeline/bikeman_event_generator/jobs/
generate_collect_events.py의 run()에 그대로 있다(로컬 `python -c "..."` 경로와
완전히 같은 코드). 여기서는 (1) Secrets Manager에서 DB 자격증명을 채우고 (2) event의
snapshot_date를 run()의 인자로 그대로 넘긴다.

Terraform의 image_config.command로 "app.generate_collect_events.handler"를 가리키면
이 handler가 진입점이 된다 - 이미지는 deploy_returned_bikes와 공유하고 함수(Lambda
리소스)만 둘로 나뉜다.
"""
from typing import Any

from ._secrets import load_bikeman_db_secret

load_bikeman_db_secret()

from generate_collect_events import run  # noqa: E402 (자격증명을 채운 뒤에 import해야 함)


def handler(event: dict, context: Any) -> dict:
    event = event or {}
    written = run(event["snapshot_date"])
    return {"statusCode": 200, "written": written}