"""
Lambda Function: notify_slack

SNS 토픽(raw_fetch_alerts)으로 들어오는 CloudWatch Alarm 알림을 받아
Slack 인커밍 웹훅으로 전달합니다. (Issue #180)

SNS가 CloudWatch Alarm에 의해 트리거된 경우 Records[0].Sns.Message는
Alarm 상세(JSON 문자열)이다. 그 외 형태로 직접 publish된 메시지는
raw text로 취급해 그대로 전달한다.
"""
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _build_slack_text(sns_message: str, subject: str) -> str:
    try:
        alarm = json.loads(sns_message)
    except (json.JSONDecodeError, TypeError):
        return f"*{subject or 'raw-fetch-lambda-alerts'}*\n{sns_message}"

    alarm_name = alarm.get("AlarmName", "unknown-alarm")
    new_state = alarm.get("NewStateValue", "UNKNOWN")
    reason = alarm.get("NewStateReason", "")
    trigger = alarm.get("Trigger", {})
    metric_name = trigger.get("MetricName", "")
    namespace = trigger.get("Namespace", "")
    changed_at = alarm.get("StateChangeTime", "")

    return (
        f"*:rotating_light: {alarm_name}* -> `{new_state}`\n"
        f"metric: {namespace}/{metric_name}\n"
        f"reason: {reason}\n"
        f"time: {changed_at}"
    )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL environment variable is required")

    records = event.get("Records", [])
    if not records:
        logger.warning("SNS Records가 비어있는 이벤트를 받음: %s", event)
        return {"statusCode": 200, "notified": 0}

    failures = []
    for record in records:
        sns = record.get("Sns", {})
        subject = sns.get("Subject", "")
        text = _build_slack_text(sns.get("Message", ""), subject)
        payload = json.dumps({"text": text}).encode("utf-8")

        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.getcode() >= 300:
                    raise RuntimeError(f"Slack webhook returned HTTP {resp.getcode()}")
        except (urllib.error.URLError, RuntimeError) as e:
            # 한 레코드가 실패해도 같은 호출 안의 나머지 레코드는 계속 시도한다 - 여기서
            # 바로 raise하면 Lambda 재시도가 이 호출의 전체 이벤트를 재전달해서, 이미
            # 성공한 앞선 레코드의 알림까지 중복 발송된다.
            logger.error("Slack 알림 전송 실패 (subject=%s): %s", subject, e)
            failures.append(subject)
            continue

        logger.info("Slack 알림 전송 완료 (subject=%s)", subject)

    notified = len(records) - len(failures)
    if failures:
        raise RuntimeError(f"Slack webhook 호출 실패 ({len(failures)}/{len(records)}건): {failures}")

    return {"statusCode": 200, "notified": notified}
