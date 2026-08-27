"""
DQ 이상 감지 시 GitHub 이슈 생성/코멘트 (#217 2단계)

interpret_dq_results.py가 남긴 해석 결과에서 is_anomaly=true인 체크만 골라
GitHub 이슈를 만들거나(신규) 동일 fingerprint의 열린 이슈에 코멘트를 남긴다
(기존). 이슈를 자동으로 닫는 로직은 절대 두지 않는다.

interpret_dq_results가 스킵됐으면(해석 결과 파일 자체가 없음) 이 태스크도 할 일이
없으므로 조용히 스킵한다. GitHub API 호출이 재시도까지 실패해도 이 태스크는
실패시키지 않는다 - 품질 이슈 "알림"이 안 됐다고 파이프라인 전체가 막히면 안
된다는 게 팀 정책(#217)이라, 대신 결과에 error를 남겨 다음 태스크(Slack)가
사람에게 알리게 한다.

사용법:
    EXECUTION_DATE=2026-08-24 DQ_SOURCE_NAME=rental_history python -m jobs.report_dq_issue
"""
import logging
import os

import config
from common.dq_github import report_issue
from common.dq_interpreter import DEFAULT_LOOKBACK_DAYS, fetch_history
from common.iceberg_catalog import build_iceberg_catalog
from common.s3_utils import ensure_bucket, get_json, put_json
from jobs.interpret_dq_results import interpretation_key
from jobs.run_dq_assertions import pending_result_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def github_issues_key(source_name: str, execution_date: str) -> str:
    return f"_meta/dq/github_issues/{source_name}/{execution_date}.json"


def _fmt_metric(value) -> str:
    # ERROR 체크는 계산 자체가 실패해서 metric_value가 null이다 - "없음"으로 명시한다.
    return f"{value:.6f}" if value is not None else "N/A (계산 실패)"


def _history_table_md(execution_date: str, current: dict, history: list[dict]) -> str:
    # current(pending 결과)는 check당 값이라 execution_date를 안 들고 있다 - 잡 실행
    # 시점의 execution_date_str을 별도 인자로 받는다.
    rows = sorted(history, key=lambda r: r.get("execution_date", ""), reverse=True)
    lines = ["| execution_date | metric_value | threshold | pass_fail |", "| --- | --- | --- | --- |"]
    lines.append(
        f"| {execution_date} (오늘) | {_fmt_metric(current['metric_value'])} | "
        f"{current.get('threshold')} | {current['pass_fail']} |"
    )
    for r in rows:
        lines.append(
            f"| {r.get('execution_date')} | {_fmt_metric(r.get('metric_value'))} | "
            f"{r.get('threshold')} | {r.get('pass_fail')} |"
        )
    return "\n".join(lines)


def _sql_snippet(source_name: str, check_name: str) -> str:
    catalog = config.SETTINGS.iceberg_catalog_name
    return (
        f"SELECT execution_date, metric_value, threshold, pass_fail\n"
        f"FROM {catalog}.dq.check_result_history\n"
        f"WHERE source_name = '{source_name}' AND check_name = '{check_name}'\n"
        f"ORDER BY execution_date DESC\n"
        f"LIMIT 15;"
    )


def _new_issue_body(
    source_name: str, check_name: str, execution_date: str, current: dict,
    history: list[dict], check_interpretation: dict, dag_id: str, task_id: str, run_id: str,
) -> str:
    error_note = f"\n⚠️ 어써션 계산 실패: {current['error']}\n" if current.get("error") else ""
    return f"""## 실행 정보
- dag_id: `{dag_id}`
- task_id: `{task_id}`
- run_id: `{run_id}`
- execution_date: `{execution_date}`
- source_name / check_name / target_column: `{source_name}` / `{check_name}` / `{current.get('target_column')}`

## 실측값 vs 최근 히스토리
{_history_table_md(execution_date, current, history)}
{error_note}
## 해석 에이전트 판단
- severity: **{check_interpretation.get('severity')}**
- reasoning: {check_interpretation.get('reasoning')}
- suggested_action: {check_interpretation.get('suggested_action')}

## 재현 쿼리
```sql
{_sql_snippet(source_name, check_name)}
```

---
이 이슈는 DQ 파이프라인이 자동 생성했습니다. 원인이 해소되면 **사람이 직접** 닫아주세요 - 자동으로 닫히지 않습니다.
"""


def _comment_body(
    execution_date: str, current: dict, check_interpretation: dict,
) -> str:
    error_line = f"- error: {current['error']}\n" if current.get('error') else ""
    return f"""### {execution_date} 재발
- metric_value: `{_fmt_metric(current['metric_value'])}` (threshold: `{current.get('threshold')}`, pass_fail: `{current['pass_fail']}`)
{error_line}- severity: **{check_interpretation.get('severity')}**
- reasoning: {check_interpretation.get('reasoning')}
- suggested_action: {check_interpretation.get('suggested_action')}
"""


def run(source_name: str | None = None, execution_date_str: str | None = None) -> list[dict]:
    source_name = source_name or os.environ.get("DQ_SOURCE_NAME", "rental_history")
    execution_date_str = execution_date_str or os.environ["EXECUTION_DATE"]
    lookback_days = int(os.environ.get("DQ_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS))

    bucket = config.SETTINGS.raw_bucket
    interpretation = get_json(bucket, interpretation_key(source_name, execution_date_str))
    if not interpretation:
        logger.info("%s/%s: 해석 결과 없음 - 이슈 생성 스킵", source_name, execution_date_str)
        return []

    anomalous = [c for c in interpretation.get("checks", []) if c.get("is_anomaly")]
    if not anomalous:
        logger.info("%s/%s: is_anomaly=true인 체크 없음 - 이슈 생성 스킵", source_name, execution_date_str)
        return []

    repo = os.environ.get("GITHUB_REPOSITORY", "softeerbootcamp-8th/DE_team3-WaitForDdaman")
    token = os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        logger.warning(
            "GITHUB_REPOSITORY/GITHUB_TOKEN 미설정 - 이슈 생성 스킵 (품질 이슈 알림 실패는 warning 처리, 파이프라인은 계속 진행)"
        )
        return [
            {"check_name": c["check_name"], "source_name": source_name, "severity": c.get("severity"),
             "reasoning": c.get("reasoning"), "issue_number": None, "issue_url": None, "is_new": None,
             "error": "GITHUB_REPOSITORY/GITHUB_TOKEN 미설정"}
            for c in anomalous
        ]

    pending = get_json(bucket, pending_result_key(source_name, execution_date_str)) or {"results": []}
    current_by_check = {r["check_name"]: r for r in pending["results"]}

    dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID", "dq_rental_history")
    task_id = os.environ.get("AIRFLOW_CTX_TASK_ID", "report_dq_issue")
    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "local")

    catalog = build_iceberg_catalog()
    results = []
    for check in anomalous:
        check_name = check["check_name"]
        current = current_by_check.get(check_name)
        if current is None:
            logger.warning("%s: pending 결과에서 못 찾음 - 이슈 생성 스킵", check_name)
            continue

        target_column = current.get("target_column", "")
        severity = check.get("severity", "warning")
        history = fetch_history(catalog, source_name, execution_date_str, lookback_days)
        history = [h for h in history if h.get("check_name") == check_name]

        title_tag = os.environ.get("DQ_ISSUE_TITLE_TAG", "").strip()
        title_prefix = f"[{title_tag}] " if title_tag else ""
        title = f"{title_prefix}[DQ] {source_name} - {check_name} 이상 감지 ({execution_date_str})"
        new_body = _new_issue_body(
            source_name, check_name, execution_date_str, current, history, check, dag_id, task_id, run_id,
        )
        comment_body = _comment_body(execution_date_str, current, check)

        try:
            issue = report_issue(
                repo=repo, token=token, source_name=source_name, check_name=check_name,
                target_column=target_column, severity=severity, title=title,
                body_for_new_issue=new_body, body_for_comment=comment_body,
            )
            results.append({
                "check_name": check_name, "source_name": source_name, "severity": severity,
                "reasoning": check.get("reasoning"), "issue_number": issue.issue_number,
                "issue_url": issue.issue_url, "is_new": issue.is_new, "error": None,
            })
            logger.info(
                "%s: GitHub 이슈 %s (%s) #%d", check_name,
                "생성" if issue.is_new else "코멘트 추가", issue.issue_url, issue.issue_number,
            )
        except Exception as exc:  # noqa: BLE001 - GitHub API 실패로 배치를 죽이지 않는다(#217 안전장치)
            logger.warning("%s: GitHub 이슈 생성/코멘트 실패 (재시도 후에도 실패) - %s", check_name, exc)
            results.append({
                "check_name": check_name, "source_name": source_name, "severity": severity,
                "reasoning": check.get("reasoning"), "issue_number": None, "issue_url": None,
                "is_new": None, "error": str(exc),
            })

    ensure_bucket(bucket)
    put_json(bucket, github_issues_key(source_name, execution_date_str), results)
    return results


if __name__ == "__main__":
    run()
