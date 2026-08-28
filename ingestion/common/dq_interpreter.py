"""
DQ 해석 에이전트 (#217, 1차 버전).

이번 실행의 어써션 결과 + 최근 N일 히스토리를 dq.check_result_history에서 조회해
LLM(OpenAI)에 넘기고, "과거 대비 이상인지 / 원인 후보 / 조치 필요 여부"를 구조화된
JSON으로 돌려받는다. Slack 알림은 다음 단계 - 이 모듈은 해석 결과를 만들기만 하고
어디에 알릴지는 호출하는 잡(jobs/interpret_dq_results.py)의 책임이다.

원래 요구사항은 Claude(Anthropic)였으나 로컬 검증 중 Anthropic 계정 크레딧 부족으로
OpenAI로 전환했다 - interpret()의 호출부만 다르고 프롬프트/판정 로직은 동일하다.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

from common.dq_result_store import RESULT_TABLE

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gpt-4o"
DEFAULT_LOOKBACK_DAYS = 14

_SYSTEM_PROMPT = (
    "당신은 데이터 파이프라인의 데이터 품질(DQ) 이상 탐지를 담당하는 시니어 데이터 엔지니어입니다. "
    "주어진 current_run과 history를 비교해 각 check_name별로 이상 여부를 판단하고, "
    "지정된 JSON 스키마로만 응답하세요. 다른 텍스트를 절대 포함하지 마세요. "
    "reasoning과 suggested_action 값은 반드시 한국어로 작성하세요 - GitHub 이슈/Slack "
    "알림에 그대로 노출되어 개발자가 읽는 텍스트입니다. severity/is_anomaly 등 "
    "스키마상의 다른 필드값(예: \"critical\"/\"warning\"/\"info\")은 지정된 영문 값을 그대로 쓰세요. "
    "pass_fail이 \"ERROR\"이고 metric_value가 null인 체크는 지표값이 이상해서가 아니라 "
    "테이블에서 컬럼을 찾지 못해 계산 자체가 실패한 것입니다(스키마 변경 의심) - 이 경우 "
    "반드시 is_anomaly=true, severity는 최소 \"critical\"로 판단하고, reasoning에 "
    "\"스키마 불일치로 지표 계산 실패\"라는 취지를 명시하세요."
)

_RESPONSE_SCHEMA_HINT = """{
  "source_name": "string",
  "execution_date": "string",
  "checks": [
    {
      "check_name": "string",
      "is_anomaly": true,
      "severity": "critical|warning|info",
      "reasoning": "string",
      "suggested_action": "string"
    }
  ],
  "overall_severity": "critical|warning|info"
}"""


def fetch_history(
    catalog: Catalog,
    source_name: str,
    execution_date: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> list[dict[str, Any]]:
    """execution_date 이전 lookback_days일치 히스토리를 조회한다 (당일 자기 자신은 제외).

    테이블이 아직 없으면(첫 실행) 빈 히스토리를 돌려준다 - 해석 에이전트가
    "비교 대상 없음"으로 처리하게 한다.
    """
    try:
        table = catalog.load_table(RESULT_TABLE)
    except NoSuchTableError:
        return []

    end_date = date.fromisoformat(execution_date)
    start_date = end_date - timedelta(days=lookback_days)

    arrow = table.scan(
        row_filter=And(
            EqualTo("source_name", source_name),
            And(
                GreaterThanOrEqual("execution_date", start_date.isoformat()),
                LessThan("execution_date", execution_date),
            ),
        ),
    ).to_arrow()

    return arrow.to_pylist() if len(arrow) else []


def build_prompt(
    source_name: str,
    execution_date: str,
    lookback_days: int,
    current_run: list[dict[str, Any]],
    history: list[dict[str, Any]],
) -> str:
    sorted_history = sorted(history, key=lambda r: r.get("execution_date", ""))
    return f"""아래는 "{source_name}" 소스에 대해 {execution_date} 배치에서 실행한 SQL 어써션 결과(current_run)와,
동일한 체크들의 최근 {lookback_days}일 히스토리(history, 오래된 순)입니다.

current_run:
{json.dumps(current_run, ensure_ascii=False, indent=2, default=str)}

history:
{json.dumps(sorted_history, ensure_ascii=False, indent=2, default=str)}

각 check_name별로 판단하세요. threshold가 없는(MONITOR) 체크도 과거 대비 급격한 추세
변화가 있으면 이상으로 볼 수 있습니다 (예: 결측률이 평소 20%대였는데 이번에 40%로 튐).
단순히 pass_fail=FAIL 여부만 보지 말고, history 대비 추세 이탈 여부를 반드시 함께 고려하세요.
history가 비어 있으면(첫 실행) 비교 대상이 없다는 점을 reasoning에 명시하고 is_anomaly=false로
판단하세요.

다음 JSON 스키마로만 응답하세요 (다른 텍스트 금지):
{_RESPONSE_SCHEMA_HINT}
"""


def interpret(
    source_name: str,
    execution_date: str,
    current_run: list[dict[str, Any]],
    history: list[dict[str, Any]],
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    model: str = DEFAULT_MODEL,
    api_key: str | None = None,
) -> dict[str, Any]:
    """OpenAI API를 호출해 구조화된 해석 결과를 반환한다.

    원래 Claude(Anthropic)로 설계했으나 로컬 검증 중 Anthropic 계정 크레딧 부족으로
    OpenAI로 전환했다(#217) - 호출부만 OpenAI SDK로 바꿨고, 프롬프트/스키마/판정
    로직은 모델 제공자와 무관하게 동일하다.
    """
    from openai import OpenAI

    client = OpenAI(api_key=api_key) if api_key else OpenAI()
    prompt = build_prompt(source_name, execution_date, lookback_days, current_run, history)

    response = client.chat.completions.create(
        model=model,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    text = response.choices[0].message.content

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        logger.error("해석 에이전트 응답이 JSON이 아님: %s", text)
        raise ValueError(f"해석 에이전트 응답 파싱 실패: {exc}") from exc
