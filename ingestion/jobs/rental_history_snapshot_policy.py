"""대여이력 Raw 승격 정책의 순수 함수 모음.

수집 window 계산, manifest 유효성 판정, FINAL/PRELIMINARY 후보 선택, selection/promotion
S3 key 규칙을 한곳에 모은다. 이 모듈은 S3/Spark/Airflow를 전혀 건드리지 않으므로
collector·selector·promotion·워터마크 잡이 모두 같은 규칙을 공유할 수 있다.

판단 기준은 항상 "논리 실행 값"이다 - 프로세스가 실제로 언제 떴는지가 아니라 Airflow가
넘겨준 cutoff(C)와 그 날짜(R), S3에 저장된 확정 워터마크(W)만 본다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

DATASET = "rental_history"
RAW_PREFIX = "raw/rental_history/api/"
PROMOTION_PREFIX = "_meta/promotion/bronze_rental_history/"

SNAPSHOT_FINAL = "FINAL"
SNAPSHOT_PRELIMINARY = "PRELIMINARY"

ROLE_CONFIRMED = "CONFIRMED"
ROLE_CURRENT = "CURRENT"

MODE_NORMAL = "NORMAL"
MODE_DEGRADED = "DEGRADED"

# PRELIMINARY fallback 허용 상한. 05:00 예비 -> 06:00 확정 운영을 기준으로 보수적으로 잡았다.
DEFAULT_PRELIMINARY_MAX_AGE_MINUTES = 120

# metric cardinality를 통제하기 위해 reason code는 아래 여섯 개로 고정한다.
# 자유 문자열 상세는 항상 details 쪽에만 담는다.
REASON_MISSING = "MISSING"
REASON_INCOMPLETE = "INCOMPLETE"
REASON_EMPTY = "EMPTY"
REASON_SCHEMA_INVALID = "SCHEMA_INVALID"
REASON_RANGE_MISMATCH = "RANGE_MISMATCH"
REASON_STALE_OR_FUTURE = "STALE_OR_FUTURE"

REASON_CODES = (
    REASON_MISSING,
    REASON_INCOMPLETE,
    REASON_EMPTY,
    REASON_SCHEMA_INVALID,
    REASON_RANGE_MISMATCH,
    REASON_STALE_OR_FUTURE,
)
FINAL_REASON_CODES = tuple(f"FINAL_{code}" for code in REASON_CODES)

_MANIFEST_KEY_PATTERN = re.compile(
    r"^raw/rental_history/api/target_date=(?P<target_date>\d{4}-\d{2}-\d{2})/"
    r"observed_at=(?P<observed_at>\d{8}T\d{6}[+-]\d{4})/"
    r"snapshot_type=(?P<snapshot_type>[A-Z_]+)/manifest\.json$"
)
_OBSERVED_AT_KEY_FORMAT = "%Y%m%dT%H%M%S%z"


class SnapshotPolicyError(Exception):
    """정책 입력값 자체가 잘못돼 판단을 진행할 수 없는 경우."""


@dataclass(frozen=True)
class CollectionWindow:
    """한 날짜에 대해 수집·승격이 필요한 시간대 범위."""

    target_date: date
    hours: list[int]
    required: bool
    role: str


@dataclass(frozen=True)
class Rejection:
    """후보가 탈락한 이유. code는 고정 집합, details는 사람이 읽는 자유 문자열."""

    code: str
    details: str


@dataclass(frozen=True)
class Candidate:
    """S3에서 읽어온 manifest 하나와 payload 존재 여부."""

    manifest_key: str
    manifest: Any
    payload_exists: bool = True


@dataclass
class Selection:
    """날짜별 선택 결과와 전체 mode."""

    selected: list[dict] = field(default_factory=list)
    missing: list[dict] = field(default_factory=list)
    rejections: dict[str, int] = field(default_factory=dict)
    required_confirmed_dates: list[str] = field(default_factory=list)
    current_date_required: bool = False

    @property
    def mode(self) -> str:
        if any(s["snapshot_type"] == SNAPSHOT_PRELIMINARY for s in self.selected):
            return MODE_DEGRADED
        return MODE_NORMAL

    @property
    def ok(self) -> bool:
        return not self.missing


def parse_bool(value: str | None, default: bool = False) -> bool:
    """`true`/`false`만 허용한다. 값이 없으면 default, 그 외 문자열은 안전하게 실패시킨다."""
    if value is None:
        return default
    normalized = value.strip().lower()
    if not normalized:
        return default
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"boolean flag must be 'true' or 'false': {value!r}")


def parse_max_days(value: str | None) -> int | None:
    """MAX_DAYS_PER_RUN을 읽는다. 빈 값은 수동 catch-up의 '무제한' 의미를 유지한다."""
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    parsed = int(normalized)
    if parsed < 1:
        raise ValueError(f"MAX_DAYS_PER_RUN must be >= 1: {value!r}")
    return parsed


def build_final_windows(
    cutoff: datetime,
    confirmed_through: date,
    max_days: int | None,
    t0_enabled: bool,
) -> list[CollectionWindow]:
    """확정 backlog(필수)와 당일 window(T0 플래그에 따라 필수/관측 전용)를 만든다.

    확정 구간은 워터마크 다음 날부터 전날까지이며, max_days가 있으면 가장 오래된 쪽부터
    그만큼만 잡는다. 워터마크를 건너뛰고 최신 날짜로 점프하지 않는다.
    """
    run_date = cutoff.astimezone(KST).date()
    confirmed_start = confirmed_through + timedelta(days=1)
    confirmed_end = run_date - timedelta(days=1)
    if max_days is not None:
        capped_end = confirmed_through + timedelta(days=max_days)
        if capped_end < confirmed_end:
            confirmed_end = capped_end

    windows: list[CollectionWindow] = []
    current = confirmed_start
    while current <= confirmed_end:
        windows.append(
            CollectionWindow(
                target_date=current,
                hours=list(range(24)),
                required=True,
                role=ROLE_CONFIRMED,
            )
        )
        current += timedelta(days=1)

    current_hours = list(range(cutoff.astimezone(KST).hour))
    if current_hours:
        windows.append(
            CollectionWindow(
                target_date=run_date,
                hours=current_hours,
                required=bool(t0_enabled),
                role=ROLE_CURRENT,
            )
        )
    return windows


def expected_hours(
    target_date: date, run_date: date, reference_at: datetime
) -> list[int]:
    """실행일 이전 날짜는 하루 전체, 실행일 당일은 기준 시각 직전 완료 시간대까지."""
    if target_date < run_date:
        return list(range(24))
    return list(range(reference_at.astimezone(KST).hour))


def parse_manifest_key(manifest_key: str) -> dict | None:
    """manifest key에서 target_date/observed_at/snapshot_type을 되읽는다."""
    matched = _MANIFEST_KEY_PATTERN.match(manifest_key)
    if not matched:
        return None
    try:
        observed_at = datetime.strptime(
            matched.group("observed_at"), _OBSERVED_AT_KEY_FORMAT
        ).astimezone(KST)
    except ValueError:
        return None
    return {
        "target_date": date.fromisoformat(matched.group("target_date")),
        "observed_at": observed_at,
        "snapshot_type": matched.group("snapshot_type"),
        "payload_key": manifest_key.replace("manifest.json", "payload.json"),
    }


def _parse_iso_kst(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(KST)


def validate_manifest(
    manifest: Any,
    manifest_key: str,
    expected_hours: Sequence[int],
    payload_exists: bool = True,
) -> Rejection | None:
    """manifest 하나가 승격 후보로 쓸 수 있는지 판정한다. 유효하면 None을 돌려준다."""
    parsed_key = parse_manifest_key(manifest_key)
    if parsed_key is None:
        return Rejection(REASON_SCHEMA_INVALID, f"manifest key 형식 불일치: {manifest_key}")

    if not isinstance(manifest, dict):
        return Rejection(REASON_MISSING, f"manifest 없음 또는 JSON 파싱 실패: {manifest_key}")

    if manifest.get("dataset") != DATASET:
        return Rejection(REASON_SCHEMA_INVALID, f"dataset 불일치: {manifest.get('dataset')!r}")

    if manifest.get("target_date") != parsed_key["target_date"].isoformat():
        return Rejection(
            REASON_SCHEMA_INVALID,
            f"target_date가 key와 다름: {manifest.get('target_date')!r}",
        )

    if manifest.get("snapshot_type") != parsed_key["snapshot_type"]:
        return Rejection(
            REASON_SCHEMA_INVALID,
            f"snapshot_type이 key와 다름: {manifest.get('snapshot_type')!r}",
        )

    observed_at = _parse_iso_kst(manifest.get("observed_at"))
    if observed_at is None or observed_at != parsed_key["observed_at"]:
        return Rejection(
            REASON_SCHEMA_INVALID,
            f"observed_at이 key와 다름: {manifest.get('observed_at')!r}",
        )

    if manifest.get("payload_key") != parsed_key["payload_key"]:
        return Rejection(
            REASON_SCHEMA_INVALID,
            f"payload_key가 key와 다름: {manifest.get('payload_key')!r}",
        )

    status = manifest.get("status")
    if status == "COMPLETE_EMPTY":
        return Rejection(REASON_EMPTY, "status=COMPLETE_EMPTY")
    if status != "COMPLETE":
        return Rejection(REASON_INCOMPLETE, f"status={status!r}")

    if manifest.get("schema_valid") is not True:
        return Rejection(REASON_SCHEMA_INVALID, f"schema_valid={manifest.get('schema_valid')!r}")

    requested = manifest.get("requested_hours")
    if requested != list(expected_hours):
        return Rejection(
            REASON_RANGE_MISMATCH,
            f"requested_hours={requested!r} expected={list(expected_hours)!r}",
        )
    if manifest.get("completed_hours") != requested:
        return Rejection(
            REASON_INCOMPLETE,
            f"completed_hours={manifest.get('completed_hours')!r} requested={requested!r}",
        )

    row_count = manifest.get("row_count")
    if not isinstance(row_count, int) or row_count <= 0:
        return Rejection(REASON_EMPTY, f"row_count={row_count!r}")

    if not payload_exists:
        return Rejection(REASON_MISSING, f"payload 객체 없음: {parsed_key['payload_key']}")

    return None


def validate_preliminary_freshness(
    observed_at: datetime,
    cutoff: datetime,
    max_age_minutes: int = DEFAULT_PRELIMINARY_MAX_AGE_MINUTES,
) -> Rejection | None:
    """예비 관측본의 최신성을 범위 동일성과 분리해서 검증한다."""
    observed_at = observed_at.astimezone(KST)
    cutoff = cutoff.astimezone(KST)

    if observed_at.date() != cutoff.date():
        return Rejection(
            REASON_STALE_OR_FUTURE,
            f"실행일과 다른 날짜에 관측됨: observed_at={observed_at.isoformat()}",
        )
    if observed_at >= cutoff:
        return Rejection(
            REASON_STALE_OR_FUTURE,
            f"cutoff 이후 관측본: observed_at={observed_at.isoformat()}",
        )
    age_minutes = (cutoff - observed_at).total_seconds() / 60
    if age_minutes > max_age_minutes:
        return Rejection(
            REASON_STALE_OR_FUTURE,
            f"age={age_minutes:.0f}분 > 상한 {max_age_minutes}분",
        )
    return None


def final_reason_code(code: str) -> str:
    """후보 탈락 사유를 promotion metadata용 고정 reason code로 바꾼다."""
    if code not in REASON_CODES:
        raise SnapshotPolicyError(f"unknown rejection code: {code}")
    return f"FINAL_{code}"


def _count(rejections: dict[str, int], code: str) -> None:
    rejections[code] = rejections.get(code, 0) + 1


def _selected_entry(
    parsed_key: dict, manifest: dict, fallback_reason: str | None
) -> dict:
    return {
        "target_date": manifest["target_date"],
        "snapshot_type": manifest["snapshot_type"],
        "observed_at": manifest["observed_at"],
        "payload_key": manifest["payload_key"],
        "manifest_key": parsed_key["payload_key"].replace("payload.json", "manifest.json"),
        "requested_hours": list(manifest["requested_hours"]),
        "row_count": manifest["row_count"],
        "fallback_reason": fallback_reason,
    }


def select_snapshots(
    cutoff: datetime,
    windows: Sequence[CollectionWindow],
    candidates: Iterable[Candidate],
    fallback_enabled: bool,
    max_age_minutes: int = DEFAULT_PRELIMINARY_MAX_AGE_MINUTES,
) -> Selection:
    """필수 window마다 FINAL 우선, 허용될 때만 PRELIMINARY로 채워 선택을 확정한다.

    optional window(T0=false의 당일)는 관측 전용이므로 선택 대상에서 아예 제외한다.
    필수 window 중 하나라도 후보가 없으면 missing에 남기고 부분 승격을 만들지 않는다.
    """
    cutoff = cutoff.astimezone(KST)
    run_date = cutoff.date()
    fallback_dates = {run_date, run_date - timedelta(days=1)}

    by_date: dict[date, list[tuple[dict, Candidate]]] = {}
    for candidate in candidates:
        parsed_key = parse_manifest_key(candidate.manifest_key)
        if parsed_key is None:
            continue
        by_date.setdefault(parsed_key["target_date"], []).append((parsed_key, candidate))

    selection = Selection()
    selection.required_confirmed_dates = [
        window.target_date.isoformat()
        for window in windows
        if window.required and window.role == ROLE_CONFIRMED
    ]
    selection.current_date_required = any(
        window.required and window.role == ROLE_CURRENT for window in windows
    )

    for window in windows:
        if not window.required:
            continue

        target_candidates = by_date.get(window.target_date, [])
        final_rejection = _pick_final(
            cutoff, window, target_candidates, selection.rejections
        )
        if isinstance(final_rejection, tuple):
            parsed_key, manifest = final_rejection
            selection.selected.append(_selected_entry(parsed_key, manifest, None))
            continue

        reason = final_reason_code(final_rejection.code)
        if not fallback_enabled:
            selection.missing.append(
                {
                    "target_date": window.target_date.isoformat(),
                    "reason": reason,
                    "details": f"fallback 비활성: {final_rejection.details}",
                }
            )
            continue
        if window.target_date not in fallback_dates:
            selection.missing.append(
                {
                    "target_date": window.target_date.isoformat(),
                    "reason": reason,
                    "details": (
                        "fallback은 전날/당일에만 허용: " f"{final_rejection.details}"
                    ),
                }
            )
            continue

        picked = _pick_preliminary(
            cutoff, window, target_candidates, max_age_minutes, selection.rejections
        )
        if picked is None:
            selection.missing.append(
                {
                    "target_date": window.target_date.isoformat(),
                    "reason": reason,
                    "details": (
                        "유효한 PRELIMINARY 후보 없음: " f"{final_rejection.details}"
                    ),
                }
            )
            continue

        parsed_key, manifest = picked
        selection.selected.append(_selected_entry(parsed_key, manifest, reason))

    return selection


def _pick_final(
    cutoff: datetime,
    window: CollectionWindow,
    target_candidates: Sequence[tuple[dict, Candidate]],
    rejections: dict[str, int],
) -> tuple[dict, dict] | Rejection:
    """현재 logical run의 FINAL만 후보로 본다. 유효하면 (key, manifest)를 돌려준다."""
    run_date = cutoff.date()
    matched = [
        (parsed_key, candidate)
        for parsed_key, candidate in target_candidates
        if parsed_key["snapshot_type"] == SNAPSHOT_FINAL
    ]
    if not matched:
        return Rejection(REASON_MISSING, f"{window.target_date} FINAL manifest 없음")

    current_run = [
        (parsed_key, candidate)
        for parsed_key, candidate in matched
        if parsed_key["observed_at"] == cutoff
    ]
    if not current_run:
        _count(rejections, REASON_STALE_OR_FUTURE)
        return Rejection(
            REASON_STALE_OR_FUTURE,
            f"{window.target_date} 현재 run(cutoff={cutoff.isoformat()})의 FINAL 없음",
        )

    parsed_key, candidate = current_run[-1]
    rejection = validate_manifest(
        manifest=candidate.manifest,
        manifest_key=candidate.manifest_key,
        expected_hours=expected_hours(window.target_date, run_date, cutoff),
        payload_exists=candidate.payload_exists,
    )
    if rejection is not None:
        _count(rejections, rejection.code)
        return rejection
    return parsed_key, candidate.manifest


def _pick_preliminary(
    cutoff: datetime,
    window: CollectionWindow,
    target_candidates: Sequence[tuple[dict, Candidate]],
    max_age_minutes: int,
    rejections: dict[str, int],
) -> tuple[dict, dict] | None:
    """유효한 예비 후보 중 observed_at이 가장 최신인 것을 고른다."""
    run_date = cutoff.date()
    valid: list[tuple[datetime, dict, dict]] = []
    for parsed_key, candidate in target_candidates:
        if parsed_key["snapshot_type"] != SNAPSHOT_PRELIMINARY:
            continue
        observed_at = parsed_key["observed_at"]
        freshness = validate_preliminary_freshness(observed_at, cutoff, max_age_minutes)
        if freshness is not None:
            _count(rejections, freshness.code)
            continue
        hours = expected_hours(window.target_date, run_date, observed_at)
        if not hours:
            _count(rejections, REASON_RANGE_MISMATCH)
            continue
        rejection = validate_manifest(
            manifest=candidate.manifest,
            manifest_key=candidate.manifest_key,
            expected_hours=hours,
            payload_exists=candidate.payload_exists,
        )
        if rejection is not None:
            _count(rejections, rejection.code)
            continue
        valid.append((observed_at, parsed_key, candidate.manifest))

    if not valid:
        return None
    observed_at, parsed_key, manifest = max(valid, key=lambda item: item[0])
    return parsed_key, manifest


def build_promotion_id(cutoff: datetime) -> str:
    """논리 cutoff를 compact KST 문자열로 바꾼 promotion 식별자."""
    return cutoff.astimezone(KST).strftime(_OBSERVED_AT_KEY_FORMAT)


def _promotion_prefix(run_date: date, promotion_id: str) -> str:
    return (
        f"{PROMOTION_PREFIX}run_date={run_date.isoformat()}/"
        f"promotion_id={promotion_id}/"
    )


def selection_key(run_date: date, promotion_id: str) -> str:
    """같은 DAGRun 재시도가 같은 key를 덮어쓰도록 결정적으로 만든다."""
    return f"{_promotion_prefix(run_date, promotion_id)}selection.json"


def promotion_key(run_date: date, promotion_id: str) -> str:
    """Iceberg commit 이후에만 쓰는 Bronze commit marker의 key."""
    return f"{_promotion_prefix(run_date, promotion_id)}promotion.json"


def raw_target_prefix(target_date: date) -> str:
    """manifest 탐색 시 대상 날짜 prefix만 조회하기 위한 key prefix."""
    return f"{RAW_PREFIX}target_date={target_date.isoformat()}/"
