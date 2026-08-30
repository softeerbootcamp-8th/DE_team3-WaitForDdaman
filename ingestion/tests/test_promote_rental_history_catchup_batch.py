"""Catchup 전용 대여이력 배치 승격 테스트.

일 배치의 후보 선택(selection.json)도, manifest 재검증도 하지 않는다. 수집 성공은
prepare task가 보장하므로, 이 잡은 넘겨받은 날짜의 결정적 FINAL Raw payload를 읽어
한 번의 commit으로 반영하는 책임만 진다.
"""
from datetime import date

import pytest

from bronze import promote_rental_history_catchup_batch as batch_job

VALID_ROW = {
    "BIKE_ID": "SPB-1",
    "RENT_DT": "2026-05-01 04:00:00",
    "RENT_ID": "101",
    "RTN_DT": "2026-05-01 04:10:00",
    "RTN_ID": "102",
    "USE_MIN": "10",
    "USE_DST": "1234.5",
}


class _FakeStore:
    """S3 대신 쓰는 인메모리 저장소. put/get 호출 순서를 그대로 기록한다."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.writes = []
        self.reads = []

    def get(self, bucket, key):
        self.reads.append(key)
        return self.objects.get(key)

    def put(self, bucket, key, value):
        self.objects[key] = value
        self.writes.append(key)


def _seed(dates, rows_per_date=1):
    objects = {}
    for d in dates:
        payload_key, _ = batch_job.final_snapshot_keys(date.fromisoformat(d))
        objects[payload_key] = [VALID_ROW] * rows_per_date
    return objects


def _counting_promote(payloads):
    return {p["rent_date_partition"]: p["row_count"] for p in payloads}


# ------------------------------------------------------------ 배치 경계


def test_batches_are_deterministic_and_sized():
    dates = [f"2026-05-{d:02d}" for d in range(1, 15)]

    assert batch_job.build_batches(dates, batch_size=6) == [
        [f"2026-05-{d:02d}" for d in range(1, 7)],
        [f"2026-05-{d:02d}" for d in range(7, 13)],
        ["2026-05-13", "2026-05-14"],
    ]


def test_batches_preserve_given_order_without_resorting():
    """gap 목록이 불연속이어도 순서를 바꾸지 않는다 - 재실행 시 같은 그룹이 재현돼야 한다."""
    dates = ["2026-05-01", "2026-05-05", "2026-05-09"]

    assert batch_job.build_batches(dates, batch_size=2) == [
        ["2026-05-01", "2026-05-05"],
        ["2026-05-09"],
    ]


def test_batch_size_must_be_positive():
    with pytest.raises(ValueError, match="batch_size"):
        batch_job.build_batches(["2026-05-01"], batch_size=0)


def test_default_batch_size_is_six():
    assert batch_job.DEFAULT_BATCH_SIZE == 6


# ------------------------------- 후보 선택도 manifest 재검증도 하지 않는다


def test_reads_only_the_deterministic_final_payload():
    """selection.json도, manifest도 읽지 않는다 - 수집 성공은 prepare가 보장한다."""
    store = _FakeStore(_seed(["2026-05-01"]))

    batch_job.promote_batch(
        "bucket", ["2026-05-01"], batch_id="b1", batch_size=6,
        read_json=store.get, write_json=store.put, promote_fn=_counting_promote,
    )

    assert not any("selection.json" in k for k in store.reads)
    assert not any("manifest.json" in k for k in store.reads)
    assert all("observed_at=20260501T235959+0900" in k for k in store.reads)
    assert all(k.endswith("payload.json") for k in store.reads)


def test_payload_is_promoted_without_manifest_present():
    """manifest가 없어도 payload만 있으면 승격한다 - 재검증하지 않는다는 뜻."""
    store = _FakeStore(_seed(["2026-05-01"], rows_per_date=3))

    result = batch_job.promote_batch(
        "bucket", ["2026-05-01"], batch_id="b1", batch_size=6,
        read_json=store.get, write_json=store.put, promote_fn=_counting_promote,
    )

    assert result["promoted_dates"] == ["2026-05-01"]


def test_missing_payload_fails_before_any_commit():
    """payload가 실제로 없으면 Iceberg를 건드리기 전에 실패한다."""
    store = _FakeStore()
    committed = []

    with pytest.raises(batch_job.CatchupPromotionError, match="payload"):
        batch_job.promote_batch(
            "bucket", ["2026-05-01"], batch_id="b1", batch_size=6,
            read_json=store.get, write_json=store.put,
            promote_fn=lambda p: committed.append(p) or {},
        )

    assert committed == []
    assert store.writes == []


def test_non_array_payload_fails_before_any_commit():
    payload_key, _ = batch_job.final_snapshot_keys(date(2026, 5, 1))
    store = _FakeStore({payload_key: {"rows": [VALID_ROW]}})

    with pytest.raises(batch_job.CatchupPromotionError, match="배열"):
        batch_job.promote_batch(
            "bucket", ["2026-05-01"], batch_id="b1", batch_size=6,
            read_json=store.get, write_json=store.put, promote_fn=_counting_promote,
        )

    assert store.writes == []


def test_one_missing_payload_blocks_the_whole_batch():
    """반쪽짜리 커밋을 만들지 않는다 - 전량 로드에 성공해야 커밋한다."""
    store = _FakeStore(_seed(["2026-05-01"]))  # 2일차 payload 없음
    committed = []

    with pytest.raises(batch_job.CatchupPromotionError):
        batch_job.promote_batch(
            "bucket", ["2026-05-01", "2026-05-02"], batch_id="b1", batch_size=6,
            read_json=store.get, write_json=store.put,
            promote_fn=lambda p: committed.append(p) or {},
        )

    assert committed == []
    assert store.writes == []


# --------------------------------------------------------- 단일 커밋


def test_batch_is_committed_in_a_single_call():
    dates = ["2026-05-01", "2026-05-02", "2026-05-03"]
    store = _FakeStore(_seed(dates))
    calls = []

    def recording_promote(payloads):
        calls.append([p["rent_date_partition"] for p in payloads])
        return _counting_promote(payloads)

    batch_job.promote_batch(
        "bucket", dates, batch_id="b1", batch_size=6,
        read_json=store.get, write_json=store.put, promote_fn=recording_promote,
    )

    assert calls == [dates]


# ------------------------------------------------------- marker 순서


def test_promotion_markers_are_written_only_after_the_commit():
    """커밋 성공 이전에는 어떤 promotion marker도 쓰지 않는다 (marker-last)."""
    dates = ["2026-05-01", "2026-05-02"]
    store = _FakeStore(_seed(dates))
    order = []

    def fake_promote(payloads):
        order.append("commit")
        return _counting_promote(payloads)

    def recording_put(bucket, key, value):
        order.append(f"put:{key}")
        store.put(bucket, key, value)

    result = batch_job.promote_batch(
        "bucket", dates, batch_id="b1", batch_size=6,
        read_json=store.get, write_json=recording_put, promote_fn=fake_promote,
    )

    assert order[0] == "commit"
    assert all(o.startswith("put:") for o in order[1:])
    assert len([o for o in order if "_meta/promotion" in o]) == 2
    assert result["promoted_dates"] == dates


def test_commit_failure_writes_no_promotion_marker():
    store = _FakeStore(_seed(["2026-05-01"]))

    def failing_promote(payloads):
        raise RuntimeError("iceberg commit 실패")

    with pytest.raises(RuntimeError, match="iceberg"):
        batch_job.promote_batch(
            "bucket", ["2026-05-01"], batch_id="b1", batch_size=6,
            read_json=store.get, write_json=store.put, promote_fn=failing_promote,
        )

    assert store.writes == []


def test_empty_date_list_is_a_noop():
    store = _FakeStore()
    committed = []

    result = batch_job.promote_batch(
        "bucket", [], batch_id="b1", batch_size=6,
        read_json=store.get, write_json=store.put,
        promote_fn=lambda p: committed.append(p) or {},
    )

    assert result["promoted_dates"] == []
    assert committed == []
    assert store.writes == []


# --------------------------------------------------- promotion 문서


def test_promotion_document_carries_batch_audit_fields():
    """날짜별 문서를 유지하되 같은 단일 commit에 포함됐음을 batch_id로 추적한다."""
    dates = ["2026-05-01", "2026-05-02"]
    store = _FakeStore(_seed(dates))

    batch_job.promote_batch(
        "bucket", dates, batch_id="20260501T000000+0900", batch_size=6,
        read_json=store.get, write_json=store.put, promote_fn=_counting_promote,
    )

    doc = store.objects[batch_job.promotion_marker_key(date(2026, 5, 1))]
    assert doc["status"] == "COMPLETE"
    assert doc["batch_id"] == "20260501T000000+0900"
    assert doc["batch_size"] == 6
    assert doc["batch_dates"] == dates
    assert doc["promoted_partitions"] == ["2026-05-01"]


def test_promotion_document_is_compatible_with_completion_marker_reader():
    """write_rental_history_completion_marker가 보는 필드를 그대로 갖춰야 한다."""
    store = _FakeStore(_seed(["2026-05-01"], rows_per_date=7))

    batch_job.promote_batch(
        "bucket", ["2026-05-01"], batch_id="b1", batch_size=6,
        read_json=store.get, write_json=store.put, promote_fn=_counting_promote,
    )

    doc = store.objects[batch_job.promotion_marker_key(date(2026, 5, 1))]
    assert doc["status"] == "COMPLETE"
    assert isinstance(doc["bronze_row_count_by_partition"], dict)
    assert sum(doc["bronze_row_count_by_partition"].values()) == 7


# ------------------------------------------------------------ 멱등성


def test_rerunning_the_same_batch_is_idempotent():
    dates = ["2026-05-01", "2026-05-02"]
    store = _FakeStore(_seed(dates))

    first = batch_job.promote_batch(
        "bucket", dates, batch_id="b1", batch_size=6,
        read_json=store.get, write_json=store.put, promote_fn=_counting_promote,
    )
    keys_after_first = {k for k in store.objects if "_meta/promotion" in k}
    second = batch_job.promote_batch(
        "bucket", dates, batch_id="b1", batch_size=6,
        read_json=store.get, write_json=store.put, promote_fn=_counting_promote,
    )

    assert first["promoted_dates"] == second["promoted_dates"]
    assert keys_after_first == {k for k in store.objects if "_meta/promotion" in k}
