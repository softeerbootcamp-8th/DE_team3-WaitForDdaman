"""날짜 Backfill 완료 marker 잡 테스트."""

from bronze import write_rental_history_completion_marker as marker_job


def _run_with_objects(monkeypatch, manifest, promotion):
    written = {}
    monkeypatch.setenv("BACKFILL_TARGET_DATE", "2026-08-22")
    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T23:59:59+09:00")
    monkeypatch.setenv("DAG_RUN_ID", "backfill__2026-08-22")
    monkeypatch.setattr(marker_job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(
        marker_job,
        "get_json",
        lambda bucket, key: promotion if key.endswith("promotion.json") else manifest,
    )
    monkeypatch.setattr(
        marker_job,
        "put_json",
        lambda bucket, key, payload: written.update({key: payload}),
    )
    result = marker_job.run()
    return result, written


def test_complete_promotion_writes_complete_marker(monkeypatch):
    result, written = _run_with_objects(
        monkeypatch,
        manifest={"status": "COMPLETE", "row_count": 3},
        promotion={
            "status": "COMPLETE",
            "bronze_row_count_by_partition": {"2026-08-22": 3},
        },
    )

    assert result["status"] == "COMPLETE"
    assert result["row_count"] == 3
    assert list(written) == [
        "_meta/completion/bronze_rental_history/target_date=2026-08-22/completion.json"
    ]


def test_complete_empty_manifest_is_marked_for_manual_confirmation(monkeypatch):
    result, _ = _run_with_objects(
        monkeypatch,
        manifest={"status": "COMPLETE_EMPTY", "row_count": 0},
        promotion=None,
    )

    assert result["status"] == "COMPLETE_EMPTY"
    assert result["row_count"] == 0
    assert result["error"]
