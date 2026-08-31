"""EMR Serverless 로컬 dry-run 테스트."""

import boto3


def test_emr_dry_run_does_not_call_aws(monkeypatch, capsys):
    from dag_common import run_emr_serverless_spark_job

    monkeypatch.setenv("EMR_SERVERLESS_DRY_RUN", "true")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("dry-run에서는 boto3 EMR API를 호출하면 안 됩니다")

    monkeypatch.setattr(boto3, "client", fail_if_called)

    result = run_emr_serverless_spark_job(
        entry_point="local:///opt/app/src/bronze/initial_load_rental_history.py",
        name="test-initial-load",
        entry_point_arguments=["--input-files-json", '["s3://bucket/a.csv"]'],
    )

    assert result == "dry-run:test-initial-load"
    output = capsys.readouterr().out
    assert "--input-files-json" in output
    assert "s3://bucket/a.csv" in output
