"""Tests for the deputies snapshot Raw metadata.json (v1.0 + audit envelope)."""

from __future__ import annotations

from shared.deputies_snapshot import (
    build_deputies_snapshot_metadata,
    deputies_metadata_path,
    deputies_success_path,
    persist_deputies_snapshot_metadata,
)


def _build(status: str, **overrides):
    base = dict(
        pipeline_run_id="ceap_daily_20260510",
        execution_id="exec-1",
        reference_date="2026-05-10",
        reference_timezone="America/Sao_Paulo",
        status=status,
        started_at_utc="2026-05-10T00:00:00+00:00",
        completed_at_utc=None,
        total_pages=0,
        record_count=0,
        error_message=None,
    )
    base.update(overrides)
    return build_deputies_snapshot_metadata(**base)


def test_running_metadata_has_required_audit_fields() -> None:
    meta = _build("RUNNING")
    assert meta["status"] == "RUNNING"
    assert meta["metadata_version"] == "1.0"
    assert meta["pipeline_run_id"] == "ceap_daily_20260510"
    assert meta["execution_id"] == "exec-1"
    assert meta["reference_date"] == "2026-05-10"
    assert meta["reference_timezone"] == "America/Sao_Paulo"
    for key in (
        "_pipeline_run_id",
        "_execution_id",
        "_source_system",
        "_source_endpoint",
        "_reference_date",
        "_ingested_at_utc",
        "_loaded_at",
    ):
        assert key in meta, f"missing audit field {key}"
    assert meta["_pipeline_run_id"] == "ceap_daily_20260510"
    assert meta["_execution_id"] == "exec-1"
    assert meta["_source_endpoint"] == "deputado"


def test_completed_metadata_has_snapshot_block_and_counts() -> None:
    meta = _build(
        "COMPLETED",
        completed_at_utc="2026-05-10T01:00:00+00:00",
        total_pages=6,
        record_count=513,
    )
    assert meta["status"] == "COMPLETED"
    assert meta["total_pages"] == 6
    assert meta["files_written"] == 6
    assert meta["record_count"] == 513
    assert meta["snapshot_status"] == "COMPLETED"
    assert meta["snapshot_record_count"] == 513
    assert meta["snapshot_type"] == "dimension"


def test_persist_running_does_not_write_success_marker(raw_writer) -> None:
    meta = _build("RUNNING")
    persist_deputies_snapshot_metadata(
        raw_writer, "2026-05-10", meta, write_success_marker_now=False
    )
    assert raw_writer.path_exists(deputies_metadata_path("2026-05-10"))
    assert not raw_writer.path_exists(deputies_success_path("2026-05-10"))


def test_persist_completed_writes_success_marker(raw_writer) -> None:
    meta = _build(
        "COMPLETED",
        completed_at_utc="2026-05-10T01:00:00+00:00",
        total_pages=6,
        record_count=513,
    )
    persist_deputies_snapshot_metadata(
        raw_writer, "2026-05-10", meta, write_success_marker_now=True
    )
    assert raw_writer.path_exists(deputies_success_path("2026-05-10"))


def test_persist_failed_does_not_write_success_marker(raw_writer) -> None:
    meta = _build(
        "FAILED",
        completed_at_utc=None,
        total_pages=0,
        record_count=0,
        error_message="HTTPError: 503",
    )
    persist_deputies_snapshot_metadata(
        raw_writer, "2026-05-10", meta, write_success_marker_now=True
    )
    assert not raw_writer.path_exists(deputies_success_path("2026-05-10"))
    assert raw_writer.path_exists(deputies_metadata_path("2026-05-10"))


def test_persist_partially_completed_does_not_write_success_marker(raw_writer) -> None:
    meta = _build(
        "PARTIALLY_COMPLETED",
        completed_at_utc=None,
        total_pages=2,
        record_count=120,
        error_message="HTTPError: 500 on page 3",
    )
    persist_deputies_snapshot_metadata(
        raw_writer, "2026-05-10", meta, write_success_marker_now=True
    )
    assert not raw_writer.path_exists(deputies_success_path("2026-05-10"))
    assert raw_writer.path_exists(deputies_metadata_path("2026-05-10"))


def test_metadata_uses_v1_endpoints_and_paths() -> None:
    meta = _build("RUNNING")
    assert meta["api_path"] == "/deputados"
    assert meta["api_base_url"].endswith("/api/v2")
    assert meta["raw_path"].startswith(
        "raw/camara/deputados/api/list/reference_date=2026-05-10"
    )
    assert meta["success_marker_path"].endswith("_SUCCESS")
