"""Tests for ``shared.ceap_raw_manifest`` (CEAP run-level metadata.json)."""

from __future__ import annotations

from shared.ceap_raw_manifest import (
    build_ceap_dispatcher_run_metadata,
    ceap_run_metadata_path,
    ceap_run_success_path,
    is_ceap_run_manifest_valid_for_bronze,
    persist_ceap_dispatcher_run_metadata,
    read_ceap_run_metadata,
)


def _build_running_meta(pipeline_run_id: str = "ceap_daily_20260510"):
    return build_ceap_dispatcher_run_metadata(
        pipeline_run_id=pipeline_run_id,
        mode="daily",
        status="RUNNING",
        started_at_utc="2026-05-10T00:00:00+00:00",
        finished_at_utc=None,
        failed_at_utc=None,
        total_tasks_expected=0,
        total_tasks_queued=0,
        total_tasks_pending=0,
        target_year=2026,
        months_to_process_json="[4,5]",
        enqueue_phase_complete=False,
        deputies_snapshot_date="2026-05-10",
        deputies_snapshot_path="raw/camara/deputados/api/list/reference_date=2026-05-10",
        deputies_snapshot_record_count=513,
        deputies_snapshot_status="COMPLETED",
    )


def _build_completed_meta(pipeline_run_id: str = "ceap_daily_20260510"):
    return build_ceap_dispatcher_run_metadata(
        pipeline_run_id=pipeline_run_id,
        mode="daily",
        status="COMPLETED",
        started_at_utc="2026-05-10T00:00:00+00:00",
        finished_at_utc="2026-05-10T01:00:00+00:00",
        failed_at_utc=None,
        total_tasks_expected=10,
        total_tasks_queued=0,
        total_tasks_pending=0,
        target_year=2026,
        months_to_process_json="[4,5]",
        enqueue_phase_complete=True,
        deputies_snapshot_date="2026-05-10",
        deputies_snapshot_path="raw/camara/deputados/api/list/reference_date=2026-05-10",
        deputies_snapshot_record_count=513,
        deputies_snapshot_status="COMPLETED",
        total_tasks_success=10,
        total_tasks_failed=0,
        total_tasks_poison=0,
        total_tasks_running=0,
    )


def test_running_manifest_does_not_write_success_marker(raw_writer) -> None:
    pid = "ceap_daily_20260510"
    meta = _build_running_meta(pid)

    persist_ceap_dispatcher_run_metadata(
        raw_writer, meta, write_success_marker_now=False
    )
    assert raw_writer.path_exists(ceap_run_metadata_path(pid))
    assert not raw_writer.path_exists(ceap_run_success_path(pid))


def test_completed_manifest_writes_success_when_allowed(raw_writer) -> None:
    pid = "ceap_daily_20260510"
    meta = _build_completed_meta(pid)
    persist_ceap_dispatcher_run_metadata(
        raw_writer, meta, write_success_marker_now=True
    )
    assert raw_writer.path_exists(ceap_run_success_path(pid))
    stored = read_ceap_run_metadata(raw_writer, pid) or {}
    assert stored["status"] == "COMPLETED"
    assert stored["pipeline_run_id"] == pid


def test_failed_manifest_never_writes_success_marker(raw_writer) -> None:
    pid = "ceap_daily_20260510"
    meta = build_ceap_dispatcher_run_metadata(
        pipeline_run_id=pid,
        mode="daily",
        status="FAILED",
        started_at_utc="2026-05-10T00:00:00+00:00",
        finished_at_utc=None,
        failed_at_utc="2026-05-10T00:30:00+00:00",
        total_tasks_expected=0,
        total_tasks_queued=0,
        total_tasks_pending=0,
        target_year=2026,
        months_to_process_json="[4]",
        enqueue_phase_complete=False,
        error_type="QueueSendError",
        error_message="connection timeout",
    )
    persist_ceap_dispatcher_run_metadata(
        raw_writer, meta, write_success_marker_now=True
    )
    assert raw_writer.path_exists(ceap_run_metadata_path(pid))
    assert not raw_writer.path_exists(ceap_run_success_path(pid))
    stored = read_ceap_run_metadata(raw_writer, pid) or {}
    assert stored["status"] == "FAILED"
    assert stored["error_type"] == "QueueSendError"


def test_partially_completed_manifest_does_not_write_success_marker(raw_writer) -> None:
    pid = "ceap_daily_20260510"
    meta = build_ceap_dispatcher_run_metadata(
        pipeline_run_id=pid,
        mode="daily",
        status="PARTIALLY_COMPLETED",
        started_at_utc="2026-05-10T00:00:00+00:00",
        finished_at_utc=None,
        failed_at_utc="2026-05-10T00:30:00+00:00",
        total_tasks_expected=10,
        total_tasks_queued=4,
        total_tasks_pending=6,
        target_year=2026,
        months_to_process_json="[4,5]",
        enqueue_phase_complete=False,
        total_tasks_success=4,
        total_tasks_failed=0,
    )
    persist_ceap_dispatcher_run_metadata(
        raw_writer, meta, write_success_marker_now=False
    )
    assert raw_writer.path_exists(ceap_run_metadata_path(pid))
    assert not raw_writer.path_exists(ceap_run_success_path(pid))


def test_is_ceap_run_manifest_valid_for_bronze_completed(raw_writer) -> None:
    pid = "ceap_daily_20260510"
    meta = _build_completed_meta(pid)
    persist_ceap_dispatcher_run_metadata(
        raw_writer, meta, write_success_marker_now=True
    )
    ok, _ = is_ceap_run_manifest_valid_for_bronze(raw_writer, pid)
    assert ok is True


def test_is_ceap_run_manifest_valid_for_bronze_rejects_running(raw_writer) -> None:
    pid = "ceap_daily_20260510"
    meta = _build_running_meta(pid)
    persist_ceap_dispatcher_run_metadata(
        raw_writer, meta, write_success_marker_now=False
    )
    ok, _ = is_ceap_run_manifest_valid_for_bronze(raw_writer, pid)
    assert ok is False


def test_months_to_process_serialised_as_list_not_string() -> None:
    meta = build_ceap_dispatcher_run_metadata(
        pipeline_run_id="ceap_daily_20260510",
        mode="daily",
        status="RUNNING",
        started_at_utc="2026-05-10T00:00:00+00:00",
        finished_at_utc=None,
        failed_at_utc=None,
        total_tasks_expected=1026,
        total_tasks_queued=0,
        total_tasks_pending=0,
        target_year=2026,
        months_to_process_json="[5, 4]",
        enqueue_phase_complete=False,
    )
    assert meta["months_to_process"] == [5, 4]
    assert meta["reference_months"] == [5, 4]


def test_ambiguous_zero_fields_are_dropped_from_ceap_manifest() -> None:
    meta = _build_completed_meta()
    for key in ("total_pages", "items_per_page", "files_written", "record_count"):
        assert key not in meta, f"{key} must not appear in CEAP run manifest"


def test_deputies_snapshot_pipeline_run_id_is_surfaced() -> None:
    meta = build_ceap_dispatcher_run_metadata(
        pipeline_run_id="ceap_daily_20260510",
        mode="daily",
        status="RUNNING",
        started_at_utc="2026-05-10T00:00:00+00:00",
        finished_at_utc=None,
        failed_at_utc=None,
        total_tasks_expected=0,
        total_tasks_queued=0,
        total_tasks_pending=0,
        target_year=2026,
        months_to_process_json="[5]",
        enqueue_phase_complete=False,
        deputies_snapshot_date="2026-05-10",
        deputies_snapshot_path=(
            "raw/camara/deputados/api/list/reference_date=2026-05-10/"
            "pipeline_run_id=ceap_daily_20260510"
        ),
        deputies_snapshot_pipeline_run_id="ceap_daily_20260510",
        deputies_snapshot_status="COMPLETED",
    )
    assert meta["deputies_snapshot_pipeline_run_id"] == "ceap_daily_20260510"
    assert meta["parent_entity"] == "deputados_list"
    assert meta["parent_pipeline_run_id"] == "ceap_daily_20260510"
    assert meta["parent_snapshot_path"].endswith(
        "/pipeline_run_id=ceap_daily_20260510"
    )


def test_optional_aggregates_only_emitted_when_provided() -> None:
    base = _build_running_meta()
    for key in (
        "max_tasks_per_dispatch",
        "total_raw_files_written",
        "total_records_collected",
    ):
        assert key not in base, f"{key} must be opt-in"

    enriched = build_ceap_dispatcher_run_metadata(
        pipeline_run_id="ceap_daily_20260510",
        mode="daily",
        status="RUNNING",
        started_at_utc="2026-05-10T00:00:00+00:00",
        finished_at_utc=None,
        failed_at_utc=None,
        total_tasks_expected=1026,
        total_tasks_queued=26,
        total_tasks_pending=0,
        target_year=2026,
        months_to_process_json="[5, 4]",
        enqueue_phase_complete=False,
        max_tasks_per_dispatch=1000,
        total_raw_files_written=42,
        total_records_collected=12345,
    )
    assert enriched["max_tasks_per_dispatch"] == 1000
    assert enriched["total_raw_files_written"] == 42
    assert enriched["total_records_collected"] == 12345
