"""Tests for ``shared.metadata`` v1.0 contract used by every Raw run."""

from __future__ import annotations

import pytest

from shared.metadata import (
    METADATA_VERSION,
    PROFILE_DIMENSION_SNAPSHOT,
    PROFILE_FANOUT_RUN,
    build_run_metadata,
    persist_run_manifest,
    validate_completed_metadata,
    validate_metadata_content,
    write_run_metadata,
    write_success_marker,
)


def _base_kwargs(**overrides):
    base = dict(
        source="camara_dadosabertos",
        domain="ceap",
        entity="deputado_despesas",
        endpoint="deputado_despesas",
        api_base_url="https://dadosabertos.camara.leg.br/api/v2",
        api_path="/deputados/{id}/despesas",
        pipeline_run_id="ceap_daily_20260510",
        run_type="daily",
        status="RUNNING",
        started_at="2026-05-10T00:00:00+00:00",
        raw_path="raw/camara/ceap/api/despesas/",
        success_marker_path="raw/camara/ceap/api/despesas/.../_SUCCESS",
    )
    base.update(overrides)
    return base


def test_build_run_metadata_includes_mandatory_fields() -> None:
    meta = build_run_metadata(**_base_kwargs())
    assert meta["metadata_version"] == METADATA_VERSION
    for key in (
        "source",
        "domain",
        "entity",
        "endpoint",
        "api_base_url",
        "api_path",
        "pipeline_run_id",
        "run_type",
        "status",
        "started_at",
        "created_at",
        "raw_path",
        "success_marker_path",
        "total_pages",
        "items_per_page",
        "files_written",
        "record_count",
    ):
        assert key in meta, f"missing mandatory key: {key}"


def test_build_run_metadata_filters_partitioning_block() -> None:
    meta = build_run_metadata(
        **_base_kwargs(
            partitioning={
                "reference_date": "2026-05-10",
                "reference_timezone": "America/Sao_Paulo",
                "unknown_field": "ignored",
            }
        )
    )
    assert meta["reference_date"] == "2026-05-10"
    assert meta["reference_timezone"] == "America/Sao_Paulo"
    assert "unknown_field" not in meta


def test_build_run_metadata_includes_dependencies_when_provided() -> None:
    deps = [
        {
            "entity": "deputado",
            "pipeline_run_id": "ceap_daily_20260510",
            "path": "raw/camara/deputados/api/list/reference_date=2026-05-10/",
            "status": "COMPLETED",
            "record_count": 513,
        }
    ]
    meta = build_run_metadata(**_base_kwargs(dependencies=deps))
    assert meta["dependencies"] == deps


def test_persist_run_manifest_writes_only_metadata_when_not_completed(raw_writer) -> None:
    meta = build_run_metadata(**_base_kwargs(status="RUNNING"))
    metadata_path = "raw/x/_metadata/runs/pipeline_run_id=r/metadata.json"
    success_path = "raw/x/_metadata/runs/pipeline_run_id=r/_SUCCESS"

    mp, success_written = persist_run_manifest(
        raw_writer,
        metadata_path=metadata_path,
        success_path=success_path,
        metadata=meta,
    )
    assert mp == metadata_path
    assert success_written is False
    assert raw_writer.path_exists(metadata_path)
    assert not raw_writer.path_exists(success_path)


def test_persist_run_manifest_writes_success_only_for_completed(raw_writer) -> None:
    meta = build_run_metadata(
        **_base_kwargs(
            status="COMPLETED",
            completed_at="2026-05-10T01:00:00+00:00",
            total_pages=1,
            files_written=1,
            record_count=10,
        )
    )
    metadata_path = "raw/x/_metadata/runs/pipeline_run_id=r/metadata.json"
    success_path = "raw/x/_metadata/runs/pipeline_run_id=r/_SUCCESS"
    mp, success_written = persist_run_manifest(
        raw_writer,
        metadata_path=metadata_path,
        success_path=success_path,
        metadata=meta,
    )
    assert mp == metadata_path
    assert success_written is True
    assert raw_writer.path_exists(success_path)


def test_write_success_marker_no_op_when_not_completed(raw_writer) -> None:
    meta = build_run_metadata(**_base_kwargs(status="FAILED"))
    written = write_success_marker(raw_writer, "raw/x/_SUCCESS", meta)
    assert written is False
    assert not raw_writer.path_exists("raw/x/_SUCCESS")


def test_write_success_marker_writes_when_completed(raw_writer) -> None:
    meta = build_run_metadata(
        **_base_kwargs(
            status="COMPLETED",
            completed_at="2026-05-10T01:00:00+00:00",
            total_pages=1,
            files_written=1,
            record_count=10,
        )
    )
    written = write_success_marker(raw_writer, "raw/x/_SUCCESS", meta)
    assert written is True
    assert raw_writer.path_exists("raw/x/_SUCCESS")


@pytest.mark.parametrize("bad_status", ["RUNNING", "FAILED", "PARTIALLY_COMPLETED"])
def test_validate_metadata_content_rejects_non_completed(bad_status) -> None:
    meta = build_run_metadata(**_base_kwargs(status=bad_status))
    ok, reason = validate_metadata_content(
        meta, profile=PROFILE_DIMENSION_SNAPSHOT
    )
    assert ok is False
    assert reason


def test_validate_metadata_content_dimension_profile_requires_counts() -> None:
    meta_missing_records = build_run_metadata(
        **_base_kwargs(
            status="COMPLETED",
            completed_at="2026-05-10T01:00:00+00:00",
            total_pages=2,
            files_written=2,
            record_count=0,
        )
    )
    ok, reason = validate_metadata_content(
        meta_missing_records, profile=PROFILE_DIMENSION_SNAPSHOT
    )
    assert ok is False
    assert "record_count" in reason


def test_validate_metadata_content_fanout_profile_requires_balanced_tasks() -> None:
    meta = build_run_metadata(
        **_base_kwargs(
            status="COMPLETED",
            completed_at="2026-05-10T01:00:00+00:00",
            tasks={
                "total_tasks_expected": 10,
                "total_tasks_success": 9,
                "total_tasks_failed": 1,
                "total_tasks_pending": 0,
                "total_tasks_poison": 0,
                "total_tasks_running": 0,
                "enqueue_phase_complete": True,
            },
        )
    )
    ok, reason = validate_metadata_content(meta, profile=PROFILE_FANOUT_RUN)
    assert ok is False
    assert reason in ("success!=expected", "total_tasks_failed!=0")


def test_validate_completed_metadata_requires_success_marker(raw_writer) -> None:
    meta = build_run_metadata(
        **_base_kwargs(
            status="COMPLETED",
            completed_at="2026-05-10T01:00:00+00:00",
            tasks={
                "total_tasks_expected": 1,
                "total_tasks_success": 1,
                "enqueue_phase_complete": True,
            },
        )
    )
    success_path = "raw/x/_SUCCESS"
    ok, reason = validate_completed_metadata(
        raw_writer, meta, success_path, profile=PROFILE_FANOUT_RUN
    )
    assert ok is False
    assert reason == "success_marker_missing"

    write_run_metadata(raw_writer, "raw/x/metadata.json", meta)
    raw_writer.write_text(success_path, "")
    ok2, _ = validate_completed_metadata(
        raw_writer, meta, success_path, profile=PROFILE_FANOUT_RUN
    )
    assert ok2 is True
