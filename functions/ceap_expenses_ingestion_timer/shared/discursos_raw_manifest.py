"""metadata.json + _SUCCESS for the discursos domain (microbatch fanout).

Two manifest layers:

* Aggregate (dispatcher-owned):

  ``raw/camara/discursos/api/_metadata/runs/pipeline_run_id={pid}/metadata.json``

* Per-deputado detail (worker-owned), one per (deputado_id, pipeline_run_id):

  ``raw/camara/discursos/api/discursos/deputado_id={did}/pipeline_run_id={pid}/``
  ``_metadata/runs/metadata.json``
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from .domain_catalog import (
    DEFAULT_API_BASE_URL,
    DEFAULT_AUDIT_FIELDS,
    DEFAULT_HASH_STRATEGY,
    DEFAULT_SOURCE_SYSTEM,
    EndpointSpec,
)
from .metadata import (
    PROFILE_DIMENSION_SNAPSHOT,
    PROFILE_FANOUT_RUN,
    RunStatus,
    build_run_metadata,
    validate_completed_metadata,
    write_run_metadata,
    write_success_marker,
)

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter


DISCURSOS_BASE_PREFIX = "raw/camara/discursos/api"
DISCURSOS_DETAIL_PREFIX = f"{DISCURSOS_BASE_PREFIX}/discursos"


# --- Aggregate run manifest --------------------------------------------------


def discursos_run_manifest_prefix(pipeline_run_id: str) -> str:
    return (
        f"{DISCURSOS_BASE_PREFIX}/_metadata/runs/"
        f"pipeline_run_id={pipeline_run_id}"
    )


def discursos_run_metadata_path(pipeline_run_id: str) -> str:
    return f"{discursos_run_manifest_prefix(pipeline_run_id)}/metadata.json"


def discursos_run_success_path(pipeline_run_id: str) -> str:
    return f"{discursos_run_manifest_prefix(pipeline_run_id)}/_SUCCESS"


def build_discursos_dispatcher_run_metadata(
    *,
    pipeline_run_id: str,
    mode: str,
    status: str,
    started_at_utc: str,
    finished_at_utc: str | None,
    failed_at_utc: str | None,
    window_start_utc: str | None,
    window_end_utc: str | None,
    total_deputados_detected: int,
    total_tasks_expected: int,
    total_tasks_queued: int,
    total_tasks_pending: int,
    total_tasks_success: int,
    total_tasks_failed: int,
    total_tasks_poison: int,
    total_tasks_running: int,
    enqueue_phase_complete: bool,
    deputies_snapshot_path: str | None = None,
    deputies_snapshot_pipeline_run_id: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    source_system: str = DEFAULT_SOURCE_SYSTEM,
    hash_strategy: str = DEFAULT_HASH_STRATEGY,
    audit_fields_applied: tuple[str, ...] = DEFAULT_AUDIT_FIELDS,
    total_raw_files_written: int | None = None,
    total_records_collected: int | None = None,
    manifest_extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    st_upper = str(status).upper()
    normalized = cast(
        RunStatus,
        st_upper
        if st_upper
        in {
            "STARTED",
            "RUNNING",
            "COMPLETED",
            "PARTIAL",
            "FAILED",
            "PARTIALLY_COMPLETED",
        }
        else "RUNNING",
    )
    if mode == "reconciliation":
        run_type_norm = "reconciliation"
    elif mode == "backfill":
        run_type_norm = "backfill"
    else:
        run_type_norm = "daily"
    raw_dir = DISCURSOS_BASE_PREFIX
    success_path = discursos_run_success_path(pipeline_run_id)
    meta = build_run_metadata(
        source=source_system,
        domain="discursos",
        entity="deputado_discursos",
        endpoint="deputado_discursos",
        api_base_url=api_base_url,
        api_path="/deputados/{id}/discursos",
        pipeline_run_id=pipeline_run_id,
        execution_id=pipeline_run_id,
        run_type=run_type_norm,
        status=normalized,
        started_at=started_at_utc,
        completed_at=finished_at_utc,
        raw_path=raw_dir,
        success_marker_path=success_path,
        error_message=error_message,
        partitioning={
            "watermark_field": "dataHoraInicio",
            "watermark_start": window_start_utc or "",
            "watermark_end": window_end_utc or "",
        },
        tasks={
            "total_tasks_expected": int(total_tasks_expected or 0),
            "total_tasks_queued": int(total_tasks_queued or 0),
            "total_tasks_pending": int(total_tasks_pending or 0),
            "total_tasks_success": int(total_tasks_success or 0),
            "total_tasks_failed": int(total_tasks_failed or 0),
            "total_tasks_poison": int(total_tasks_poison or 0),
            "total_tasks_running": int(total_tasks_running or 0),
            "enqueue_phase_complete": bool(enqueue_phase_complete),
        },
        fanout={
            "fanout_from": "deputies_snapshot",
            "parent_entity": "deputado",
            "parent_id_field": "id",
            "parent_record_count": int(total_deputados_detected or 0),
            "parent_pipeline_run_id": deputies_snapshot_pipeline_run_id,
        },
        hash_strategy=hash_strategy,
        audit_fields_applied=audit_fields_applied,
    )
    for k in ("total_pages", "items_per_page", "files_written", "record_count"):
        meta.pop(k, None)

    meta["pipeline_run_id"] = pipeline_run_id
    meta["status"] = st_upper
    meta["mode"] = mode
    meta["started_at_utc"] = started_at_utc
    meta["finished_at_utc"] = finished_at_utc
    meta["failed_at_utc"] = failed_at_utc
    meta["window_start_utc"] = window_start_utc
    meta["window_end_utc"] = window_end_utc
    meta["total_deputados_detected"] = int(total_deputados_detected or 0)
    meta["total_tasks_expected"] = int(total_tasks_expected or 0)
    meta["total_tasks_queued"] = int(total_tasks_queued or 0)
    meta["total_tasks_pending"] = int(total_tasks_pending or 0)
    meta["total_tasks_success"] = int(total_tasks_success or 0)
    meta["total_tasks_failed"] = int(total_tasks_failed or 0)
    meta["total_tasks_poison"] = int(total_tasks_poison or 0)
    meta["total_tasks_running"] = int(total_tasks_running or 0)
    meta["enqueue_phase_complete"] = bool(enqueue_phase_complete)
    if deputies_snapshot_path is not None:
        meta["deputies_snapshot_path"] = deputies_snapshot_path
    if deputies_snapshot_pipeline_run_id is not None:
        meta["deputies_snapshot_pipeline_run_id"] = deputies_snapshot_pipeline_run_id
    if total_raw_files_written is not None:
        meta["total_raw_files_written"] = int(total_raw_files_written)
    if total_records_collected is not None:
        meta["total_records_collected"] = int(total_records_collected)
    if error_type:
        meta["error_type"] = error_type
    elif "error_type" in meta:
        del meta["error_type"]
    if manifest_extras:
        for k, v in manifest_extras.items():
            if v is not None:
                meta[k] = v
    return meta


def persist_discursos_run_metadata(
    adls: AdlsRawWriter,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = discursos_run_metadata_path(pipeline_run_id)
    success_path = discursos_run_success_path(pipeline_run_id)
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_discursos_run_manifest_valid(
    adls: AdlsRawWriter, pipeline_run_id: str
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(discursos_run_metadata_path(pipeline_run_id)) or {}
    success_path = discursos_run_success_path(pipeline_run_id)
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_FANOUT_RUN
    )
    return ok, meta


# --- Per-deputado detail manifest --------------------------------------------


def discursos_detail_data_dir(deputado_id: str, pipeline_run_id: str) -> str:
    return (
        f"{DISCURSOS_DETAIL_PREFIX}/deputado_id={deputado_id}/"
        f"pipeline_run_id={pipeline_run_id}"
    )


def discursos_detail_manifest_prefix(
    deputado_id: str, pipeline_run_id: str
) -> str:
    return (
        f"{DISCURSOS_DETAIL_PREFIX}/deputado_id={deputado_id}/"
        f"_metadata/runs/pipeline_run_id={pipeline_run_id}"
    )


def discursos_detail_metadata_path(
    deputado_id: str, pipeline_run_id: str
) -> str:
    return (
        f"{discursos_detail_manifest_prefix(deputado_id, pipeline_run_id)}"
        "/metadata.json"
    )


def discursos_detail_success_path(
    deputado_id: str, pipeline_run_id: str
) -> str:
    return (
        f"{discursos_detail_manifest_prefix(deputado_id, pipeline_run_id)}"
        "/_SUCCESS"
    )


def build_discursos_detail_metadata(
    *,
    endpoint: EndpointSpec,
    pipeline_run_id: str,
    execution_id: str,
    deputado_id: str,
    window_start_utc: str | None,
    window_end_utc: str | None,
    status: str,
    started_at_utc: str,
    completed_at_utc: str | None,
    failed_at_utc: str | None,
    total_pages: int,
    record_count: int,
    error_type: str | None = None,
    error_message: str | None = None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    source_system: str = DEFAULT_SOURCE_SYSTEM,
    hash_strategy: str = DEFAULT_HASH_STRATEGY,
    audit_fields_applied: tuple[str, ...] = DEFAULT_AUDIT_FIELDS,
) -> dict[str, Any]:
    st_upper = str(status).upper()
    normalized = cast(
        RunStatus,
        st_upper
        if st_upper
        in {
            "STARTED",
            "RUNNING",
            "COMPLETED",
            "PARTIAL",
            "FAILED",
            "PARTIALLY_COMPLETED",
        }
        else "RUNNING",
    )
    raw_dir = discursos_detail_data_dir(deputado_id, pipeline_run_id)
    success_path = discursos_detail_success_path(deputado_id, pipeline_run_id)
    meta = build_run_metadata(
        source=source_system,
        domain="discursos",
        entity=endpoint.name,
        endpoint=endpoint.name,
        api_base_url=api_base_url,
        api_path=endpoint.path_template.format(id=deputado_id),
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        run_type="snapshot",
        status=normalized,
        started_at=started_at_utc,
        completed_at=completed_at_utc,
        raw_path=raw_dir,
        success_marker_path=success_path,
        total_pages=int(total_pages or 0),
        items_per_page=int(endpoint.items_per_page or 0),
        files_written=int(total_pages or 0),
        record_count=int(record_count or 0),
        error_message=error_message,
        partitioning={
            "deputado_id": deputado_id,
            "watermark_field": "dataHoraInicio",
            "watermark_start": window_start_utc or "",
            "watermark_end": window_end_utc or "",
        },
        snapshot={
            "snapshot_type": "deputado_discursos",
            "snapshot_id": deputado_id,
            "snapshot_status": normalized,
            "snapshot_record_count": int(record_count or 0),
        },
        hash_strategy=hash_strategy,
        audit_fields_applied=audit_fields_applied,
    )
    meta["pipeline_run_id"] = pipeline_run_id
    meta["status"] = st_upper
    meta["deputado_id"] = deputado_id
    meta["window_start_utc"] = window_start_utc
    meta["window_end_utc"] = window_end_utc
    meta["started_at_utc"] = started_at_utc
    meta["finished_at_utc"] = completed_at_utc
    meta["failed_at_utc"] = failed_at_utc
    if error_type:
        meta["error_type"] = error_type
    elif "error_type" in meta:
        del meta["error_type"]
    return meta


def persist_discursos_detail_metadata(
    adls: AdlsRawWriter,
    deputado_id: str,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = discursos_detail_metadata_path(deputado_id, pipeline_run_id)
    success_path = discursos_detail_success_path(deputado_id, pipeline_run_id)
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_discursos_detail_manifest_valid(
    adls: AdlsRawWriter, deputado_id: str, pipeline_run_id: str
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(
        discursos_detail_metadata_path(deputado_id, pipeline_run_id)
    ) or {}
    success_path = discursos_detail_success_path(deputado_id, pipeline_run_id)
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_DIMENSION_SNAPSHOT
    )
    return ok, meta


def now_iso_utc() -> str:
    return datetime.now(UTC).isoformat()
