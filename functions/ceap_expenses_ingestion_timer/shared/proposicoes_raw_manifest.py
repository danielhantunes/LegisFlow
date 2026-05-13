"""metadata.json + _SUCCESS for proposições ingestion (microbatch + fanout).

Two manifest layers under Raw:

* **Per-run aggregate** (dispatcher-owned):

  ``raw/camara/proposicoes/api/_metadata/runs/pipeline_run_id={pid}/metadata.json``

  Holds aggregate counters across the whole window (proposicoes detected,
  tasks fanned out, etc.).

* **Per-proposicao detail** (worker-owned), one per
  (proposicao_id, sub_endpoint, pipeline_run_id):

  ``raw/camara/proposicoes/api/{autores|tramitacoes}/proposicao_id={pid_p}/``
  ``pipeline_run_id={pid}/_metadata/runs/metadata.json``
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
    RunType,
    build_run_metadata,
    validate_completed_metadata,
    write_run_metadata,
    write_success_marker,
)

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter


PROPOSICOES_BASE_PREFIX = "raw/camara/proposicoes/api"
PROPOSICOES_LIST_PREFIX = f"{PROPOSICOES_BASE_PREFIX}/list"
PROPOSICOES_AUTORES_PREFIX = f"{PROPOSICOES_BASE_PREFIX}/autores"
PROPOSICOES_TRAMITACOES_PREFIX = f"{PROPOSICOES_BASE_PREFIX}/tramitacoes"


def _sub_prefix_for_endpoint(endpoint_name: str) -> str:
    """Resolve the on-disk prefix for a worker endpoint."""
    if endpoint_name == "proposicao_autores":
        return PROPOSICOES_AUTORES_PREFIX
    if endpoint_name == "proposicao_tramitacoes":
        return PROPOSICOES_TRAMITACOES_PREFIX
    raise KeyError(f"Unknown proposicoes worker endpoint: {endpoint_name!r}")


# --- Aggregate run manifest (dispatcher) -------------------------------------


def proposicoes_run_manifest_prefix(pipeline_run_id: str) -> str:
    return (
        f"{PROPOSICOES_BASE_PREFIX}/_metadata/runs/pipeline_run_id={pipeline_run_id}"
    )


def proposicoes_run_metadata_path(pipeline_run_id: str) -> str:
    return f"{proposicoes_run_manifest_prefix(pipeline_run_id)}/metadata.json"


def proposicoes_run_success_path(pipeline_run_id: str) -> str:
    return f"{proposicoes_run_manifest_prefix(pipeline_run_id)}/_SUCCESS"


def build_proposicoes_dispatcher_run_metadata(
    *,
    pipeline_run_id: str,
    mode: str,
    status: str,
    started_at_utc: str,
    finished_at_utc: str | None,
    failed_at_utc: str | None,
    window_start_utc: str | None,
    window_end_utc: str | None,
    total_proposicoes_detected: int,
    total_tasks_expected: int,
    total_tasks_queued: int,
    total_tasks_pending: int,
    total_tasks_success: int,
    total_tasks_failed: int,
    total_tasks_poison: int,
    total_tasks_running: int,
    enqueue_phase_complete: bool,
    sub_endpoints: tuple[str, ...] = ("proposicao_autores", "proposicao_tramitacoes"),
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
    raw_dir = PROPOSICOES_BASE_PREFIX
    success_path = proposicoes_run_success_path(pipeline_run_id)
    if mode == "reconciliation":
        run_type_norm = "reconciliation"
    elif mode == "backfill":
        run_type_norm = "backfill"
    elif mode == "microbatch":
        run_type_norm = "microbatch"
    else:
        run_type_norm = "daily"
    part_block: dict[str, Any] = {
        "watermark_field": "ultimaTramitacao_dataHora",
        "watermark_start": window_start_utc or "",
        "watermark_end": window_end_utc or "",
    }
    if manifest_extras:
        ds = manifest_extras.get("date_start")
        de = manifest_extras.get("date_end")
        if ds:
            part_block["date_start"] = str(ds)
        if de:
            part_block["date_end"] = str(de)
    meta = build_run_metadata(
        source=source_system,
        domain="proposicoes",
        entity="proposicoes",
        endpoint="proposicoes_fanout",
        api_base_url=api_base_url,
        api_path="/proposicoes + /proposicoes/{id}/{autores|tramitacoes}",
        pipeline_run_id=pipeline_run_id,
        execution_id=pipeline_run_id,
        run_type=cast(RunType, run_type_norm),
        status=normalized,
        started_at=started_at_utc,
        completed_at=finished_at_utc,
        raw_path=raw_dir,
        success_marker_path=success_path,
        error_message=error_message,
        partitioning=part_block,
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
            "fanout_from": "/proposicoes",
            "parent_entity": "proposicao",
            "parent_id_field": "id",
            "parent_record_count": int(total_proposicoes_detected or 0),
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
    meta["total_proposicoes_detected"] = int(total_proposicoes_detected or 0)
    meta["sub_endpoints"] = list(sub_endpoints)
    meta["total_tasks_expected"] = int(total_tasks_expected or 0)
    meta["total_tasks_queued"] = int(total_tasks_queued or 0)
    meta["total_tasks_pending"] = int(total_tasks_pending or 0)
    meta["total_tasks_success"] = int(total_tasks_success or 0)
    meta["total_tasks_failed"] = int(total_tasks_failed or 0)
    meta["total_tasks_poison"] = int(total_tasks_poison or 0)
    meta["total_tasks_running"] = int(total_tasks_running or 0)
    meta["enqueue_phase_complete"] = bool(enqueue_phase_complete)
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


def persist_proposicoes_run_metadata(
    adls: AdlsRawWriter,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = proposicoes_run_metadata_path(pipeline_run_id)
    success_path = proposicoes_run_success_path(pipeline_run_id)
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_proposicoes_run_manifest_valid(
    adls: AdlsRawWriter, pipeline_run_id: str
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(proposicoes_run_metadata_path(pipeline_run_id)) or {}
    success_path = proposicoes_run_success_path(pipeline_run_id)
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_FANOUT_RUN
    )
    return ok, meta


# --- Per-(proposicao, sub-endpoint) detail manifest --------------------------


def proposicao_sub_data_dir(
    endpoint_name: str, proposicao_id: str, pipeline_run_id: str
) -> str:
    base = _sub_prefix_for_endpoint(endpoint_name)
    return f"{base}/proposicao_id={proposicao_id}/pipeline_run_id={pipeline_run_id}"


def proposicao_sub_manifest_prefix(
    endpoint_name: str, proposicao_id: str, pipeline_run_id: str
) -> str:
    base = _sub_prefix_for_endpoint(endpoint_name)
    return (
        f"{base}/proposicao_id={proposicao_id}/"
        f"_metadata/runs/pipeline_run_id={pipeline_run_id}"
    )


def proposicao_sub_metadata_path(
    endpoint_name: str, proposicao_id: str, pipeline_run_id: str
) -> str:
    return (
        f"{proposicao_sub_manifest_prefix(endpoint_name, proposicao_id, pipeline_run_id)}"
        "/metadata.json"
    )


def proposicao_sub_success_path(
    endpoint_name: str, proposicao_id: str, pipeline_run_id: str
) -> str:
    return (
        f"{proposicao_sub_manifest_prefix(endpoint_name, proposicao_id, pipeline_run_id)}"
        "/_SUCCESS"
    )


def build_proposicao_sub_metadata(
    *,
    endpoint: EndpointSpec,
    pipeline_run_id: str,
    execution_id: str,
    proposicao_id: str,
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
    raw_dir = proposicao_sub_data_dir(endpoint.name, proposicao_id, pipeline_run_id)
    success_path = proposicao_sub_success_path(
        endpoint.name, proposicao_id, pipeline_run_id
    )
    meta = build_run_metadata(
        source=source_system,
        domain="proposicoes",
        entity=endpoint.name,
        endpoint=endpoint.name,
        api_base_url=api_base_url,
        api_path=endpoint.path_template.format(id=proposicao_id),
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
            "proposicao_id": proposicao_id,
        },
        snapshot={
            "snapshot_type": "proposicao_sub",
            "snapshot_id": proposicao_id,
            "snapshot_status": normalized,
            "snapshot_record_count": int(record_count or 0),
        },
        hash_strategy=hash_strategy,
        audit_fields_applied=audit_fields_applied,
    )
    meta["pipeline_run_id"] = pipeline_run_id
    meta["status"] = st_upper
    meta["proposicao_id"] = proposicao_id
    meta["sub_endpoint"] = endpoint.name
    meta["started_at_utc"] = started_at_utc
    meta["finished_at_utc"] = completed_at_utc
    meta["failed_at_utc"] = failed_at_utc
    if error_type:
        meta["error_type"] = error_type
    elif "error_type" in meta:
        del meta["error_type"]
    return meta


def persist_proposicao_sub_metadata(
    adls: AdlsRawWriter,
    endpoint_name: str,
    proposicao_id: str,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = proposicao_sub_metadata_path(
        endpoint_name, proposicao_id, pipeline_run_id
    )
    success_path = proposicao_sub_success_path(
        endpoint_name, proposicao_id, pipeline_run_id
    )
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_proposicao_sub_manifest_valid(
    adls: AdlsRawWriter,
    endpoint_name: str,
    proposicao_id: str,
    pipeline_run_id: str,
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(
        proposicao_sub_metadata_path(endpoint_name, proposicao_id, pipeline_run_id)
    ) or {}
    success_path = proposicao_sub_success_path(
        endpoint_name, proposicao_id, pipeline_run_id
    )
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_DIMENSION_SNAPSHOT
    )
    return ok, meta


def now_iso_utc() -> str:
    return datetime.now(UTC).isoformat()
