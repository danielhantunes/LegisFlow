"""metadata.json + _SUCCESS for votações ingestion (microbatch + reconciliation).

Two manifest layers are written under Raw:

* **Per-run aggregate** (dispatcher-owned) for the whole window:

  ``raw/camara/votacoes/api/_metadata/runs/pipeline_run_id={pid}/metadata.json``

  Holds aggregate counters (votações detected, fanout tasks, etc.). Mirrors
  the CEAP fanout contract.

* **Per-votação detail** (worker-owned), one per (votacao_id, pipeline_run_id):

  ``raw/camara/votacoes/api/votos/votacao_id={vid}/pipeline_run_id={pid}/_metadata/runs/metadata.json``

  Holds page counts, record counts and the ``_SUCCESS`` marker for that
  individual votação's votes snapshot.
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


VOTACOES_BASE_PREFIX = "raw/camara/votacoes/api"
VOTACOES_LIST_PREFIX = f"{VOTACOES_BASE_PREFIX}/list"
VOTACOES_VOTOS_PREFIX = f"{VOTACOES_BASE_PREFIX}/votos"


# --- Aggregate run manifest (dispatcher) -------------------------------------


def votacoes_run_manifest_prefix(pipeline_run_id: str) -> str:
    return f"{VOTACOES_BASE_PREFIX}/_metadata/runs/pipeline_run_id={pipeline_run_id}"


def votacoes_run_metadata_path(pipeline_run_id: str) -> str:
    return f"{votacoes_run_manifest_prefix(pipeline_run_id)}/metadata.json"


def votacoes_run_success_path(pipeline_run_id: str) -> str:
    return f"{votacoes_run_manifest_prefix(pipeline_run_id)}/_SUCCESS"


def build_votacoes_dispatcher_run_metadata(
    *,
    pipeline_run_id: str,
    mode: str,
    status: str,
    started_at_utc: str,
    finished_at_utc: str | None,
    failed_at_utc: str | None,
    window_start_utc: str | None,
    window_end_utc: str | None,
    total_votacoes_detected: int,
    total_tasks_expected: int,
    total_tasks_queued: int,
    total_tasks_pending: int,
    total_tasks_success: int,
    total_tasks_failed: int,
    total_tasks_poison: int,
    total_tasks_running: int,
    enqueue_phase_complete: bool,
    error_type: str | None = None,
    error_message: str | None = None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    source_system: str = DEFAULT_SOURCE_SYSTEM,
    hash_strategy: str = DEFAULT_HASH_STRATEGY,
    audit_fields_applied: tuple[str, ...] = DEFAULT_AUDIT_FIELDS,
    total_raw_files_written: int | None = None,
    total_records_collected: int | None = None,
) -> dict[str, Any]:
    """Aggregate manifest for a votacoes run (similar to CEAP fanout shape)."""
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
    raw_dir = VOTACOES_BASE_PREFIX
    success_path = votacoes_run_success_path(pipeline_run_id)
    # Normalise mode → valid RunType literal expected by build_run_metadata.
    if mode == "reconciliation":
        run_type_norm = "reconciliation"
    elif mode == "backfill":
        run_type_norm = "backfill"
    else:
        # microbatch / daily ticks fall under "daily" semantics.
        run_type_norm = "daily"
    meta = build_run_metadata(
        source=source_system,
        domain="votacoes",
        entity="votacoes",
        endpoint="votacoes_fanout",
        api_base_url=api_base_url,
        api_path="/votacoes + /votacoes/{id}/votos",
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
            "watermark_field": "dataHoraRegistro",
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
            "fanout_from": "/votacoes",
            "parent_entity": "votacao",
            "parent_id_field": "id",
            "parent_record_count": int(total_votacoes_detected or 0),
        },
        hash_strategy=hash_strategy,
        audit_fields_applied=audit_fields_applied,
    )
    # CEAP-style cleanup: drop snapshot-style ambiguous zeros.
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
    meta["total_votacoes_detected"] = int(total_votacoes_detected or 0)
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
    return meta


def persist_votacoes_run_metadata(
    adls: AdlsRawWriter,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = votacoes_run_metadata_path(pipeline_run_id)
    success_path = votacoes_run_success_path(pipeline_run_id)
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_votacoes_run_manifest_valid(
    adls: AdlsRawWriter, pipeline_run_id: str
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(votacoes_run_metadata_path(pipeline_run_id)) or {}
    success_path = votacoes_run_success_path(pipeline_run_id)
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_FANOUT_RUN
    )
    return ok, meta


# --- Per-votação detail manifest (worker) ------------------------------------


def votacao_votos_data_dir(votacao_id: str, pipeline_run_id: str) -> str:
    return (
        f"{VOTACOES_VOTOS_PREFIX}/votacao_id={votacao_id}/"
        f"pipeline_run_id={pipeline_run_id}"
    )


def votacao_votos_run_manifest_prefix(votacao_id: str, pipeline_run_id: str) -> str:
    return (
        f"{VOTACOES_VOTOS_PREFIX}/votacao_id={votacao_id}/"
        f"_metadata/runs/pipeline_run_id={pipeline_run_id}"
    )


def votacao_votos_metadata_path(votacao_id: str, pipeline_run_id: str) -> str:
    return f"{votacao_votos_run_manifest_prefix(votacao_id, pipeline_run_id)}/metadata.json"


def votacao_votos_success_path(votacao_id: str, pipeline_run_id: str) -> str:
    return f"{votacao_votos_run_manifest_prefix(votacao_id, pipeline_run_id)}/_SUCCESS"


def build_votacao_votos_metadata(
    *,
    endpoint: EndpointSpec,
    pipeline_run_id: str,
    execution_id: str,
    votacao_id: str,
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
    raw_dir = votacao_votos_data_dir(votacao_id, pipeline_run_id)
    success_path = votacao_votos_success_path(votacao_id, pipeline_run_id)
    meta = build_run_metadata(
        source=source_system,
        domain="votacoes",
        entity=endpoint.name,
        endpoint=endpoint.name,
        api_base_url=api_base_url,
        api_path=endpoint.path_template.format(id=votacao_id),
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
            "votacao_id": votacao_id,
        },
        snapshot={
            "snapshot_type": "votacao_votos",
            "snapshot_id": votacao_id,
            "snapshot_status": normalized,
            "snapshot_record_count": int(record_count or 0),
        },
        hash_strategy=hash_strategy,
        audit_fields_applied=audit_fields_applied,
    )
    meta["pipeline_run_id"] = pipeline_run_id
    meta["status"] = st_upper
    meta["votacao_id"] = votacao_id
    meta["started_at_utc"] = started_at_utc
    meta["finished_at_utc"] = completed_at_utc
    meta["failed_at_utc"] = failed_at_utc
    if error_type:
        meta["error_type"] = error_type
    elif "error_type" in meta:
        del meta["error_type"]
    return meta


def persist_votacao_votos_metadata(
    adls: AdlsRawWriter,
    votacao_id: str,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = votacao_votos_metadata_path(votacao_id, pipeline_run_id)
    success_path = votacao_votos_success_path(votacao_id, pipeline_run_id)
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_votacao_votos_manifest_valid(
    adls: AdlsRawWriter, votacao_id: str, pipeline_run_id: str
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(votacao_votos_metadata_path(votacao_id, pipeline_run_id)) or {}
    success_path = votacao_votos_success_path(votacao_id, pipeline_run_id)
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_DIMENSION_SNAPSHOT
    )
    return ok, meta


def now_iso_utc() -> str:
    return datetime.now(UTC).isoformat()
