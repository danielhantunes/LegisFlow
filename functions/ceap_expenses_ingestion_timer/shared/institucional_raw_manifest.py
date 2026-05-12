"""metadata.json + _SUCCESS for the institucional domain (daily fanout).

Two manifest layers:

* Aggregate (dispatcher-owned):

  ``raw/camara/institucional/api/_metadata/runs/pipeline_run_id={pid}/metadata.json``

* Per-(parent, sub-endpoint) detail (worker-owned):

  ``raw/camara/institucional/api/{parent_kind}/{sub_kind}/parent_id={pid}/``
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
    build_run_metadata,
    validate_completed_metadata,
    write_run_metadata,
    write_success_marker,
)

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter


INSTITUCIONAL_BASE_PREFIX = "raw/camara/institucional/api"


# Mapping from worker endpoint name -> (parent_kind, sub_kind, parent_label)
_WORKER_ENDPOINT_LAYOUT: dict[str, tuple[str, str, str]] = {
    "orgao_membros":       ("orgaos",       "membros", "orgao"),
    "partido_membros":     ("partidos",     "membros", "partido"),
    "frente_membros":      ("frentes",      "membros", "frente"),
    "legislatura_lideres": ("legislaturas", "lideres", "legislatura"),
    "legislatura_mesa":    ("legislaturas", "mesa",    "legislatura"),
}

WORKER_ENDPOINTS: tuple[str, ...] = tuple(_WORKER_ENDPOINT_LAYOUT.keys())

PARENT_ENDPOINTS: tuple[str, ...] = (
    "orgaos_parent",
    "partidos_parent",
    "frentes_parent",
    "legislaturas_parent",
)

# Mapping from parent endpoint name -> (parent_label, on-disk parent_kind)
_PARENT_LAYOUT: dict[str, tuple[str, str]] = {
    "orgaos_parent":       ("orgao",       "orgaos"),
    "partidos_parent":     ("partido",     "partidos"),
    "frentes_parent":      ("frente",      "frentes"),
    "legislaturas_parent": ("legislatura", "legislaturas"),
}


def parent_endpoint_label(parent_endpoint_name: str) -> str:
    return _PARENT_LAYOUT[parent_endpoint_name][0]


def parent_endpoint_kind(parent_endpoint_name: str) -> str:
    return _PARENT_LAYOUT[parent_endpoint_name][1]


def parent_listing_dir(parent_endpoint_name: str, pipeline_run_id: str) -> str:
    """Where the dispatcher writes the parent listing pages."""
    kind = parent_endpoint_kind(parent_endpoint_name)
    return (
        f"{INSTITUCIONAL_BASE_PREFIX}/parents/{kind}/"
        f"pipeline_run_id={pipeline_run_id}"
    )


def worker_endpoint_layout(endpoint_name: str) -> tuple[str, str, str]:
    """Returns ``(parent_kind, sub_kind, parent_label)`` for a worker endpoint.

    ``parent_kind`` is the on-disk plural ("orgaos") used in the path layout;
    ``parent_label`` is the singular tag attached to ``_audit._parent_entity``.
    """
    if endpoint_name not in _WORKER_ENDPOINT_LAYOUT:
        raise KeyError(
            f"Unknown institucional worker endpoint: {endpoint_name!r}"
        )
    return _WORKER_ENDPOINT_LAYOUT[endpoint_name]


def parent_label_for_worker(endpoint_name: str) -> str:
    return worker_endpoint_layout(endpoint_name)[2]


# --- Aggregate run manifest --------------------------------------------------


def institucional_run_manifest_prefix(pipeline_run_id: str) -> str:
    return (
        f"{INSTITUCIONAL_BASE_PREFIX}/_metadata/runs/"
        f"pipeline_run_id={pipeline_run_id}"
    )


def institucional_run_metadata_path(pipeline_run_id: str) -> str:
    return f"{institucional_run_manifest_prefix(pipeline_run_id)}/metadata.json"


def institucional_run_success_path(pipeline_run_id: str) -> str:
    return f"{institucional_run_manifest_prefix(pipeline_run_id)}/_SUCCESS"


def build_institucional_dispatcher_run_metadata(
    *,
    pipeline_run_id: str,
    mode: str,
    status: str,
    started_at_utc: str,
    finished_at_utc: str | None,
    failed_at_utc: str | None,
    parents_detected: dict[str, int],
    total_tasks_expected: int,
    total_tasks_queued: int,
    total_tasks_pending: int,
    total_tasks_success: int,
    total_tasks_failed: int,
    total_tasks_poison: int,
    total_tasks_running: int,
    enqueue_phase_complete: bool,
    sub_endpoints: tuple[str, ...] = WORKER_ENDPOINTS,
    error_type: str | None = None,
    error_message: str | None = None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    source_system: str = DEFAULT_SOURCE_SYSTEM,
    hash_strategy: str = DEFAULT_HASH_STRATEGY,
    audit_fields_applied: tuple[str, ...] = DEFAULT_AUDIT_FIELDS,
    total_raw_files_written: int | None = None,
    total_records_collected: int | None = None,
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
    raw_dir = INSTITUCIONAL_BASE_PREFIX
    success_path = institucional_run_success_path(pipeline_run_id)
    parent_total = sum(int(v or 0) for v in parents_detected.values())
    meta = build_run_metadata(
        source=source_system,
        domain="institucional",
        entity="institucional",
        endpoint="institucional_fanout",
        api_base_url=api_base_url,
        api_path=(
            "/orgaos + /partidos + /frentes + /legislaturas + "
            "/{kind}/{id}/{membros|lideres|mesa}"
        ),
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
            "snapshot_kind": "institucional_daily",
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
            "fanout_from": "/orgaos + /partidos + /frentes + /legislaturas",
            "parent_entity": "institucional_parent_set",
            "parent_id_field": "id",
            "parent_record_count": parent_total,
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
    meta["parents_detected"] = {k: int(v or 0) for k, v in parents_detected.items()}
    meta["total_parents_detected"] = parent_total
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
    return meta


def persist_institucional_run_metadata(
    adls: AdlsRawWriter,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = institucional_run_metadata_path(pipeline_run_id)
    success_path = institucional_run_success_path(pipeline_run_id)
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_institucional_run_manifest_valid(
    adls: AdlsRawWriter, pipeline_run_id: str
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(institucional_run_metadata_path(pipeline_run_id)) or {}
    success_path = institucional_run_success_path(pipeline_run_id)
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_FANOUT_RUN
    )
    return ok, meta


# --- Per-(parent, sub-endpoint) detail manifest ------------------------------


def institucional_sub_data_dir(
    endpoint_name: str, parent_id: str, pipeline_run_id: str
) -> str:
    parent_kind, sub_kind, _label = worker_endpoint_layout(endpoint_name)
    return (
        f"{INSTITUCIONAL_BASE_PREFIX}/{parent_kind}/{sub_kind}/"
        f"parent_id={parent_id}/pipeline_run_id={pipeline_run_id}"
    )


def institucional_sub_manifest_prefix(
    endpoint_name: str, parent_id: str, pipeline_run_id: str
) -> str:
    parent_kind, sub_kind, _label = worker_endpoint_layout(endpoint_name)
    return (
        f"{INSTITUCIONAL_BASE_PREFIX}/{parent_kind}/{sub_kind}/"
        f"parent_id={parent_id}/_metadata/runs/pipeline_run_id={pipeline_run_id}"
    )


def institucional_sub_metadata_path(
    endpoint_name: str, parent_id: str, pipeline_run_id: str
) -> str:
    return (
        f"{institucional_sub_manifest_prefix(endpoint_name, parent_id, pipeline_run_id)}"
        "/metadata.json"
    )


def institucional_sub_success_path(
    endpoint_name: str, parent_id: str, pipeline_run_id: str
) -> str:
    return (
        f"{institucional_sub_manifest_prefix(endpoint_name, parent_id, pipeline_run_id)}"
        "/_SUCCESS"
    )


def build_institucional_sub_metadata(
    *,
    endpoint: EndpointSpec,
    pipeline_run_id: str,
    execution_id: str,
    parent_id: str,
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
    raw_dir = institucional_sub_data_dir(endpoint.name, parent_id, pipeline_run_id)
    success_path = institucional_sub_success_path(
        endpoint.name, parent_id, pipeline_run_id
    )
    meta = build_run_metadata(
        source=source_system,
        domain="institucional",
        entity=endpoint.name,
        endpoint=endpoint.name,
        api_base_url=api_base_url,
        api_path=endpoint.path_template.format(id=parent_id),
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
            "parent_id": parent_id,
            "parent_label": parent_label_for_worker(endpoint.name),
        },
        snapshot={
            "snapshot_type": "institucional_sub",
            "snapshot_id": parent_id,
            "snapshot_status": normalized,
            "snapshot_record_count": int(record_count or 0),
        },
        hash_strategy=hash_strategy,
        audit_fields_applied=audit_fields_applied,
    )
    meta["pipeline_run_id"] = pipeline_run_id
    meta["status"] = st_upper
    meta["parent_id"] = parent_id
    meta["parent_label"] = parent_label_for_worker(endpoint.name)
    meta["sub_endpoint"] = endpoint.name
    meta["started_at_utc"] = started_at_utc
    meta["finished_at_utc"] = completed_at_utc
    meta["failed_at_utc"] = failed_at_utc
    if error_type:
        meta["error_type"] = error_type
    elif "error_type" in meta:
        del meta["error_type"]
    return meta


def persist_institucional_sub_metadata(
    adls: AdlsRawWriter,
    endpoint_name: str,
    parent_id: str,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = institucional_sub_metadata_path(
        endpoint_name, parent_id, pipeline_run_id
    )
    success_path = institucional_sub_success_path(
        endpoint_name, parent_id, pipeline_run_id
    )
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_institucional_sub_manifest_valid(
    adls: AdlsRawWriter,
    endpoint_name: str,
    parent_id: str,
    pipeline_run_id: str,
) -> tuple[bool, dict[str, Any]]:
    meta = adls.read_json(
        institucional_sub_metadata_path(endpoint_name, parent_id, pipeline_run_id)
    ) or {}
    success_path = institucional_sub_success_path(
        endpoint_name, parent_id, pipeline_run_id
    )
    ok, _reason = validate_completed_metadata(
        adls, meta, success_path, profile=PROFILE_DIMENSION_SNAPSHOT
    )
    return ok, meta


def now_iso_utc() -> str:
    return datetime.now(UTC).isoformat()
