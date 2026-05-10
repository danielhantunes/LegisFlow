"""CEAP despesas run manifests under Raw ``_metadata/runs`` (Bronze contract)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from .metadata import (
    PROFILE_FANOUT_RUN,
    build_run_metadata,
    validate_completed_metadata,
    write_run_metadata,
    write_success_marker,
)

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter

CEAP_DESPESAS_PREFIX = "raw/camara/ceap/api/despesas"


def ceap_run_manifest_prefix(pipeline_run_id: str) -> str:
    return f"{CEAP_DESPESAS_PREFIX}/_metadata/runs/pipeline_run_id={pipeline_run_id}"


def ceap_run_metadata_path(pipeline_run_id: str) -> str:
    return f"{ceap_run_manifest_prefix(pipeline_run_id)}/metadata.json"


def ceap_run_success_path(pipeline_run_id: str) -> str:
    return f"{ceap_run_manifest_prefix(pipeline_run_id)}/_SUCCESS"


def read_ceap_run_metadata(
    adls: AdlsRawWriter, pipeline_run_id: str
) -> dict[str, Any] | None:
    return adls.read_json(ceap_run_metadata_path(pipeline_run_id))


def build_ceap_dispatcher_run_metadata(
    *,
    pipeline_run_id: str,
    mode: str,
    status: str,
    started_at_utc: str,
    finished_at_utc: str | None,
    failed_at_utc: str | None,
    total_tasks_expected: int,
    total_tasks_queued: int,
    total_tasks_pending: int,
    target_year: int,
    months_to_process_json: str,
    enqueue_phase_complete: bool,
    deputies_snapshot_date: str = "",
    deputies_snapshot_path: str = "",
    deputies_snapshot_record_count: int = 0,
    deputies_snapshot_status: str = "",
    error_type: str | None = None,
    error_message: str | None = None,
    total_tasks_success: int = 0,
    total_tasks_failed: int = 0,
    total_tasks_poison: int = 0,
    total_tasks_running: int = 0,
) -> dict[str, Any]:
    """Payload for ``metadata.json`` under ``_metadata/runs`` (dispatcher-owned).

    Includes v1.0 base fields via :func:`build_run_metadata` plus explicit
    ``*_utc`` timestamps for operations / App Insights correlation.
    """
    st_upper = str(status).upper()
    normalized = cast(
        Any,
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
    succ_path = ceap_run_success_path(pipeline_run_id)
    try:
        month_ints = json.loads(months_to_process_json)
    except (json.JSONDecodeError, TypeError):
        month_ints = []
    extras: dict[str, Any] = {"months_to_process": months_to_process_json}
    if isinstance(month_ints, list):
        extras["reference_months"] = month_ints

    fanout_kw: dict[str, Any] | None = (
        {"parent_entity": "deputados_list", "parent_snapshot_path": deputies_snapshot_path}
        if deputies_snapshot_path
        else None
    )

    meta = build_run_metadata(
        source="camara",
        domain="ceap",
        entity="deputado_despesas",
        endpoint="deputado_despesas",
        api_base_url="https://dadosabertos.camara.leg.br/api/v2",
        api_path="/deputados/{id}/despesas",
        pipeline_run_id=pipeline_run_id,
        run_type="reconciliation" if mode == "reconciliation" else "daily",
        status=normalized,
        started_at=started_at_utc,
        completed_at=finished_at_utc,
        raw_path=f"{CEAP_DESPESAS_PREFIX}/",
        success_marker_path=succ_path,
        error_message=error_message,
        partitioning={"target_year": target_year},
        tasks={
            "total_tasks_expected": int(total_tasks_expected or 0),
            "total_tasks_queued": int(total_tasks_queued or 0),
            "total_tasks_success": int(total_tasks_success or 0),
            "total_tasks_failed": int(total_tasks_failed or 0),
            "total_tasks_pending": int(total_tasks_pending or 0),
            "total_tasks_poison": int(total_tasks_poison or 0),
            "total_tasks_running": int(total_tasks_running or 0),
            "enqueue_phase_complete": enqueue_phase_complete,
        },
        fanout=fanout_kw,
        extras=extras,
    )
    meta["started_at_utc"] = started_at_utc
    meta["finished_at_utc"] = finished_at_utc
    meta["failed_at_utc"] = failed_at_utc
    meta["pipeline_run_id"] = pipeline_run_id
    meta["status"] = st_upper
    meta["deputies_snapshot_date"] = deputies_snapshot_date or ""
    meta["deputies_snapshot_record_count"] = int(deputies_snapshot_record_count or 0)
    meta["deputies_snapshot_status"] = deputies_snapshot_status or ""
    meta["deputies_snapshot_path"] = deputies_snapshot_path or ""
    if error_type:
        meta["error_type"] = error_type
    elif "error_type" in meta:
        del meta["error_type"]
    return meta


def persist_ceap_dispatcher_run_metadata(
    adls: AdlsRawWriter,
    manifest: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, str]:
    """Writes ``metadata.json`` and optionally ``_SUCCESS`` (only when allowed)."""
    pid = str(manifest.get("pipeline_run_id", "") or "")
    mpath = ceap_run_metadata_path(pid)
    spath = ceap_run_success_path(pid)
    write_run_metadata(adls, mpath, manifest)
    if write_success_marker_now:
        write_success_marker(adls, spath, manifest)
    return mpath, spath


def is_ceap_run_manifest_valid_for_bronze(
    adls: AdlsRawWriter, pipeline_run_id: str
) -> tuple[bool, dict[str, Any]]:
    """True when Raw manifests satisfy the CEAP completion contract (no Table Storage).

    Delegates to the shared ``PROFILE_FANOUT_RUN`` validator for consistency
    with other fanout-style domains.
    """
    meta = read_ceap_run_metadata(adls, pipeline_run_id) or {}
    ok, _reason = validate_completed_metadata(
        adls,
        meta,
        ceap_run_success_path(pipeline_run_id),
        profile=PROFILE_FANOUT_RUN,
    )
    return ok, meta
