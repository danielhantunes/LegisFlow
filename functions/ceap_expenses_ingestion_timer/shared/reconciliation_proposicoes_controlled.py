"""Controlled (checkpointed) proposições reconciliation — starter + scheduler hooks."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from .adls_writer import AdlsRawWriter
from .domain_catalog import PROPOSICOES_DOMAIN
from .logger import get_logger, log_structured
from .proposicoes_reconciliation_tick import execute_proposicoes_reconciliation_tick
from .reconciliation_batch_manifest import (
    build_reconciliation_batch_manifest,
    persist_reconciliation_batch_manifest,
    reconciliation_batch_manifest_path,
)
from .reconciliation_control_store import ReconciliationControlStore, new_control_id
from .run_registry import GenericRunRegistry

logger = get_logger()

DOMAIN = "proposicoes"


def _manifest_extras(tick_out: dict[str, Any] | None, err: str | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if err:
        out["error"] = err
    if isinstance(tick_out, dict):
        keys = (
            "skipped",
            "reason",
            "run_status_final",
            "listing_complete",
            "enqueue_phase_complete",
            "messages_enqueued",
            "distinct_proposicao_ids",
        )
        out["tick_summary"] = {k: tick_out[k] for k in keys if k in tick_out}
    return out


def allocate_proposicoes_recoctl_pipeline_run_id() -> str:
    """Unique id compatible with :func:`shared.domain_catalog.is_well_formed_pipeline_run_id`."""
    return f"proposicoes_recoctl_{uuid.uuid4().hex[:16]}"


def start_proposicoes_controlled_reconciliation(
    *,
    now: datetime,
    date_start: str,
    date_end: str,
    target_year: int,
    recon_day: int,
    max_tasks_per_run: int,
    max_runtime_minutes: int,
    dry_run: bool,
) -> dict[str, Any]:
    """Create ReconciliationControl row (RUNNING). Does not execute ingestion tick."""
    conn = os.environ["AzureWebJobsStorage"]
    table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    store = ReconciliationControlStore.from_connection_string(conn, table)
    if store.has_running(domain=DOMAIN):
        return {"error": "already_has_running_reconciliation", "domain": DOMAIN}
    control_id = new_control_id()
    pipeline_run_id = allocate_proposicoes_recoctl_pipeline_run_id()
    if dry_run:
        return {
            "dry_run": True,
            "control_id": control_id,
            "would_pipeline_run_id": pipeline_run_id,
            "window_start": date_start,
            "window_end": date_end,
            "target_year": target_year,
        }
    store.create_running(
        domain=DOMAIN,
        control_id=control_id,
        pipeline_run_id=pipeline_run_id,
        window_start=date_start,
        window_end=date_end,
        target_year=target_year,
        recon_day=recon_day,
        max_tasks_per_run=max(1, int(max_tasks_per_run)),
        max_runtime_minutes=max(1, int(max_runtime_minutes)),
        dry_run=False,
        context_json=json.dumps({"source": "controlled_reconciliation_v1"}),
    )
    log_structured(
        logger,
        "info",
        "proposicoes controlled reconciliation started",
        control_id=control_id,
        pipeline_run_id=pipeline_run_id,
        window_start=date_start,
        window_end=date_end,
    )
    return {
        "status": "RUNNING",
        "control_id": control_id,
        "pipeline_run_id": pipeline_run_id,
        "domain": DOMAIN,
        "window_start": date_start,
        "window_end": date_end,
        "target_year": target_year,
    }


def _checkpoint_from_registry(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "phase": "list_fanout",
        "list_next_page": int(run.get("recon_list_next_page") or 1),
        "listing_complete": bool(run.get("recon_listing_complete")),
        "enqueue_phase_complete": bool(run.get("enqueue_phase_complete")),
        "registry_status": str(run.get("status", "") or ""),
    }


def run_proposicoes_controlled_batch(
    *,
    control: dict[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """One scheduler invocation: run at most one reconciliation tick with caps from control."""
    now = now or datetime.now(timezone.utc)
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    conn = os.environ["AzureWebJobsStorage"]
    table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    store = ReconciliationControlStore.from_connection_string(conn, table)
    domain = str(control.get("domain") or DOMAIN)
    control_id = str(control.get("control_id") or control.get("RowKey") or "")
    if not control_id:
        return {"error": "missing_control_id"}
    if str(control.get("status", "")).upper() != "RUNNING":
        return {"skipped": True, "reason": "not_running", "status": control.get("status")}

    started_wall = datetime.now(timezone.utc)
    max_rt = int(control.get("max_runtime_minutes") or 9)
    deadline = started_wall + timedelta(minutes=max_rt)

    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    raw_writer = AdlsRawWriter(account_name=raw_account)
    registry = GenericRunRegistry.from_connection_string(
        conn,
        table,
        runs_partition_key=PROPOSICOES_DOMAIN.runs_partition_key,
        locks_partition_key=PROPOSICOES_DOMAIN.locks_partition_key,
        lock_row_key=PROPOSICOES_DOMAIN.lock_row_key,
    )
    pipeline_run_id = str(control.get("pipeline_run_id") or "")
    run_before = registry.get_run(pipeline_run_id) or {}
    checkpoint_before = _checkpoint_from_registry(run_before)

    max_tasks = max(1, int(control.get("max_tasks_per_run") or 500))
    date_start = str(control.get("window_start") or "")
    date_end = str(control.get("window_end") or "")
    target_year = int(control.get("target_year") or now_utc.year)
    recon_day = int(control.get("recon_day") or 0)

    batch_index = int(control.get("batches_total") or 0) + 1
    manifest_path = reconciliation_batch_manifest_path(
        domain=domain, control_id=control_id, batch_index=batch_index
    )
    tick_out: dict[str, Any] = {}
    err: str | None = None
    try:
        if datetime.now(timezone.utc) > deadline:
            store.upsert_merge(
                domain=domain,
                control_id=control_id,
                fields={
                    "last_batch_status": "LIMIT_REACHED",
                    "last_error": "max_runtime_minutes_exceeded_before_tick",
                    "last_batch_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            return {"skipped": True, "reason": "max_runtime_guard"}

        tick_out = execute_proposicoes_reconciliation_tick(
            now=now_utc,
            date_start=date_start,
            date_end=date_end,
            target_year=target_year,
            recon_day=recon_day,
            pipeline_run_id=pipeline_run_id,
            max_messages_per_tick_override=max_tasks,
        )
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {str(exc)[:500]}"
        log_structured(
            logger,
            "warning",
            "proposicoes controlled batch tick failed",
            control_id=control_id,
            pipeline_run_id=pipeline_run_id,
            error=err,
        )

    run_after = registry.get_run(pipeline_run_id) or {}
    checkpoint_after = _checkpoint_from_registry(run_after)
    finished_at = datetime.now(timezone.utc).isoformat()

    rs = str(run_after.get("status", "") or "").upper()
    tick_skipped = bool(tick_out.get("skipped")) if isinstance(tick_out, dict) else False
    rs_tick = str(tick_out.get("run_status_final") or "").upper() if isinstance(tick_out, dict) else ""

    if err:
        batch_status = "FAILED"
        control_status = "RUNNING"
        last_batch = "FAILED"
    elif tick_skipped and tick_out.get("reason") == "lock_held":
        batch_status = "PARTIAL_SUCCESS"
        last_batch = "LOCK_HELD"
        control_status = "RUNNING"
    elif tick_skipped and str(tick_out.get("reason", "")).startswith("already_completed"):
        batch_status = "PARTIAL_SUCCESS"
        last_batch = "ALREADY_COMPLETED"
        control_status = "COMPLETED"
    elif rs_tick == "COMPLETED" or rs == "COMPLETED":
        batch_status = "PARTIAL_SUCCESS"
        last_batch = "BATCH_OK"
        control_status = "COMPLETED"
    elif rs == "FAILED":
        batch_status = "FAILED"
        control_status = "FAILED"
        last_batch = "REGISTRY_FAILED"
    else:
        batch_status = "PARTIAL_SUCCESS"
        last_batch = "BATCH_OK"
        control_status = "RUNNING"

    enq = int(tick_out.get("messages_enqueued") or 0) if isinstance(tick_out, dict) else 0
    seen = int(tick_out.get("records_seen") or 0) if isinstance(tick_out, dict) else 0
    skip_h = int(tick_out.get("skipped_same_list_hash") or 0) if isinstance(tick_out, dict) else 0

    prev_enq = int(control.get("messages_enqueued_total") or 0)
    prev_seen = int(control.get("records_seen_total") or 0)
    prev_skip = int(control.get("skipped_same_hash_total") or 0)

    manifest = build_reconciliation_batch_manifest(
        control_id=control_id,
        pipeline_run_id=pipeline_run_id,
        domain=domain,
        window_start=date_start,
        window_end=date_end,
        checkpoint_before=checkpoint_before,
        checkpoint_after=checkpoint_after,
        records_seen=seen,
        messages_enqueued=enq,
        records_skipped_same_hash=skip_h,
        records_failed=1 if err else 0,
        status=batch_status,
        started_at=started_wall.isoformat(),
        finished_at=finished_at,
        extras=_manifest_extras(tick_out, err),
    )
    try:
        persist_reconciliation_batch_manifest(raw_writer, manifest_path, manifest)
    except Exception as raw_exc:  # noqa: BLE001
        log_structured(
            logger,
            "warning",
            "reconciliation batch manifest write failed",
            control_id=control_id,
            path=manifest_path,
            error=str(raw_exc)[:300],
        )

    store.upsert_merge(
        domain=domain,
        control_id=control_id,
        fields={
            "status": control_status,
            "checkpoint_json": json.dumps(checkpoint_after),
            "batches_total": batch_index,
            "records_seen_total": prev_seen + seen,
            "messages_enqueued_total": prev_enq + enq,
            "skipped_same_hash_total": prev_skip + skip_h,
            "last_batch_status": last_batch,
            "last_batch_at": finished_at,
            "last_error": err or "",
            "finished_at": finished_at if control_status == "COMPLETED" else "",
        },
    )

    return {
        "control_id": control_id,
        "domain": domain,
        "batch_index": batch_index,
        "batch_status": batch_status,
        "control_status": control_status,
        "manifest_path": manifest_path,
        "tick_summary": _manifest_extras(tick_out, err).get("tick_summary", {}),
        "error": err,
    }
