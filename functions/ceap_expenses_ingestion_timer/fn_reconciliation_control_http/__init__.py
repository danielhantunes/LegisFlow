"""HTTP: start / status / pause / resume / cancel for controlled reconciliation."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import azure.functions as func

from shared.logger import get_logger, log_structured
from shared.manual_reconciliation_common import (
    validate_dates_with_target_year,
)
from shared.reconciliation_control_store import ReconciliationControlStore
from shared.reconciliation_proposicoes_controlled import (
    start_proposicoes_controlled_reconciliation,
)
from shared.reconciliation_scheduler_core import execute_reconciliation_scheduler_tick

logger = get_logger()


def _json(body: dict[str, object], *, status: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body, ensure_ascii=False),
        status_code=status,
        mimetype="application/json",
    )


def _enabled() -> bool:
    return str(os.getenv("ENABLE_RECONCILIATION_CONTROL_HTTP", "")).lower() in (
        "1",
        "true",
        "yes",
    )


def _param(req: func.HttpRequest, key: str) -> str:
    raw = (req.params or {}).get(key)
    if raw is None:
        return ""
    if isinstance(raw, list):
        return str(raw[0]).strip() if raw else ""
    return str(raw).strip()


def main(req: func.HttpRequest) -> func.HttpResponse:
    if not _enabled():
        return _json(
            {
                "error": (
                    "Disabled. Set ENABLE_RECONCILIATION_CONTROL_HTTP=true "
                    "(function key required; authLevel=function)."
                )
            },
            status=403,
        )

    try:
        body = req.get_json() or {}
    except ValueError:
        body = {}

    action = str(
        body.get("action")
        or _param(req, "action")
        or ("status" if req.method == "GET" else "")
    ).strip().lower()

    conn = os.environ["AzureWebJobsStorage"]
    table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    store = ReconciliationControlStore.from_connection_string(conn, table)
    now = datetime.now(UTC)

    if action == "status":
        domain = str(body.get("domain") or _param(req, "domain")).strip().lower()
        control_id = str(body.get("control_id") or _param(req, "control_id")).strip()
        if not domain or not control_id:
            return _json({"error": "domain_and_control_id_required"}, status=400)
        row = store.get(domain=domain, control_id=control_id)
        if not row:
            return _json({"error": "not_found"}, status=404)
        return _json({"control": _public_control_view(row)}, status=200)

    if action == "pause":
        domain = str(body.get("domain") or "").strip().lower()
        control_id = str(body.get("control_id") or "").strip()
        if not domain or not control_id:
            return _json({"error": "domain_and_control_id_required"}, status=400)
        cur = store.get(domain=domain, control_id=control_id)
        if not cur:
            return _json({"error": "not_found"}, status=404)
        if str(cur.get("status", "")).upper() != "RUNNING":
            return _json({"error": "invalid_state_for_pause", "status": cur.get("status")}, status=409)
        store.upsert_merge(domain=domain, control_id=control_id, fields={"status": "PAUSED"})
        log_structured(logger, "info", "reconciliation paused", control_id=control_id, domain=domain)
        return _json({"status": "PAUSED", "control_id": control_id, "domain": domain}, status=200)

    if action == "resume":
        domain = str(body.get("domain") or "").strip().lower()
        control_id = str(body.get("control_id") or "").strip()
        if not domain or not control_id:
            return _json({"error": "domain_and_control_id_required"}, status=400)
        cur = store.get(domain=domain, control_id=control_id)
        if not cur:
            return _json({"error": "not_found"}, status=404)
        if str(cur.get("status", "")).upper() != "PAUSED":
            return _json({"error": "invalid_state_for_resume", "status": cur.get("status")}, status=409)
        store.upsert_merge(domain=domain, control_id=control_id, fields={"status": "RUNNING"})
        log_structured(logger, "info", "reconciliation resumed", control_id=control_id, domain=domain)
        return _json({"status": "RUNNING", "control_id": control_id, "domain": domain}, status=200)

    if action == "cancel":
        domain = str(body.get("domain") or "").strip().lower()
        control_id = str(body.get("control_id") or "").strip()
        if not domain or not control_id:
            return _json({"error": "domain_and_control_id_required"}, status=400)
        cur = store.get(domain=domain, control_id=control_id)
        if not cur:
            return _json({"error": "not_found"}, status=404)
        st = str(cur.get("status", "")).upper()
        if st in ("COMPLETED", "CANCELLED"):
            return _json({"error": "already_terminal", "status": st}, status=409)
        store.upsert_merge(
            domain=domain,
            control_id=control_id,
            fields={
                "status": "CANCELLED",
                "finished_at": datetime.now(UTC).isoformat(),
            },
        )
        log_structured(logger, "info", "reconciliation cancelled", control_id=control_id, domain=domain)
        return _json({"status": "CANCELLED", "control_id": control_id, "domain": domain}, status=200)

    if action == "run_once":
        """Operator/debug: process one scheduler pass synchronously."""
        out = execute_reconciliation_scheduler_tick(conn=conn, control_table=table, now=now)
        return _json(out, status=200)

    if action != "start":
        return _json({"error": "unknown_action", "allowed": [
            "start", "status", "pause", "resume", "cancel", "run_once",
        ]}, status=400)

    domain = str(body.get("domain") or "proposicoes").strip().lower()
    if domain != "proposicoes":
        return _json({"error": "domain_not_supported_yet", "domain": domain}, status=400)

    try:
        target_year = int(body.get("target_year", 0))
    except (TypeError, ValueError):
        return _json({"error": "target_year_must_be_integer"}, status=400)

    date_start = str(body.get("date_start", "") or "").strip()
    date_end = str(body.get("date_end", "") or "").strip()
    dry_run = str(body.get("dry_run", "false")).lower() in ("1", "true", "yes")

    if not date_start or not date_end:
        return _json({"error": "date_start_and_date_end_required"}, status=400)

    allow_year_mismatch = str(
        os.getenv("PROPOSICOES_MANUAL_ALLOW_YEAR_MISMATCH", "")
    ).lower() in ("1", "true", "yes")
    verrors = validate_dates_with_target_year(
        target_year=target_year,
        date_start=date_start,
        date_end=date_end,
        allow_year_mismatch=allow_year_mismatch,
    )
    if verrors:
        return _json({"error": "validation_failed", "details": verrors}, status=400)

    max_tasks = max(1, int(body.get("max_tasks_per_run", 500)))
    max_rt = max(1, int(body.get("max_runtime_minutes", 9)))
    recon_day = int(body.get("recon_day", now.isoweekday()))

    out = start_proposicoes_controlled_reconciliation(
        now=now,
        date_start=date_start,
        date_end=date_end,
        target_year=target_year,
        recon_day=recon_day,
        max_tasks_per_run=max_tasks,
        max_runtime_minutes=max_rt,
        dry_run=dry_run,
    )
    if out.get("error"):
        return _json(out, status=409)
    return _json(out, status=200)


def _public_control_view(row: dict[str, object]) -> dict[str, object]:
    hide = {"PartitionKey", "RowKey", "odata.metadata", "Timestamp", "etag"}
    return {k: v for k, v in row.items() if k not in hide}
