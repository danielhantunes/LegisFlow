"""HTTP: dry-run or start manual votações reconciliation (same ``pipeline_run_id`` as daily recon)."""

from __future__ import annotations

import json
import os
import traceback
from datetime import UTC, datetime

import azure.functions as func

from shared.api_client import CamaraApiClient
from shared.domain_catalog import VOTACOES_DOMAIN, votacoes_reconciliation_run_id
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.run_registry import GenericRunRegistry
from shared.votacoes_api_dispatcher_logic import (
    count_votacoes_in_date_range_dry_run,
    validate_manual_votacoes_reconciliation_dates,
)
from shared.votacoes_dispatcher_tick import execute_votacoes_ingestion_tick

logger = get_logger()


def _json(body: dict[str, object], *, status: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body, ensure_ascii=False),
        status_code=status,
        mimetype="application/json",
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    domain = VOTACOES_DOMAIN
    flag = str(os.getenv("ENABLE_MANUAL_RECONCILIATION_FUNCTIONS", "")).lower()
    if flag not in ("1", "true", "yes"):
        log_structured(
            logger,
            "warning",
            "Manual votacoes reconciliation rejected (feature disabled).",
            enable_manual_reconciliation_functions=False,
        )
        return _json(
            {
                "error": (
                    "Manual reconciliation is disabled. Set "
                    "ENABLE_MANUAL_RECONCILIATION_FUNCTIONS=true."
                )
            },
            status=403,
        )

    try:
        body = req.get_json() or {}
    except ValueError:
        return _json({"error": "Invalid JSON body."}, status=400)

    now = datetime.now(UTC)
    recon_day = max(1, min(28, int(os.getenv("VOTACOES_RECONCILIATION_DAY", "25"))))
    pipeline_run_id = votacoes_reconciliation_run_id(now.strftime("%Y-%m-%d"))

    try:
        target_year = int(body.get("target_year", 0))
    except (TypeError, ValueError):
        return _json({"error": "target_year must be an integer."}, status=400)

    date_start = str(body.get("date_start", "") or "").strip()
    date_end = str(body.get("date_end", "") or "").strip()
    dry_run = bool(body.get("dry_run", True))

    if not date_start or not date_end:
        return _json({"error": "date_start and date_end are required (YYYY-MM-DD)."}, status=400)

    allow_year_mismatch = str(
        os.getenv("VOTACOES_MANUAL_ALLOW_YEAR_MISMATCH", "")
    ).lower() in ("1", "true", "yes")
    verrors = validate_manual_votacoes_reconciliation_dates(
        target_year=target_year,
        date_start=date_start,
        date_end=date_end,
        allow_year_mismatch=allow_year_mismatch,
    )
    if verrors:
        return _json({"error": "validation_failed", "details": verrors}, status=400)

    list_endpoint = domain.endpoint("votacoes")
    api = CamaraApiClient(base_url=domain.api_base_url)
    max_pages_count = int(os.getenv("VOTACOES_MANUAL_RECONCILIATION_MAX_LIST_PAGES", "5000"))

    if dry_run:
        n_ids, pages, warnings = count_votacoes_in_date_range_dry_run(
            api=api,
            list_endpoint=list_endpoint,
            date_start=date_start,
            date_end=date_end,
            max_pages=max_pages_count,
        )
        log_structured(
            logger,
            "info",
            "Manual votacoes reconciliation dry-run.",
            pipeline_run_id=pipeline_run_id,
            date_start=date_start,
            date_end=date_end,
            dry_run=True,
            total_tasks_expected=n_ids,
            messages_enqueued=0,
        )
        return _json(
            {
                "dry_run": True,
                "pipeline_run_id": pipeline_run_id,
                "date_start": date_start,
                "date_end": date_end,
                "target_year": target_year,
                "distinct_votacao_ids": n_ids,
                "pages_fetched": pages,
                "warnings": warnings,
            },
            status=200,
        )

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("VOTACOES_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    lock_ttl = int(os.getenv("VOTACOES_LOCK_TTL_MINUTES", str(domain.lock_ttl_minutes)))
    max_messages_per_tick = max(
        1, int(os.getenv("VOTACOES_MAX_MESSAGES_PER_TICK", "500"))
    )
    max_list_pages = int(
        os.getenv(
            "VOTACOES_MANUAL_RECONCILIATION_MAX_LIST_PAGES",
            os.getenv("VOTACOES_MAX_LIST_PAGES", "200"),
        )
    )
    max_pages_tick = max_list_pages
    stale_raw = os.getenv("VOTACOES_STALE_AFTER_MINUTES")
    stale_after = (
        int(stale_raw)
        if stale_raw not in (None, "")
        else int(domain.stale_after_minutes)
    )

    registry = GenericRunRegistry.from_connection_string(
        conn,
        control_table,
        runs_partition_key=domain.runs_partition_key,
        locks_partition_key=domain.locks_partition_key,
        lock_row_key=domain.lock_row_key,
    )
    parts = GenericPartitionStateStore.from_connection_string(
        conn, state_table, partition_key=domain.state_partition_key
    )

    run = registry.get_run(pipeline_run_id) or {}
    if str(run.get("status", "")).upper() == "COMPLETED":
        return _json(
            {
                "error": (
                    "This pipeline_run_id is already COMPLETED; use reset before "
                    "re-running."
                ),
                "pipeline_run_id": pipeline_run_id,
            },
            status=409,
        )

    window_start = datetime.fromisoformat(f"{date_start}T00:00:00+00:00")
    window_end_cap = datetime.fromisoformat(f"{date_end}T23:59:59.999999+00:00")
    window_end = min(now, window_end_cap)

    try:
        summary = execute_votacoes_ingestion_tick(
            domain=domain,
            now=now,
            registry=registry,
            parts=parts,
            raw_account=raw_account,
            queue_name=queue_name,
            lock_ttl=lock_ttl,
            max_messages_per_tick=max_messages_per_tick,
            max_list_pages=max_list_pages,
            max_pages_tick=max_pages_tick,
            stale_after=stale_after,
            pipeline_run_id=pipeline_run_id,
            mode="reconciliation",
            run_type_label="reconciliation",
            date_start=date_start,
            date_end=date_end,
            window_start=window_start,
            window_end=window_end,
            target_year=target_year,
            recon_day=recon_day,
        )
    except Exception as exc:  # noqa: BLE001
        log_structured(
            logger,
            "error",
            "Manual votacoes reconciliation failed.",
            pipeline_run_id=pipeline_run_id,
            date_start=date_start,
            date_end=date_end,
            dry_run=False,
            error=str(exc)[:2048],
            error_type=type(exc).__name__,
            traceback=traceback.format_exc()[:8000],
        )
        return _json(
            {"error": str(exc), "error_type": type(exc).__name__},
            status=500,
        )

    if summary.get("skipped"):
        reason = str(summary.get("reason", ""))
        status = 409 if reason in ("lock_held", "already_completed") else 200
        log_structured(
            logger,
            "warning" if status >= 400 else "info",
            "Manual votacoes reconciliation skipped after lock or state check.",
            pipeline_run_id=pipeline_run_id,
            reason=reason,
            dry_run=False,
        )
        return _json({"skipped": True, "reason": reason, **summary}, status=status)

    log_structured(
        logger,
        "info",
        "Manual votacoes reconciliation tick executed.",
        pipeline_run_id=pipeline_run_id,
        date_start=date_start,
        date_end=date_end,
        dry_run=False,
        total_tasks_expected=summary.get("total_tasks_expected"),
        messages_enqueued=summary.get("messages_enqueued"),
        run_status_final=summary.get("run_status_final"),
    )
    return _json({"dry_run": False, **summary}, status=200)
