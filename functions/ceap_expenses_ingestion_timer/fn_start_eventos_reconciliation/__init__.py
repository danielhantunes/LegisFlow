"""HTTP: dry-run or start manual eventos reconciliation."""

from __future__ import annotations

import json
import os
import traceback
from datetime import UTC, datetime

import azure.functions as func

from shared.api_client import CamaraApiClient
from shared.domain_catalog import EVENTOS_DOMAIN, eventos_reconciliation_run_id
from shared.eventos_reconciliation_tick import (
    count_eventos_in_date_range_dry_run,
    execute_eventos_reconciliation_tick,
)
from shared.logger import get_logger, log_structured
from shared.manual_reconciliation_common import (
    manual_reconciliation_enabled,
    registry_run_completed,
    validate_dates_no_target_year,
)
from shared.run_registry import GenericRunRegistry

logger = get_logger()


def _json(body: dict[str, object], *, status: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body, ensure_ascii=False),
        status_code=status,
        mimetype="application/json",
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    if not manual_reconciliation_enabled():
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
    recon_day = max(1, min(28, int(os.getenv("EVENTOS_RECONCILIATION_DAY", "25"))))
    pipeline_run_id = eventos_reconciliation_run_id(now.strftime("%Y-%m-%d"))

    date_start = str(body.get("date_start", "") or "").strip()
    date_end = str(body.get("date_end", "") or "").strip()
    dry_run = bool(body.get("dry_run", True))

    if not date_start or not date_end:
        return _json({"error": "date_start and date_end are required (YYYY-MM-DD)."}, status=400)

    verrors = validate_dates_no_target_year(date_start, date_end)
    if verrors:
        return _json({"error": "validation_failed", "details": verrors}, status=400)

    domain = EVENTOS_DOMAIN
    list_endpoint = domain.endpoint("eventos")
    api = CamaraApiClient(base_url=domain.api_base_url)
    max_pages_count = int(
        os.getenv("EVENTOS_MANUAL_RECONCILIATION_MAX_LIST_PAGES", "5000")
    )

    if dry_run:
        n_ids, pages, warnings = count_eventos_in_date_range_dry_run(
            api=api,
            list_endpoint=list_endpoint,
            date_start=date_start,
            date_end=date_end,
            max_pages=max_pages_count,
        )
        log_structured(
            logger,
            "info",
            "Manual eventos reconciliation dry-run.",
            pipeline_run_id=pipeline_run_id,
            date_start=date_start,
            date_end=date_end,
            dry_run=True,
        )
        return _json(
            {
                "dry_run": True,
                "pipeline_run_id": pipeline_run_id,
                "date_start": date_start,
                "date_end": date_end,
                "distinct_evento_ids": n_ids,
                "pages_fetched": pages,
                "warnings": warnings,
            },
            status=200,
        )

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    registry = GenericRunRegistry.from_connection_string(
        conn,
        control_table,
        runs_partition_key=domain.runs_partition_key,
        locks_partition_key=domain.locks_partition_key,
        lock_row_key=domain.lock_row_key,
    )
    if registry_run_completed(registry, pipeline_run_id):
        return _json(
            {
                "error": (
                    "This pipeline_run_id is already COMPLETED; use reset before re-running."
                ),
                "pipeline_run_id": pipeline_run_id,
            },
            status=409,
        )

    ds = datetime.fromisoformat(date_start).date()
    de = datetime.fromisoformat(date_end).date()
    lookback_meta = max(1, (de - ds).days + 1)

    try:
        execute_eventos_reconciliation_tick(
            now=now,
            date_start=date_start,
            date_end=date_end,
            recon_day=recon_day,
            lookback_days=lookback_meta,
        )
    except Exception as exc:  # noqa: BLE001
        log_structured(
            logger,
            "error",
            "Manual eventos reconciliation failed.",
            pipeline_run_id=pipeline_run_id,
            error=str(exc)[:2048],
            error_type=type(exc).__name__,
            traceback=traceback.format_exc()[:8000],
        )
        return _json(
            {"error": str(exc), "error_type": type(exc).__name__},
            status=500,
        )

    log_structured(
        logger,
        "info",
        "Manual eventos reconciliation tick executed.",
        pipeline_run_id=pipeline_run_id,
        date_start=date_start,
        date_end=date_end,
        dry_run=False,
    )
    return _json(
        {
            "dry_run": False,
            "pipeline_run_id": pipeline_run_id,
            "date_start": date_start,
            "date_end": date_end,
        },
        status=200,
    )
