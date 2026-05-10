"""HTTP: controlled reset of one CEAP pipeline_run_id (dev/test only).

Requires ENABLE_CEAP_RESET_FUNCTION=true.
"""

from __future__ import annotations

import json
import os
import traceback

import azure.functions as func

from shared.ceap_pipeline_reset import (
    ResetSummary,
    is_allowed_pipeline_run_id,
    run_ceap_pipeline_reset,
)
from shared.logger import get_logger, log_structured

logger = get_logger()


def main(req: func.HttpRequest) -> func.HttpResponse:
    def _json_response(body: dict[str, object], *, status: int) -> func.HttpResponse:
        return func.HttpResponse(
            json.dumps(body, ensure_ascii=False),
            status_code=status,
            mimetype="application/json",
        )

    enabled = os.getenv("ENABLE_CEAP_RESET_FUNCTION", "false").lower() == "true"
    if not enabled:
        log_structured(
            logger,
            "warning",
            "CEAP pipeline reset rejected (feature disabled).",
            enable_ceap_reset_function=False,
        )
        return _json_response(
            {"error": "ENABLE_CEAP_RESET_FUNCTION is not true; refusing reset."},
            status=403,
        )

    try:
        body = req.get_json() or {}
    except ValueError:
        return _json_response({"error": "Invalid JSON body."}, status=400)

    pipeline_run_id = str(body.get("pipeline_run_id", "") or "").strip()
    dry_run = bool(body.get("dry_run", False))
    delete_raw = bool(body.get("delete_raw", True))
    delete_queues = bool(body.get("delete_queues", True))
    delete_tables = bool(body.get("delete_tables", True))
    delete_deputies_snapshot = bool(body.get("delete_deputies_snapshot", False))

    if not pipeline_run_id:
        return _json_response(
            {"error": "pipeline_run_id is required."},
            status=400,
        )
    if not is_allowed_pipeline_run_id(pipeline_run_id):
        return _json_response(
            {
                "error": (
                    "pipeline_run_id must match ceap_daily_YYYYMMDD or "
                    "ceap_reconciliation_YYYYMMDD."
                ),
            },
            status=400,
        )

    conn_str = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_work = os.environ["CEAP_API_QUEUE_NAME"]
    queue_poison = os.environ["CEAP_API_POISON_QUEUE_NAME"]
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    filesystem = os.getenv("LAKEHOUSE_FILESYSTEM_NAME", "lakehouse")

    try:
        summary: ResetSummary = run_ceap_pipeline_reset(
            pipeline_run_id=pipeline_run_id,
            dry_run=dry_run,
            delete_raw=delete_raw,
            delete_queues=delete_queues,
            delete_tables=delete_tables,
            delete_deputies_snapshot=delete_deputies_snapshot,
            conn_str=conn_str,
            control_table=control_table,
            state_table=state_table,
            queue_work_name=queue_work,
            queue_poison_name=queue_poison,
            raw_account=raw_account,
            filesystem=filesystem,
        )
    except Exception as exc:
        log_structured(
            logger,
            "error",
            "CEAP pipeline reset failed.",
            pipeline_run_id=pipeline_run_id,
            dry_run=dry_run,
            delete_raw=delete_raw,
            delete_queues=delete_queues,
            delete_tables=delete_tables,
            delete_deputies_snapshot=delete_deputies_snapshot,
            error=str(exc)[:2048],
            error_type=type(exc).__name__,
            traceback=traceback.format_exc()[:8000],
        )
        return _json_response(
            {"error": str(exc), "error_type": type(exc).__name__},
            status=500,
        )

    d = summary.deleted
    log_structured(
        logger,
        "info",
        "CEAP pipeline reset finished.",
        pipeline_run_id=pipeline_run_id,
        dry_run=dry_run,
        delete_raw=delete_raw,
        delete_queues=delete_queues,
        delete_tables=delete_tables,
        delete_deputies_snapshot=delete_deputies_snapshot,
        records_deleted=int(d.get("state_records", 0) or 0),
        files_deleted=int(d.get("raw_files", 0) or 0)
        + int(d.get("metadata_files", 0) or 0)
        + int(d.get("deputies_snapshot_files", 0) or 0),
        queue_messages_deleted=int(d.get("queue_messages", 0) or 0)
        + int(d.get("poison_messages", 0) or 0),
        warnings=summary.warnings,
    )

    return _json_response(summary.to_json(), status=200)
