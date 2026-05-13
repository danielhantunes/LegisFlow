"""HTTP: controlled reset of one proposicoes pipeline_run_id (dev/test only).

Requires either ``ENABLE_RESET_FUNCTIONS=true`` or
``ENABLE_PROPOSICOES_RESET_FUNCTION=true``.
"""

from __future__ import annotations

import json
import os
import traceback

import azure.functions as func

from shared.admin_guard import reset_enabled_for_domain
from shared.domain_catalog import PROPOSICOES_DOMAIN
from shared.logger import get_logger, log_structured
from shared.proposicoes_pipeline_reset import (
    ResetSummary,
    run_proposicoes_pipeline_reset,
)
from shared.proposicoes_pipeline_reset_helpers import (
    is_allowed_proposicoes_pipeline_run_id,
)

logger = get_logger()


def _json_response(body: dict[str, object], *, status: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body, ensure_ascii=False),
        status_code=status,
        mimetype="application/json",
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    domain = PROPOSICOES_DOMAIN
    if not reset_enabled_for_domain(domain.reset_feature_flag_env):
        log_structured(
            logger,
            "warning",
            "Proposicoes pipeline reset rejected (feature disabled).",
            enable_reset_functions=False,
        )
        return _json_response(
            {
                "error": (
                    "Reset is disabled. Set ENABLE_RESET_FUNCTIONS=true or "
                    f"{domain.reset_feature_flag_env}=true."
                )
            },
            status=403,
        )

    try:
        body = req.get_json() or {}
    except ValueError:
        return _json_response({"error": "Invalid JSON body."}, status=400)

    pipeline_run_id = str(body.get("pipeline_run_id", "") or "").strip()
    dry_run = bool(body.get("dry_run", True))
    delete_raw = bool(body.get("delete_raw", True))
    delete_queues = bool(body.get("delete_queues", True))
    delete_tables = bool(body.get("delete_tables", True))

    if not pipeline_run_id:
        return _json_response(
            {"error": "pipeline_run_id is required."}, status=400
        )
    if not is_allowed_proposicoes_pipeline_run_id(pipeline_run_id):
        return _json_response(
            {
                "error": (
                    "pipeline_run_id must match proposicoes_microbatch_YYYYMMDDHHMM, "
                    "proposicoes_daily_YYYYMMDD, or proposicoes_reconciliation_YYYYMMDD."
                )
            },
            status=400,
        )

    conn_str = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_work = os.getenv("PROPOSICOES_QUEUE_NAME", domain.queue_work)
    queue_poison = os.getenv(
        "PROPOSICOES_POISON_QUEUE_NAME", domain.queue_poison
    )
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    filesystem = os.getenv("LAKEHOUSE_FILESYSTEM_NAME", "lakehouse")

    try:
        summary: ResetSummary = run_proposicoes_pipeline_reset(
            pipeline_run_id=pipeline_run_id,
            dry_run=dry_run,
            delete_raw=delete_raw,
            delete_queues=delete_queues,
            delete_tables=delete_tables,
            conn_str=conn_str,
            control_table=control_table,
            state_table=state_table,
            queue_work_name=queue_work,
            queue_poison_name=queue_poison,
            raw_account=raw_account,
            filesystem=filesystem,
        )
    except Exception as exc:  # noqa: BLE001
        log_structured(
            logger,
            "error",
            "Proposicoes pipeline reset failed.",
            pipeline_run_id=pipeline_run_id,
            dry_run=dry_run,
            error=str(exc)[:2048],
            error_type=type(exc).__name__,
            traceback=traceback.format_exc()[:8000],
        )
        return _json_response(
            {"error": str(exc), "error_type": type(exc).__name__}, status=500
        )

    log_structured(
        logger,
        "info",
        "Proposicoes pipeline reset finished.",
        pipeline_run_id=pipeline_run_id,
        dry_run=dry_run,
        summary=summary.to_json(),
    )
    return _json_response(summary.to_json(), status=200)
