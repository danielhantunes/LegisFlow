"""Timer: drives checkpointed reconciliation batches (default every 20 minutes)."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import azure.functions as func

from shared.logger import get_logger, log_structured
from shared.reconciliation_scheduler_core import execute_reconciliation_scheduler_tick

logger = get_logger()


def _enabled() -> bool:
    return str(os.getenv("ENABLE_RECONCILIATION_SCHEDULER", "")).lower() in (
        "1",
        "true",
        "yes",
    )


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    if not _enabled():
        return
    conn = os.environ["AzureWebJobsStorage"]
    table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    now = datetime.now(UTC)
    try:
        execute_reconciliation_scheduler_tick(conn=conn, control_table=table, now=now)
    except Exception as exc:  # noqa: BLE001
        log_structured(
            logger,
            "warning",
            "reconciliation_scheduler failed",
            error=str(exc)[:500],
            error_type=type(exc).__name__,
        )
        raise
