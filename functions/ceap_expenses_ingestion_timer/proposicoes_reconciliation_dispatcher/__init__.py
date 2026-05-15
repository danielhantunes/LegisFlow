"""Timer: proposições weekly reconciliation starter (Sunday 06:30 UTC default).

When ``PROPOSICOES_USE_CONTROLLED_RECONCILIATION`` is true, this timer only
registers a :class:`shared.reconciliation_control_store.ReconciliationControl`
row (RUNNING); batches are driven by ``reconciliation_scheduler``.

Otherwise it runs the legacy monolithic
:func:`shared.proposicoes_reconciliation_tick.execute_proposicoes_reconciliation_tick`.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import azure.functions as func

from shared.logger import get_logger, log_structured
from shared.proposicoes_reconciliation_tick import execute_proposicoes_reconciliation_tick
from shared.reconciliation_proposicoes_controlled import (
    start_proposicoes_controlled_reconciliation,
)

logger = get_logger()


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    now = datetime.now(UTC)
    today = now.date()
    first_this_month = today.replace(day=1)
    last_day_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_day_prev_month.replace(day=1)
    date_start = first_prev_month.isoformat()
    date_end = today.isoformat()
    target_year = int(os.getenv("TARGET_YEAR", str(now.year)))
    recon_day = now.isoweekday()

    use_controlled = str(
        os.getenv("PROPOSICOES_USE_CONTROLLED_RECONCILIATION", "")
    ).lower() in ("1", "true", "yes")
    if use_controlled:
        max_tasks = max(1, int(os.getenv("PROPOSICOES_RECON_SCHEDULER_MAX_TASKS", "500")))
        max_rt = max(1, int(os.getenv("PROPOSICOES_RECON_SCHEDULER_MAX_RUNTIME_MIN", "9")))
        out = start_proposicoes_controlled_reconciliation(
            now=now,
            date_start=date_start,
            date_end=date_end,
            target_year=target_year,
            recon_day=recon_day,
            max_tasks_per_run=max_tasks,
            max_runtime_minutes=max_rt,
            dry_run=False,
        )
        if out.get("error"):
            log_structured(
                logger,
                "warning",
                "proposicoes reconciliation starter skipped",
                error=out.get("error"),
            )
        else:
            log_structured(
                logger,
                "info",
                "proposicoes controlled reconciliation registered",
                control_id=out.get("control_id"),
                pipeline_run_id=out.get("pipeline_run_id"),
            )
        return

    execute_proposicoes_reconciliation_tick(
        now=now,
        date_start=date_start,
        date_end=date_end,
        target_year=target_year,
        recon_day=recon_day,
    )
