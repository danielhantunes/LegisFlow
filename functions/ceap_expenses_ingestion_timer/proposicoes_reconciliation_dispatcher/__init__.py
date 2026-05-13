"""Timer: proposições weekly reconciliation (Sunday 06:30 UTC default).

Window: first day of **previous calendar month** through **today** (covers
mês anterior + mês atual). Delegates to :func:`execute_proposicoes_reconciliation_tick`.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import azure.functions as func

from shared.proposicoes_reconciliation_tick import execute_proposicoes_reconciliation_tick


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    now = datetime.now(UTC)
    today = now.date()
    first_this_month = today.replace(day=1)
    last_day_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_day_prev_month.replace(day=1)
    date_start = first_prev_month.isoformat()
    date_end = today.isoformat()
    target_year = int(os.getenv("TARGET_YEAR", str(now.year)))
    execute_proposicoes_reconciliation_tick(
        now=now,
        date_start=date_start,
        date_end=date_end,
        target_year=target_year,
        recon_day=now.isoweekday(),
    )
