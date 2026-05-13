"""Timer: eventos daily list + fanout (07:15 UTC default).

One ``pipeline_run_id`` per UTC calendar day (``eventos_daily_YYYYMMDD``).
Lists ``/eventos`` for today through ``EVENTOS_DAILY_FUTURE_DAYS`` and
fanouts with list-hash idempotency.
"""

from __future__ import annotations

from datetime import UTC, datetime

import azure.functions as func

from shared.eventos_daily_tick import execute_eventos_daily_tick


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    execute_eventos_daily_tick(now=datetime.now(UTC))
