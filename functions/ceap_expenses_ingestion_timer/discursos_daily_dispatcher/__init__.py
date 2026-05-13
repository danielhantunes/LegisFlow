"""Timer: discursos daily deputies list + fanout (default 07:25 UTC).

One ``pipeline_run_id`` per UTC calendar day (``discursos_daily_YYYYMMDD``).
"""

from __future__ import annotations

from datetime import UTC, datetime

import azure.functions as func

from shared.discursos_daily_tick import execute_discursos_daily_tick


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    execute_discursos_daily_tick(now=datetime.now(UTC))
