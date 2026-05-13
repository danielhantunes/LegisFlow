"""Timer: eventos weekly reconciliation (Sunday 08:15 UTC default).

Delegates to :func:`execute_eventos_reconciliation_tick` with a wide
``/eventos`` date window (past + future days from env).
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import azure.functions as func

from shared.eventos_reconciliation_dates import default_eventos_reconciliation_window
from shared.eventos_reconciliation_tick import execute_eventos_reconciliation_tick


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    now = datetime.now(UTC)
    ds, de = default_eventos_reconciliation_window(now=now)
    d0 = date.fromisoformat(ds)
    d1 = date.fromisoformat(de)
    lookback_meta = (d1 - d0).days + 1
    execute_eventos_reconciliation_tick(
        now=now,
        date_start=ds,
        date_end=de,
        recon_day=now.isoweekday(),
        lookback_days=lookback_meta,
    )
