"""Timer: discursos weekly reconciliation (default Sunday 08:25 UTC).

Uses the calendar window **first day of previous month → today** (UTC); see
:func:`shared.discursos_reconciliation_dates.default_discursos_reconciliation_window`.
"""

from __future__ import annotations

from datetime import UTC, datetime

import azure.functions as func

from shared.discursos_reconciliation_dates import (
    default_discursos_reconciliation_window,
)
from shared.discursos_reconciliation_tick import execute_discursos_reconciliation_tick


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    now = datetime.now(UTC)
    ds, de = default_discursos_reconciliation_window(now=now)
    execute_discursos_reconciliation_tick(now=now, date_start=ds, date_end=de)
