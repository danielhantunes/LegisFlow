"""Pure date helpers for eventos reconciliation (no Azure SDK imports)."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta


def default_eventos_reconciliation_window(*, now: datetime) -> tuple[str, str]:
    """UTC calendar dates for ``/eventos`` list (inclusive).

    Window: ``today - EVENTOS_RECONCILIATION_PAST_DAYS`` through
    ``today + EVENTOS_RECONCILIATION_FUTURE_DAYS`` (defaults: 7 and 30).
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    else:
        now = now.astimezone(UTC)
    today = now.date()
    past = max(0, int(os.getenv("EVENTOS_RECONCILIATION_PAST_DAYS", "7")))
    fut = max(0, int(os.getenv("EVENTOS_RECONCILIATION_FUTURE_DAYS", "30")))
    start_d = today - timedelta(days=past)
    end_d = today + timedelta(days=fut)
    return start_d.isoformat(), end_d.isoformat()
