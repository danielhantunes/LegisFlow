"""Pure date helpers for discursos weekly reconciliation (no Azure SDK imports)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def default_discursos_reconciliation_window(*, now: datetime) -> tuple[str, str]:
    """Inclusive API date range: first day of **previous** calendar month through **today** (UTC).

    Matches the proposições reconciliation calendar span (prev month + current month to date).
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    else:
        now = now.astimezone(UTC)
    today = now.date()
    first_this_month = today.replace(day=1)
    last_day_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_day_prev_month.replace(day=1)
    return first_prev_month.isoformat(), today.isoformat()
