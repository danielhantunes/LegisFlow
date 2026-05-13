"""Month window and ordering for CEAP API dispatch (no future months in current year)."""

from __future__ import annotations

from datetime import datetime, timezone


def max_dispatch_month(*, target_year: int, now: datetime | None = None) -> int:
    """
    Last calendar month to enqueue for target_year (1..12), inclusive.

    - target_year > current year: 0 (nothing to dispatch yet).
    - target_year == current year: current month (no future months).
    - target_year < current year: 12 (full historical year).
    """
    now = now or datetime.now(timezone.utc)
    cy, cm = now.year, now.month
    if target_year > cy:
        return 0
    if target_year < cy:
        return 12
    return cm


def dispatch_month_order(*, target_year: int, now: datetime | None = None) -> list[int]:
    """
    Months to visit for target_year, in dispatch order.

    For the current calendar year: prefer current month, then previous month (same year),
    then remaining months from January up to max_dispatch_month (excluding duplicates).
    For past years: linear 1..12.
    """
    now = now or datetime.now(timezone.utc)
    max_m = max_dispatch_month(target_year=target_year, now=now)
    if max_m < 1:
        return []

    if target_year < now.year:
        return list(range(1, max_m + 1))

    # target_year == now.year
    cm = now.month
    preferred: list[int] = []
    if cm <= max_m:
        preferred.append(cm)
        if cm > 1:
            prev_same_year = cm - 1
            if prev_same_year >= 1 and prev_same_year <= max_m:
                preferred.append(prev_same_year)
    rest = [m for m in range(1, max_m + 1) if m not in preferred]
    return preferred + rest


def months_daily_moving_window(
    *, target_year: int, now: datetime, lookback_months: int
) -> list[int]:
    """
    Daily mode: "current" month within target_year plus lookback, never beyond max_dispatch_month.

    - Same calendar year as target_year: anchor at today's month.
    - Target year in the past (e.g. 2025 while clock is 2026): anchor at last dispatchable
      month for that year (typically December) so the window still moves along the year tail.
    """
    max_m = max_dispatch_month(target_year=target_year, now=now)
    if max_m < 1:
        return []
    if target_year > now.year:
        return []
    if target_year == now.year:
        cm = now.month
    else:
        cm = max_m
    out: list[int] = []
    for i in range(lookback_months + 1):
        m = cm - i
        if 1 <= m <= max_m:
            out.append(m)
    return out


def months_reconciliation_window(
    *, target_year: int, now: datetime, start_month: int
) -> list[int]:
    """Reconciliation: January (or start_month) through max_dispatch_month."""
    max_m = max_dispatch_month(target_year=target_year, now=now)
    if max_m < 1:
        return []
    sm = max(1, min(12, int(start_month)))
    return list(range(sm, max_m + 1))


def months_reconciliation_current_and_previous(
    *, target_year: int, now: datetime
) -> list[int]:
    """Reconciliation: **previous calendar month + current month** within ``target_year``.

    Used for low-cost weekly-style CEAP reconciliation (same legislative year only).
    If ``now.month == 1``, only January is returned (no month 0 in the same year).
    Months are returned **ascending** (older first) for stable processing order.
    """
    max_m = max_dispatch_month(target_year=target_year, now=now)
    if max_m < 1:
        return []
    if target_year > now.year:
        return []
    if target_year < now.year:
        # Historical closed year: still only last two "active" months at year end.
        cm = 12
        prev_m = cm - 1
        return [m for m in (prev_m, cm) if 1 <= m <= max_m]

    cm = min(now.month, max_m)
    prev_m = cm - 1
    out: list[int] = []
    if prev_m >= 1:
        out.append(prev_m)
    out.append(cm)
    return [m for m in out if 1 <= m <= max_m]
