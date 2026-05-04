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
