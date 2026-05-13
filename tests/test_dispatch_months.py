"""Tests for shared.dispatch_months (CEAP month windows)."""

from __future__ import annotations

from datetime import datetime, timezone

from shared.dispatch_months import (
    months_daily_moving_window,
    months_reconciliation_current_and_previous,
)


def test_reconciliation_two_months_mid_year() -> None:
    now = datetime(2026, 5, 12, tzinfo=timezone.utc)
    assert months_reconciliation_current_and_previous(target_year=2026, now=now) == [
        4,
        5,
    ]


def test_reconciliation_two_months_january_only_current() -> None:
    now = datetime(2026, 1, 15, tzinfo=timezone.utc)
    assert months_reconciliation_current_and_previous(target_year=2026, now=now) == [1]


def test_daily_lookback_zero_is_current_month_only() -> None:
    now = datetime(2026, 5, 12, tzinfo=timezone.utc)
    assert months_daily_moving_window(target_year=2026, now=now, lookback_months=0) == [
        5,
    ]
