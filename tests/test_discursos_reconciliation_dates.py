"""Tests for :mod:`shared.discursos_reconciliation_dates`."""

from __future__ import annotations

from datetime import UTC, datetime

from shared.discursos_reconciliation_dates import default_discursos_reconciliation_window


def test_default_discursos_reconciliation_window_spans_prev_month_through_today() -> None:
    now = datetime(2026, 5, 12, 15, 0, tzinfo=UTC)
    start, end = default_discursos_reconciliation_window(now=now)
    assert start == "2026-04-01"
    assert end == "2026-05-12"


def test_default_discursos_reconciliation_window_first_of_month() -> None:
    now = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    start, end = default_discursos_reconciliation_window(now=now)
    assert start == "2026-02-01"
    assert end == "2026-03-01"


def test_default_discursos_reconciliation_window_naive_now_is_treated_as_utc() -> None:
    now = datetime(2026, 1, 15, 12, 0)
    start, end = default_discursos_reconciliation_window(now=now)
    assert start == "2025-12-01"
    assert end == "2026-01-15"
