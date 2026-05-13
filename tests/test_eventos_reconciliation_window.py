"""Tests for eventos reconciliation default date window."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import patch

from shared.eventos_reconciliation_dates import default_eventos_reconciliation_window


def test_default_eventos_reconciliation_window_respects_env() -> None:
    now = datetime(2026, 5, 12, 12, 0, 0, tzinfo=UTC)
    with patch.dict(
        os.environ,
        {"EVENTOS_RECONCILIATION_PAST_DAYS": "2", "EVENTOS_RECONCILIATION_FUTURE_DAYS": "3"},
        clear=False,
    ):
        ds, de = default_eventos_reconciliation_window(now=now)
    assert ds == "2026-05-10"
    assert de == "2026-05-15"
