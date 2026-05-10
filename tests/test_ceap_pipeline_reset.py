"""Unit tests for CEAP pipeline reset helpers (no Azure I/O)."""

from __future__ import annotations

import base64
import json

import pytest

from shared.ceap_pipeline_reset_helpers import (
    is_allowed_pipeline_run_id,
    message_matches_pipeline_run,
)


@pytest.mark.parametrize(
    "pid,ok",
    [
        ("ceap_daily_20260510", True),
        ("ceap_reconciliation_20260525", True),
        ("ceap_daily_2026051", False),
        ("ceap_replay_20260510", False),
        ("", False),
        ("ceap_daily_20260510_extra", False),
    ],
)
def test_is_allowed_pipeline_run_id(pid: str, ok: bool) -> None:
    assert is_allowed_pipeline_run_id(pid) is ok


def test_message_matches_plain_json() -> None:
    body = json.dumps(
        {"pipeline_run_id": "ceap_daily_20260510", "id_deputado": 1}
    ).encode()
    assert message_matches_pipeline_run(body, "ceap_daily_20260510") is True
    assert message_matches_pipeline_run(body, "ceap_daily_20260511") is False


def test_message_matches_base64_json() -> None:
    inner = json.dumps(
        {"pipeline_run_id": "ceap_daily_20260510", "id_deputado": 1}
    ).encode()
    body = base64.b64encode(inner)
    assert message_matches_pipeline_run(body, "ceap_daily_20260510") is True
