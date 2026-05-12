"""Tests for ``shared.queue_messages.DomainWorkMessage``."""

from __future__ import annotations

import json

import pytest

from shared.queue_messages import DomainWorkMessage


def test_round_trip_minimal_message() -> None:
    wm = DomainWorkMessage(
        domain="reference",
        endpoint="partidos",
        pipeline_run_id="reference_snapshot_20260511",
        run_type="snapshot",
        payload={"reference_date": "2026-05-11"},
        execution_id="exec-1",
        dispatched_at="2026-05-11T00:00:00+00:00",
    )
    body = wm.to_json()
    parsed = DomainWorkMessage.from_queue_body(body.encode("utf-8"))
    assert parsed == wm


def test_to_json_fills_dispatched_at() -> None:
    wm = DomainWorkMessage(
        domain="reference",
        endpoint="orgaos",
        pipeline_run_id="reference_snapshot_20260511",
        run_type="snapshot",
        payload={"reference_date": "2026-05-11"},
    )
    body = wm.to_json()
    decoded = json.loads(body)
    assert decoded["dispatched_at"]


def test_from_queue_body_rejects_non_dict_payload() -> None:
    body = json.dumps(
        {
            "domain": "reference",
            "endpoint": "partidos",
            "pipeline_run_id": "reference_snapshot_20260511",
            "run_type": "snapshot",
            "payload": "not-a-dict",
        }
    ).encode("utf-8")
    with pytest.raises(ValueError):
        DomainWorkMessage.from_queue_body(body)


def test_matches_pipeline_run_id() -> None:
    wm = DomainWorkMessage(
        domain="reference",
        endpoint="partidos",
        pipeline_run_id="reference_snapshot_20260511",
    )
    assert wm.matches_pipeline_run_id("reference_snapshot_20260511")
    assert not wm.matches_pipeline_run_id("reference_snapshot_20260512")
