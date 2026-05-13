"""Tests for shared.proposicoes_pipeline_reset_helpers (pure helpers)."""

from __future__ import annotations

import base64
import json

from shared.proposicoes_pipeline_reset_helpers import (
    decode_queue_message_text,
    is_allowed_proposicoes_pipeline_run_id,
    message_matches_pipeline_run,
    safe_path_segment,
)


def test_daily_pipeline_run_id_accepted() -> None:
    assert is_allowed_proposicoes_pipeline_run_id("proposicoes_daily_20260511")


def test_microbatch_pipeline_run_id_accepted() -> None:
    assert is_allowed_proposicoes_pipeline_run_id(
        "proposicoes_microbatch_202605112230"
    )


def test_reconciliation_pipeline_run_id_accepted() -> None:
    assert is_allowed_proposicoes_pipeline_run_id(
        "proposicoes_reconciliation_20260511"
    )


def test_disallowed_pipeline_run_ids() -> None:
    assert not is_allowed_proposicoes_pipeline_run_id("")
    assert not is_allowed_proposicoes_pipeline_run_id("votacoes_microbatch_202605112230")
    assert not is_allowed_proposicoes_pipeline_run_id("ceap_daily_20260511")
    assert not is_allowed_proposicoes_pipeline_run_id(
        "proposicoes_microbatch_2026"
    )
    assert not is_allowed_proposicoes_pipeline_run_id(
        "proposicoes_reconciliation_2026"
    )
    assert not is_allowed_proposicoes_pipeline_run_id(
        "proposicoes_microbatch_202605112230_extra"
    )


def test_safe_path_segment_strips_special_chars() -> None:
    assert safe_path_segment("proposicoes_reconciliation_20260511") == (
        "proposicoes_reconciliation_20260511"
    )
    assert safe_path_segment("a/b\\c d") == "a_b_c_d"


def test_decode_queue_message_text_handles_base64_and_raw() -> None:
    payload = json.dumps(
        {"pipeline_run_id": "proposicoes_microbatch_202605112230"}
    )
    raw = payload.encode("utf-8")
    b64 = base64.b64encode(raw)
    assert decode_queue_message_text(raw) == payload
    assert decode_queue_message_text(b64) == payload
    assert decode_queue_message_text(b"") == ""


def test_message_matches_pipeline_run() -> None:
    body = json.dumps(
        {
            "pipeline_run_id": "proposicoes_microbatch_202605112230",
            "endpoint": "proposicao_tramitacoes",
            "payload": {"proposicao_id": "42"},
        }
    ).encode("utf-8")
    assert message_matches_pipeline_run(
        body, "proposicoes_microbatch_202605112230"
    )
    assert not message_matches_pipeline_run(
        body, "proposicoes_microbatch_202605112240"
    )
    assert not message_matches_pipeline_run(b"not json", "anything")
