"""Tests for eventos pipeline_run_id validation + queue body helpers."""

from __future__ import annotations

import base64
import json

from shared.eventos_pipeline_reset_helpers import (
    decode_queue_message_text,
    is_allowed_eventos_pipeline_run_id,
    message_matches_pipeline_run,
    safe_path_segment,
)


def test_microbatch_run_id_format_is_accepted() -> None:
    assert is_allowed_eventos_pipeline_run_id("eventos_microbatch_202605112230")


def test_reconciliation_run_id_format_is_accepted() -> None:
    assert is_allowed_eventos_pipeline_run_id("eventos_reconciliation_20260511")


def test_other_domain_run_ids_are_rejected() -> None:
    assert not is_allowed_eventos_pipeline_run_id(
        "ceap_reconciliation_20260511"
    )
    assert not is_allowed_eventos_pipeline_run_id(
        "votacoes_microbatch_202605112230"
    )
    assert not is_allowed_eventos_pipeline_run_id(
        "proposicoes_microbatch_202605112230"
    )


def test_invalid_run_ids_are_rejected() -> None:
    assert not is_allowed_eventos_pipeline_run_id("")
    assert not is_allowed_eventos_pipeline_run_id("eventos_microbatch_")
    assert not is_allowed_eventos_pipeline_run_id("eventos_microbatch_2026")
    assert not is_allowed_eventos_pipeline_run_id("eventos_other_20260511")


def test_safe_path_segment_replaces_unsafe_chars() -> None:
    assert safe_path_segment("eventos_microbatch_202605112230") == (
        "eventos_microbatch_202605112230"
    )
    assert safe_path_segment("../etc/passwd") == ".._etc_passwd"


def test_decode_queue_message_text_handles_plain_and_base64() -> None:
    payload = {"pipeline_run_id": "eventos_microbatch_202605112230"}
    plain = json.dumps(payload).encode("utf-8")
    assert json.loads(decode_queue_message_text(plain)) == payload
    b64 = base64.b64encode(plain)
    assert json.loads(decode_queue_message_text(b64)) == payload


def test_message_matches_pipeline_run_works_for_match_and_mismatch() -> None:
    body = json.dumps(
        {
            "domain": "eventos",
            "endpoint": "evento_pauta",
            "pipeline_run_id": "eventos_microbatch_202605112230",
            "payload": {"evento_id": "55"},
        }
    ).encode("utf-8")
    assert message_matches_pipeline_run(body, "eventos_microbatch_202605112230")
    assert not message_matches_pipeline_run(body, "eventos_microbatch_202605112000")
    assert not message_matches_pipeline_run(b"not json", "anything")
