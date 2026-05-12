"""Tests for reference reset helpers (no Azure SDK dependency)."""

from __future__ import annotations

from shared.reference_pipeline_reset_helpers import (
    is_allowed_reference_pipeline_run_id,
    message_matches_pipeline_run,
    safe_path_segment,
)


def test_is_allowed_reference_pipeline_run_id_valid() -> None:
    assert is_allowed_reference_pipeline_run_id("reference_snapshot_20260511")


def test_is_allowed_reference_pipeline_run_id_rejects_other_domains() -> None:
    assert not is_allowed_reference_pipeline_run_id("ceap_daily_20260511")
    assert not is_allowed_reference_pipeline_run_id("votacoes_microbatch_20260511")
    assert not is_allowed_reference_pipeline_run_id("")
    assert not is_allowed_reference_pipeline_run_id("reference_snapshot_2026")


def test_safe_path_segment_replaces_unsafe_chars() -> None:
    assert safe_path_segment("a/b c?") == "a_b_c_"
    assert safe_path_segment("ok-9.0_x") == "ok-9.0_x"


def test_message_matches_pipeline_run_with_plain_json() -> None:
    body = b'{"pipeline_run_id":"reference_snapshot_20260511","domain":"reference"}'
    assert message_matches_pipeline_run(body, "reference_snapshot_20260511")
    assert not message_matches_pipeline_run(body, "reference_snapshot_20260512")


def test_message_matches_pipeline_run_with_base64_body() -> None:
    import base64

    raw = b'{"pipeline_run_id":"reference_snapshot_20260511"}'
    encoded = base64.b64encode(raw)
    assert message_matches_pipeline_run(encoded, "reference_snapshot_20260511")


def test_message_matches_pipeline_run_returns_false_on_garbage() -> None:
    assert not message_matches_pipeline_run(b"\xffnot json", "x")
