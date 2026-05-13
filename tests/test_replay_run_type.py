"""Tests for shared.replay_run_type."""

from __future__ import annotations

from shared.replay_run_type import infer_run_type_for_requeued_work


def test_reconciliation_run_id() -> None:
    assert (
        infer_run_type_for_requeued_work("proposicoes_reconciliation_20260511")
        == "reconciliation"
    )


def test_microbatch_run_id() -> None:
    assert (
        infer_run_type_for_requeued_work("eventos_microbatch_202605121200")
        == "microbatch"
    )


def test_daily_run_id() -> None:
    assert infer_run_type_for_requeued_work("eventos_daily_20260512") == "daily"


def test_replay_and_other_ids_default_to_manual() -> None:
    assert infer_run_type_for_requeued_work("proposicoes_replay_20260512") == "manual_replay"
    assert infer_run_type_for_requeued_work("") == "manual_replay"
