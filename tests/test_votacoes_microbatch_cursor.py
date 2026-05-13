"""Tests for votacoes microbatch cursor helpers."""

from __future__ import annotations

from shared.votacoes_microbatch_cursor import votacao_id_sort_key


def test_votacao_id_sort_key_orders_numeric_ids_numerically() -> None:
    ids = ["100", "9", "20", "25001234"]
    assert sorted(ids, key=votacao_id_sort_key) == ["9", "20", "100", "25001234"]


def test_votacao_id_sort_key_non_numeric_after_numeric() -> None:
    ids = ["2", "x-1", "10"]
    out = sorted(ids, key=votacao_id_sort_key)
    assert out[0] == "2" and out[1] == "10" and out[2] == "x-1"
