"""Tests for ``shared.current_year_backfill_contract``."""

from __future__ import annotations

from datetime import UTC, datetime

from shared.current_year_backfill_contract import (
    current_year_backfill_run_id,
    is_current_year_backfill_run_id,
    merge_http_params_into_body,
    parse_current_year_backfill_body,
)


def test_run_id_format_matches_well_formed_shape() -> None:
    fixed = datetime(2026, 5, 13, 15, 30, 45, tzinfo=UTC)
    rid = current_year_backfill_run_id(now=fixed)
    assert rid == "current_year_backfill_20260513153045"
    assert is_current_year_backfill_run_id(rid)
    assert not is_current_year_backfill_run_id("current_year_backfill_20260513_153045")


def test_domains_required() -> None:
    now = datetime(2026, 5, 12, tzinfo=UTC)
    req, errs = parse_current_year_backfill_body({}, now=now)
    assert req is None
    assert "domains_required" in errs


def test_unknown_domains_400_list() -> None:
    now = datetime(2026, 5, 12, tzinfo=UTC)
    req, errs = parse_current_year_backfill_body(
        {"domains": ["proposicoes", "nope"], "dry_run": True},
        now=now,
    )
    assert req is None
    assert any(e.startswith("unknown_domains:") for e in errs)


def test_past_year_requires_force() -> None:
    now = datetime(2026, 5, 12, tzinfo=UTC)
    req, errs = parse_current_year_backfill_body(
        {"year": 2025, "domains": ["proposicoes"], "dry_run": True},
        now=now,
    )
    assert req is None
    assert "past_year_requires_force_true" in errs

    req2, errs2 = parse_current_year_backfill_body(
        {"year": 2025, "domains": ["proposicoes"], "dry_run": True, "force": True},
        now=now,
    )
    assert req2 is not None
    assert errs2 == []


def test_max_tasks_confirm_gate() -> None:
    now = datetime(2026, 5, 12, tzinfo=UTC)
    req, errs = parse_current_year_backfill_body(
        {
            "domains": ["proposicoes"],
            "dry_run": True,
            "max_tasks": 6000,
        },
        now=now,
    )
    assert req is None
    assert any("confirm_max_tasks" in e for e in errs)

    req2, errs2 = parse_current_year_backfill_body(
        {
            "domains": ["proposicoes"],
            "dry_run": True,
            "max_tasks": 6000,
            "confirm_max_tasks": True,
        },
        now=now,
    )
    assert req2 is not None
    assert errs2 == []


def test_merge_query_overrides_json() -> None:
    merged = merge_http_params_into_body(
        {"domains": ["proposicoes"], "max_tasks": 10},
        {"dry_run": "false", "max_tasks": "99"},
    )
    assert merged["domains"] == ["proposicoes"]
    assert merged["dry_run"] is False
    assert merged["max_tasks"] == 99


def test_date_order_validation() -> None:
    now = datetime(2026, 5, 12, tzinfo=UTC)
    req, errs = parse_current_year_backfill_body(
        {
            "domains": ["proposicoes"],
            "start_date": "2026-05-20",
            "end_date": "2026-05-01",
            "dry_run": True,
        },
        now=now,
    )
    assert req is None
    assert "date_start_after_date_end" in errs
