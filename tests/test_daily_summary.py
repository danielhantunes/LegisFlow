"""Tests for ``shared.daily_summary_types`` rollup helpers."""

from __future__ import annotations

from shared.daily_summary_types import (
    RunInspection,
    daily_summary_json_path,
    parse_expected_domains,
    rollup_daily_status,
    rollup_domain_status,
)


def test_daily_summary_json_path() -> None:
    assert daily_summary_json_path("2026-05-11").endswith(
        "reference_date=2026-05-11/daily_summary.json"
    )


def test_parse_expected_domains_explicit() -> None:
    assert parse_expected_domains("ceap,votacoes") == ["ceap", "votacoes"]


def test_parse_expected_domains_default_includes_alias() -> None:
    d = parse_expected_domains("")
    assert "reference_snapshot" in d


def test_rollup_domain_status_empty() -> None:
    assert rollup_domain_status([]) == "NOT_FOUND"


def test_rollup_domain_status_all_completed() -> None:
    rows = [
        RunInspection(
            domain="x",
            entity="e",
            pipeline_run_id="a",
            metadata_path="m",
            success_marker_path="s",
            status="COMPLETED",
        ),
        RunInspection(
            domain="x",
            entity="e",
            pipeline_run_id="b",
            metadata_path="m2",
            success_marker_path="s2",
            status="NO_DATA",
        ),
    ]
    assert rollup_domain_status(rows) == "COMPLETED"


def test_rollup_domain_status_failed_wins() -> None:
    rows = [
        RunInspection(
            domain="x",
            entity="e",
            pipeline_run_id="a",
            metadata_path="m",
            success_marker_path="s",
            status="COMPLETED",
        ),
        RunInspection(
            domain="x",
            entity="e",
            pipeline_run_id="b",
            metadata_path="m2",
            success_marker_path="s2",
            status="FAILED",
        ),
    ]
    assert rollup_domain_status(rows) == "FAILED"


def test_rollup_daily_status_ignores_not_implemented() -> None:
    ds = {
        "ceap": "COMPLETED",
        "legacy": "NOT_IMPLEMENTED",
    }
    assert rollup_daily_status(ds, expected=["ceap", "legacy"]) == "COMPLETED"


def test_rollup_daily_status_not_found_partial() -> None:
    ds = {"ceap": "COMPLETED", "votacoes": "NOT_FOUND"}
    assert rollup_daily_status(ds, expected=["ceap", "votacoes"]) == "PARTIAL"
