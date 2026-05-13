"""Shared guards and validation for HTTP manual reconciliation starters."""

from __future__ import annotations

import os
from datetime import datetime

from .run_registry import GenericRunRegistry


def manual_reconciliation_enabled() -> bool:
    return str(os.getenv("ENABLE_MANUAL_RECONCILIATION_FUNCTIONS", "")).lower() in (
        "1",
        "true",
        "yes",
    )


def validate_iso_date_range(date_start: str, date_end: str) -> list[str]:
    errors: list[str] = []
    try:
        ds = datetime.fromisoformat(date_start)
        de = datetime.fromisoformat(date_end)
    except ValueError:
        errors.append("invalid_iso_date")
        return errors
    if ds.date() > de.date():
        errors.append("date_start_after_date_end")
    return errors


def validate_dates_with_target_year(
    *,
    target_year: int,
    date_start: str,
    date_end: str,
    allow_year_mismatch: bool,
) -> list[str]:
    errors = validate_iso_date_range(date_start, date_end)
    if errors:
        return errors
    ds = datetime.fromisoformat(date_start)
    de = datetime.fromisoformat(date_end)
    if not allow_year_mismatch:
        if ds.year != target_year or de.year != target_year:
            errors.append("dates_must_match_target_year_or_allow_year_mismatch")
    return errors


def validate_dates_no_target_year(date_start: str, date_end: str) -> list[str]:
    return validate_iso_date_range(date_start, date_end)


def registry_run_completed(registry: GenericRunRegistry, pipeline_run_id: str) -> bool:
    run = registry.get_run(pipeline_run_id) or {}
    return str(run.get("status", "")).upper() == "COMPLETED"
