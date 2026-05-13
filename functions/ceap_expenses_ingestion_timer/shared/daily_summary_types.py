"""Pure helpers for daily ingestion summary (no Azure SDK imports)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from zoneinfo import ZoneInfo

DAILY_SUMMARY_BASE = "raw/camara/_metadata/daily_summary"
METADATA_VERSION = "1.0"

EXPECTED_DOMAIN_ALIASES: dict[str, str] = {
    "reference_snapshot": "reference",
}

CRITICAL_WARNING_TYPES: frozenset[str] = frozenset(
    {
        "success_marker_missing",
        "task_counts_inconsistent",
        "success_marker_exists_but_status_not_completed",
    }
)


def daily_summary_json_path(reference_date: str) -> str:
    return f"{DAILY_SUMMARY_BASE}/reference_date={reference_date}/daily_summary.json"


def daily_summary_success_path(reference_date: str) -> str:
    return f"{DAILY_SUMMARY_BASE}/reference_date={reference_date}/_SUCCESS"


def compact_yyyymmdd(iso_date: str) -> str:
    return (iso_date or "").replace("-", "").strip()


def resolve_reference_date_string(*, tz_name: str, now_utc: datetime) -> str:
    try:
        return datetime.now(ZoneInfo(tz_name)).date().isoformat()
    except Exception:
        return now_utc.date().isoformat()


def canonical_domain_name(expected_name: str) -> str:
    return EXPECTED_DOMAIN_ALIASES.get(expected_name.strip(), expected_name.strip())


def parse_expected_domains(raw: str) -> list[str]:
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    if parts:
        return parts
    return [
        "ceap",
        "reference_snapshot",
        "votacoes",
        "proposicoes",
        "eventos",
        "institucional",
        "discursos",
    ]


def int_field(meta: dict[str, Any], key: str) -> int:
    try:
        return int(meta.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def normalize_run_status(raw: str) -> str:
    s = str(raw or "").strip().upper()
    if s in {"PARTIALLY_COMPLETED"}:
        return "PARTIAL"
    return s


@dataclass
class RunInspection:
    domain: str
    entity: str
    pipeline_run_id: str
    metadata_path: str
    success_marker_path: str
    status: str = "NOT_FOUND"
    run_type: str = ""
    started_at: str = ""
    completed_at: str = ""
    total_tasks_expected: int = 0
    total_tasks_success: int = 0
    total_tasks_failed: int = 0
    total_tasks_pending: int = 0
    total_tasks_poison: int = 0
    total_tasks_running: int = 0
    total_tasks_queued: int = 0
    total_raw_files_written: int | None = None
    total_records_collected: int | None = None
    warnings: list[dict[str, Any]] = field(default_factory=list)
    control_row: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


def append_run_warning(
    insp: RunInspection,
    *,
    warning_type: str,
    message: str,
) -> None:
    insp.warnings.append(
        {
            "domain": insp.domain,
            "pipeline_run_id": insp.pipeline_run_id,
            "warning_type": warning_type,
            "message": message,
        }
    )


def rollup_domain_status(run_rows: list[RunInspection]) -> str:
    if not run_rows:
        return "NOT_FOUND"
    statuses = [r.status for r in run_rows]
    if any(s == "FAILED" for s in statuses):
        return "FAILED"
    if any(s in {"RUNNING", "QUEUED", "QUEUING"} for s in statuses):
        return "RUNNING"
    if any(s == "PARTIAL" for s in statuses):
        return "PARTIAL"
    if any(s == "INVALID_METADATA" for s in statuses):
        return "FAILED"
    if all(s == "NOT_FOUND" for s in statuses):
        return "NOT_FOUND"
    if all(s in {"COMPLETED", "NO_DATA"} for s in statuses):
        if all(s == "NO_DATA" for s in statuses):
            return "NO_DATA"
        return "COMPLETED"
    return "PARTIAL"


def rollup_daily_status(domain_statuses: dict[str, str], *, expected: list[str]) -> str:
    stats = [
        domain_statuses.get(d, "NOT_FOUND")
        for d in expected
        if domain_statuses.get(d, "NOT_FOUND") != "NOT_IMPLEMENTED"
    ]
    if not stats:
        return "PARTIAL"
    if any(s == "FAILED" for s in stats):
        return "FAILED"
    if any(s in {"RUNNING", "QUEUED", "QUEUING"} for s in stats):
        return "RUNNING"
    if any(s in {"PARTIAL", "NOT_FOUND"} for s in stats):
        return "PARTIAL"
    if all(s in {"COMPLETED", "NO_DATA"} for s in stats):
        return "COMPLETED"
    return "PARTIAL"
