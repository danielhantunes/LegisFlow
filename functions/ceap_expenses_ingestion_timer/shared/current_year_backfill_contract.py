"""HTTP contract + validation for manual current-year API backfill.

Historical years (<= 2025) are expected to be loaded from static files; this
flow targets **the configured calendar year** (default: UTC current year)
from ``YYYY-01-01`` through ``end_date`` using existing domain queues and
workers. See module docstring on ``fn_current_year_backfill_dispatcher``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

# Domains with a concrete handler in ``shared.current_year_backfill_*``.
IMPLEMENTED_DOMAINS: frozenset[str] = frozenset({"proposicoes"})

# Domains accepted on the wire (others return 400). Expand as handlers land.
SUPPORTED_DOMAIN_KEYS: frozenset[str] = frozenset(
    {"proposicoes", "votacoes", "eventos", "discursos", "ceap"}
)

_DEFAULT_MAX_TASKS = 1000
_ABSOLUTE_MAX_TASKS = 100_000
_CONFIRM_REQUIRED_ABOVE = 5000

_RUN_ID_RE = re.compile(r"^current_year_backfill_\d{14}$")


def current_year_backfill_run_id(*, now: datetime) -> str:
    """``current_year_backfill_YYYYMMDDHHMMSS`` (UTC, 14-digit timestamp)."""
    n = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    return f"current_year_backfill_{n:%Y%m%d%H%M%S}"


def is_current_year_backfill_run_id(pipeline_run_id: str) -> bool:
    return bool(_RUN_ID_RE.match((pipeline_run_id or "").strip()))


@dataclass
class CurrentYearBackfillRequest:
    year: int
    start_date: str
    end_date: str
    domains: tuple[str, ...]
    dry_run: bool
    force: bool
    max_tasks: int
    confirm_max_tasks: bool
    raw: dict[str, Any] = field(default_factory=dict)


def _boolish(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off", ""):
        return False
    return default


def _parse_domains(raw: Any) -> tuple[str, ...] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
        return tuple(parts) if parts else None
    if isinstance(raw, list):
        out: list[str] = []
        for x in raw:
            s = str(x).strip().lower()
            if s:
                out.append(s)
        return tuple(out) if out else None
    return None


def parse_current_year_backfill_body(
    body: dict[str, Any],
    *,
    now: datetime,
) -> tuple[CurrentYearBackfillRequest | None, list[str]]:
    """Parse and validate JSON body. Returns ``(request, errors)``."""
    errors: list[str] = []
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    cal_year = now_utc.year

    year_raw = body.get("year")
    if year_raw is None or year_raw == "":
        year = cal_year
    else:
        try:
            year = int(year_raw)
        except (TypeError, ValueError):
            errors.append("year_must_be_integer")
            year = cal_year

    if year > cal_year:
        errors.append("year_cannot_exceed_current_calendar_year")
    if year < cal_year:
        if not _boolish(body.get("force"), False):
            errors.append("past_year_requires_force_true")

    start_raw = str(body.get("start_date", "") or "").strip()
    end_raw = str(body.get("end_date", "") or "").strip()
    start_date = start_raw or f"{year}-01-01"
    end_date = end_raw or now_utc.date().isoformat()

    try:
        ds = datetime.fromisoformat(start_date).date()
        de = datetime.fromisoformat(end_date).date()
    except ValueError:
        errors.append("invalid_start_or_end_date_iso")
        return None, errors

    if ds > de:
        errors.append("date_start_after_date_end")

    if ds.year < year or de.year > year:
        if not _boolish(body.get("force"), False):
            errors.append("window_outside_year_requires_force_true")

    dom = _parse_domains(body.get("domains"))
    if dom is None:
        errors.append("domains_required")
    else:
        bad = [d for d in dom if d not in SUPPORTED_DOMAIN_KEYS]
        if bad:
            errors.append(f"unknown_domains:{','.join(bad)}")

    dry_run = _boolish(body.get("dry_run"), default=True)
    force = _boolish(body.get("force"), default=False)

    try:
        max_tasks = int(body.get("max_tasks", _DEFAULT_MAX_TASKS))
    except (TypeError, ValueError):
        errors.append("max_tasks_must_be_integer")
        max_tasks = _DEFAULT_MAX_TASKS

    if max_tasks < 1:
        errors.append("max_tasks_must_be_at_least_1")
    if max_tasks > _ABSOLUTE_MAX_TASKS:
        errors.append(f"max_tasks_cannot_exceed_{_ABSOLUTE_MAX_TASKS}")
    confirm_max = _boolish(body.get("confirm_max_tasks"), False)
    if max_tasks > _CONFIRM_REQUIRED_ABOVE and not confirm_max:
        errors.append(
            f"max_tasks_above_{_CONFIRM_REQUIRED_ABOVE}_requires_confirm_max_tasks_true"
        )

    if errors:
        return None, errors

    assert dom is not None
    return (
        CurrentYearBackfillRequest(
            year=year,
            start_date=start_date,
            end_date=end_date,
            domains=dom,
            dry_run=dry_run,
            force=force,
            max_tasks=max_tasks,
            confirm_max_tasks=confirm_max,
            raw=dict(body),
        ),
        [],
    )


def merge_query_into_body(req_json: dict[str, Any], query: str) -> dict[str, Any]:
    """Shallow merge: query string overrides keys when present (JSON first)."""
    out = dict(req_json)
    if not query:
        return out
    for pair in query.split("&"):
        if "=" not in pair:
            continue
        k, _, v = pair.partition("=")
        k = k.strip()
        if not k:
            continue
        v_dec = v.strip().replace("+", " ")
        if k == "domains":
            out[k] = [p for p in v_dec.split(",") if p.strip()]
        elif k in ("dry_run", "force", "confirm_max_tasks"):
            out[k] = v_dec.lower() in ("1", "true", "yes")
        elif k in ("year", "max_tasks"):
            try:
                out[k] = int(v_dec)
            except ValueError:
                out[k] = v_dec
        else:
            out[k] = v_dec
    return out


def merge_http_params_into_body(
    req_json: dict[str, Any], params: dict[str, Any] | None
) -> dict[str, Any]:
    """Merge ``func.HttpRequest.params`` (query) into JSON body."""
    out = dict(req_json)
    if not params:
        return out
    for k_raw, v_raw in params.items():
        k = str(k_raw).strip()
        if not k:
            continue
        v = v_raw[0] if isinstance(v_raw, list) and v_raw else v_raw
        v_str = str(v).strip()
        if k == "domains":
            out[k] = [p for p in v_str.split(",") if p.strip()]
        elif k in ("dry_run", "force", "confirm_max_tasks"):
            out[k] = v_str.lower() in ("1", "true", "yes")
        elif k in ("year", "max_tasks"):
            try:
                out[k] = int(v_str)
            except ValueError:
                out[k] = v_str
        else:
            out[k] = v_str
    return out


def parse_request_json(raw_body: str) -> dict[str, Any]:
    if not raw_body.strip():
        return {}
    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_json_body") from exc
    if not isinstance(data, dict):
        raise ValueError("json_body_must_be_object")
    return data
