"""HTTP: controlled current-year API backfill (manual; no timer).

Historical data through 2025 is expected from static files; this endpoint
targets the **current calendar year** (default) from ``YYYY-01-01`` through
``end_date`` using existing domain queues and workers.

Initially implements **proposições** only; other domain keys are validated at
the API layer but rejected until handlers exist.

Security: ``authLevel=function`` + ``ENABLE_CURRENT_YEAR_BACKFILL_FUNCTION=true``.
"""

from __future__ import annotations

import json
import os
import traceback
from datetime import UTC, datetime

import azure.functions as func

from shared.current_year_backfill_contract import (
    IMPLEMENTED_DOMAINS,
    current_year_backfill_run_id,
    merge_http_params_into_body,
    parse_current_year_backfill_body,
    parse_request_json,
)
from shared.current_year_backfill_proposicoes import (
    execute_proposicoes_current_year_backfill,
)
from shared.logger import get_logger, log_structured

logger = get_logger()


def _json(body: dict[str, object], *, status: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body, ensure_ascii=False),
        status_code=status,
        mimetype="application/json",
    )


def _backfill_enabled() -> bool:
    return str(os.getenv("ENABLE_CURRENT_YEAR_BACKFILL_FUNCTION", "")).lower() in (
        "1",
        "true",
        "yes",
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    if not _backfill_enabled():
        return _json(
            {
                "error": (
                    "Current-year backfill is disabled. Set "
                    "ENABLE_CURRENT_YEAR_BACKFILL_FUNCTION=true."
                )
            },
            status=403,
        )

    raw_body = req.get_body().decode("utf-8", errors="replace")
    try:
        base = parse_request_json(raw_body)
    except ValueError as exc:
        return _json({"error": str(exc)}, status=400)

    merged = merge_http_params_into_body(base, dict(req.params or {}))
    now = datetime.now(UTC)
    parsed, errors = parse_current_year_backfill_body(merged, now=now)
    if errors:
        return _json({"error": "validation_failed", "details": errors}, status=400)
    assert parsed is not None

    run_id = current_year_backfill_run_id(now=now)
    domains_out: dict[str, object] = {}
    agg_seen = 0
    agg_enq = 0
    agg_skip = 0
    any_limit = False

    for dom in parsed.domains:
        if dom not in IMPLEMENTED_DOMAINS:
            domains_out[dom] = {
                "status": "NOT_IMPLEMENTED",
                "detail": "handler not available in this release",
            }
            continue
        try:
            if dom == "proposicoes":
                out = execute_proposicoes_current_year_backfill(
                    request=parsed,
                    pipeline_run_id=run_id,
                    now=now,
                )
                domains_out[dom] = out
                agg_seen += int(out.get("records_seen", 0))
                agg_enq += int(out.get("messages_enqueued", 0))
                agg_skip += int(out.get("records_skipped_same_hash", 0))
                if out.get("limit_reached"):
                    any_limit = True
        except Exception as exc:  # noqa: BLE001
            domains_out[dom] = {
                "status": "FAILED",
                "error": str(exc)[:500],
                "error_type": type(exc).__name__,
            }
            log_structured(
                logger,
                "warning",
                "current_year_backfill domain failed",
                run_id=run_id,
                domain=dom,
                error=str(exc)[:500],
                error_type=type(exc).__name__,
                traceback=traceback.format_exc()[:2000],
            )

    has_fail = any(
        isinstance(v, dict) and str(v.get("status", "")).upper() == "FAILED"
        for v in domains_out.values()
    )
    has_not_impl = any(
        isinstance(v, dict) and str(v.get("status", "")).upper() == "NOT_IMPLEMENTED"
        for v in domains_out.values()
    )
    if has_fail or has_not_impl:
        overall = "PARTIAL_FAILURE"
    elif any_limit:
        overall = "LIMIT_REACHED"
    elif parsed.dry_run:
        overall = "DRY_RUN"
    else:
        overall = "SUCCESS"

    http_status = 207 if overall == "PARTIAL_FAILURE" else 200

    log_structured(
        logger,
        "info",
        "current_year_backfill completed",
        run_id=run_id,
        domains=list(parsed.domains),
        messages_enqueued=agg_enq,
        records_skipped_same_hash=agg_skip,
        dry_run=parsed.dry_run,
        overall_status=overall,
    )

    return _json(
        {
            "status": overall,
            "run_id": run_id,
            "year": parsed.year,
            "window_start": parsed.start_date,
            "window_end": parsed.end_date,
            "dry_run": parsed.dry_run,
            "force": parsed.force,
            "max_tasks": parsed.max_tasks,
            "summary": {
                "domains_requested": len(parsed.domains),
                "records_seen": agg_seen,
                "messages_enqueued": agg_enq,
                "records_skipped_same_hash": agg_skip,
                "records_failed": 0,
            },
            "domains": domains_out,
        },
        status=http_status,
    )
