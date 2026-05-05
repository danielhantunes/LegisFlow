"""Timer: CEAP API dispatcher — daily moving window vs monthly reconciliation (day 25)."""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import azure.functions as func

from shared.api_client import CamaraApiClient
from shared.ceap_partition_state import CeapPartitionStateStore
from shared.ceap_run_registry import CeapRunRegistry
from shared.dispatch_months import (
    max_dispatch_month,
    months_daily_moving_window,
    months_reconciliation_window,
)
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.work_message import CeapApiWorkMessage

logger = get_logger()

# After this many consecutive ticks with zero new messages, treat enqueue phase as complete.
_IDLE_TICKS_ENQUEUE_DONE = 3


def _deputados_total_hint(payload: dict[str, Any] | None, itens_per_page: int = 100) -> int | None:
    if not payload:
        return None
    dados = payload.get("dados") or []
    for link in payload.get("links") or []:
        if link.get("rel") == "last":
            href = str(link.get("href", ""))
            m = re.search(r"pagina=(\d+)", href, re.I)
            if m:
                last_page = int(m.group(1))
                return (last_page - 1) * itens_per_page + len(dados)
    return None


def _parse_iso_utc(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _partition_enqueue_action(
    *,
    part: dict[str, Any] | None,
    pipeline_run_id: str,
    now: datetime,
    stale_after_minutes: int,
) -> str:
    """
    Returns one of:
      - enqueue_new
      - enqueue_reprocess
      - enqueue_stale_queued
      - enqueue_stale_running
      - skip_success_same_run
      - skip_queued
      - skip_running
    """
    if part is None:
        return "enqueue_new"

    st = str(part.get("status", "")).upper()
    cur = str(part.get("current_pipeline_run_id", ""))
    stale_before = now - timedelta(minutes=stale_after_minutes)

    if st in ("PENDING", "FAILED", "POISON", "STALE"):
        return "enqueue_reprocess"

    if cur == pipeline_run_id and st == "SUCCESS":
        return "skip_success_same_run"

    if st == "QUEUED":
        dispatched = _parse_iso_utc(part.get("last_dispatched_at"))
        if dispatched is not None and dispatched < stale_before:
            return "enqueue_stale_queued"
        return "skip_queued"

    if st == "RUNNING":
        started = _parse_iso_utc(part.get("last_started_at"))
        if started is not None and started < stale_before:
            return "enqueue_stale_running"
        return "skip_running"

    # For unknown/empty states, prefer re-enqueue.
    return "enqueue_reprocess"


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    now = datetime.now(UTC)
    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.environ["CEAP_API_QUEUE_NAME"]

    target_year = int(os.getenv("CEAP_TARGET_YEAR", os.getenv("CEAP_API_YEAR", "2026")))
    recon_day = int(os.getenv("CEAP_RECONCILIATION_DAY", "25"))
    lookback = int(os.getenv("CEAP_DAILY_LOOKBACK_MONTHS", "1"))
    start_month = int(os.getenv("CEAP_RECONCILIATION_START_MONTH", "1"))
    stale_after_minutes = int(os.getenv("CEAP_STALE_AFTER_MINUTES", "60"))
    max_per_tick = int(
        os.getenv("CEAP_MAX_TASKS_PER_DISPATCH", os.getenv("CEAP_DISPATCH_MAX_MESSAGES", "1000"))
    )

    max_m = max_dispatch_month(target_year=target_year, now=now)

    if target_year > now.year:
        log_structured(
            logger,
            "warning",
            "CEAP_TARGET_YEAR is in the future; nothing to enqueue.",
            target_year=target_year,
            now_year=now.year,
            now_month=now.month,
        )
        return

    is_reconciliation = now.day == recon_day
    mode = "reconciliation" if is_reconciliation else "daily"
    if is_reconciliation:
        pipeline_run_id = f"ceap_reconciliation_{now:%Y%m%d}"
        month_list = months_reconciliation_window(
            target_year=target_year, now=now, start_month=start_month
        )
    else:
        pipeline_run_id = f"ceap_daily_{now:%Y%m%d}"
        month_list = months_daily_moving_window(
            target_year=target_year, now=now, lookback_months=lookback
        )

    month_list = [m for m in month_list if m <= max_m]
    if not month_list:
        log_structured(
            logger,
            "info",
            "CEAP dispatch: no months in window.",
            mode=mode,
            pipeline_run_id=pipeline_run_id,
            target_year=target_year,
            max_dispatch_month=max_m,
        )
        return

    registry = CeapRunRegistry.from_connection_string(conn, control_table)
    parts = CeapPartitionStateStore.from_connection_string(conn, state_table)

    run = registry.get_run(pipeline_run_id)
    if run and str(run.get("status", "")).upper() == "COMPLETED":
        log_structured(
            logger,
            "info",
            "CEAP dispatch skipped: run already COMPLETED.",
            mode=mode,
            pipeline_run_id=pipeline_run_id,
            skipped_already_completed=True,
        )
        return

    if run and bool(run.get("enqueue_phase_complete")):
        log_structured(
            logger,
            "info",
            "CEAP dispatch skipped: enqueue phase already complete for this run.",
            mode=mode,
            pipeline_run_id=pipeline_run_id,
            total_tasks_queued=int(run.get("total_tasks_queued", 0) or 0),
        )
        return

    acquired, lock_token = registry.try_acquire_dispatcher_lock(
        mode=mode, pipeline_run_id=pipeline_run_id, ttl_minutes=15
    )
    if not acquired:
        log_structured(
            logger,
            "info",
            "CEAP dispatch skipped: dispatcher lock held.",
            mode=mode,
            pipeline_run_id=pipeline_run_id,
            lock_acquired=False,
        )
        return

    queued_this_tick = 0
    skipped_already_queued = 0
    skipped_already_running = 0
    skipped_success_same_run = 0
    stale_queued_reenqueued = 0
    stale_running_reenqueued = 0
    skipped_future_months = 0

    try:
        run = registry.get_run(pipeline_run_id)
        if run and str(run.get("status", "")).upper() == "COMPLETED":
            return
        if run and bool(run.get("enqueue_phase_complete")):
            return

        if not run:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "run_type": mode,
                    "status": "STARTED",
                    "target_year": target_year,
                    "months_to_process": json.dumps(month_list),
                    "started_at": now.isoformat(),
                    "total_tasks_queued": 0,
                    "total_tasks_success": 0,
                    "total_tasks_failed": 0,
                    "total_tasks_expected": 0,
                    "next_pagina": 1,
                    "next_idx": 0,
                    "next_month_idx": 0,
                    "idle_enqueue_ticks": 0,
                    "enqueue_phase_complete": False,
                }
            )

        run = registry.get_run(pipeline_run_id) or {}
        pagina = int(run.get("next_pagina", 1))
        idx = int(run.get("next_idx", 0))
        month_idx = int(run.get("next_month_idx", 0))
        total_queued = int(run.get("total_tasks_queued", 0) or 0)
        idle_ticks = int(run.get("idle_enqueue_ticks", 0) or 0)

        api = CamaraApiClient()
        remaining = max_per_tick
        hint_payload: dict[str, Any] | None = None

        while remaining > 0:
            payload, http_status = api.list_deputies_page(page=pagina)
            dados = payload.get("dados") or []
            if dados and hint_payload is None:
                hint_payload = payload
            if not dados:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "next_pagina": pagina,
                        "next_idx": 0,
                        "next_month_idx": 0,
                    }
                )
                log_structured(
                    logger,
                    "info",
                    "Dispatcher reached end of deputy list (empty page).",
                    http_status=http_status,
                    pagina=pagina,
                )
                break

            while idx < len(dados) and remaining > 0:
                dep_id = int(dados[idx]["id"])
                while month_idx < len(month_list) and remaining > 0:
                    mes = month_list[month_idx]
                    if mes > max_m:
                        skipped_future_months += 1
                        month_idx += 1
                        continue

                    part = parts.get_partition(dep_id, target_year, mes)
                    action = _partition_enqueue_action(
                        part=part,
                        pipeline_run_id=pipeline_run_id,
                        now=now,
                        stale_after_minutes=stale_after_minutes,
                    )
                    if action.startswith("skip_"):
                        if action == "skip_success_same_run":
                            skipped_success_same_run += 1
                        elif action == "skip_running":
                            skipped_already_running += 1
                        else:
                            skipped_already_queued += 1
                        month_idx += 1
                        continue

                    dispatched_at = datetime.now(UTC).isoformat()
                    wm = CeapApiWorkMessage(
                        endpoint="ceap",
                        id_deputado=dep_id,
                        ano=target_year,
                        mes=mes,
                        mode=mode,
                        pipeline_run_id=pipeline_run_id,
                        dispatched_at=dispatched_at,
                    )
                    send_json_message(queue_name, wm.to_json())

                    prev_pid = str(part.get("current_pipeline_run_id", "")) if part else ""
                    stale_requeue_count = int(part.get("stale_requeue_count", 0) or 0) if part else 0
                    reprocess_count = int(part.get("reprocess_count", 0) or 0) if part else 0
                    if action == "enqueue_stale_queued":
                        stale_queued_reenqueued += 1
                        if part and "stale_requeue_count" in part:
                            stale_requeue_count += 1
                        elif part and "reprocess_count" in part:
                            reprocess_count += 1
                        else:
                            stale_requeue_count = 1
                    elif action == "enqueue_stale_running":
                        stale_running_reenqueued += 1
                        if part and "stale_requeue_count" in part:
                            stale_requeue_count += 1
                        elif part and "reprocess_count" in part:
                            reprocess_count += 1
                        else:
                            stale_requeue_count = 1
                    parts.upsert_partition(
                        {
                            "id_deputado": dep_id,
                            "ano": target_year,
                            "mes": mes,
                            "endpoint": "ceap",
                            "status": "QUEUED",
                            "last_mode": mode,
                            "current_pipeline_run_id": pipeline_run_id,
                            "last_pipeline_run_id": prev_pid,
                            "last_dispatched_at": dispatched_at,
                            "stale_requeue_count": stale_requeue_count,
                            "reprocess_count": reprocess_count,
                            "attempt_count": int(part.get("attempt_count", 0) or 0) if part else 0,
                        }
                    )

                    remaining -= 1
                    queued_this_tick += 1
                    total_queued += 1
                    month_idx += 1

                if month_idx >= len(month_list):
                    month_idx = 0
                    idx += 1

            if idx >= len(dados):
                pagina += 1
                idx = 0
                month_idx = 0

            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "next_pagina": pagina,
                    "next_idx": idx,
                    "next_month_idx": month_idx,
                    "total_tasks_queued": total_queued,
                    "status": "QUEUING",
                }
            )

            if remaining == 0:
                break

        if queued_this_tick == 0:
            idle_ticks += 1
        else:
            idle_ticks = 0

        run_refresh = registry.get_run(pipeline_run_id) or {}
        enqueue_complete = bool(run_refresh.get("enqueue_phase_complete"))
        total_tasks_expected = int(run_refresh.get("total_tasks_expected", 0) or 0)

        if idle_ticks >= _IDLE_TICKS_ENQUEUE_DONE:
            enqueue_complete = True
        if enqueue_complete and total_tasks_expected == 0:
            total_tasks_expected = total_queued

        if enqueue_complete and total_queued == 0:
            run_status = "COMPLETED"
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "idle_enqueue_ticks": idle_ticks,
                    "enqueue_phase_complete": True,
                    "total_tasks_expected": 0,
                    "total_tasks_queued": 0,
                    "status": run_status,
                    "completed_at": datetime.now(UTC).isoformat(),
                    "last_error": "",
                }
            )
        else:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "idle_enqueue_ticks": idle_ticks,
                    "enqueue_phase_complete": enqueue_complete,
                    "total_tasks_expected": total_tasks_expected,
                    "total_tasks_queued": total_queued,
                    "status": ("QUEUED" if enqueue_complete else "QUEUING"),
                    "last_error": "",
                }
            )

        run_done = registry.get_run(pipeline_run_id) or {}
        final_status = str(run_done.get("status", ""))
        total_deputados = _deputados_total_hint(hint_payload)

        log_structured(
            logger,
            "info",
            "CEAP dispatch tick finished.",
            mode=mode,
            pipeline_run_id=pipeline_run_id,
            target_year=target_year,
            months_to_process=month_list,
            max_dispatch_month=max_m,
            total_deputados=total_deputados,
            total_tasks_expected=total_tasks_expected,
            total_tasks_queued=total_queued,
            messages_enqueued=queued_this_tick,
            stale_queued_reenqueued=stale_queued_reenqueued,
            stale_running_reenqueued=stale_running_reenqueued,
            skipped_already_queued=skipped_already_queued,
            skipped_already_running=skipped_already_running,
            skipped_success_same_run=skipped_success_same_run,
            skipped_future_months=skipped_future_months,
            lock_acquired=True,
            idle_enqueue_ticks=idle_ticks,
            enqueue_phase_complete=enqueue_complete,
            next_pagina=pagina,
            next_idx=idx,
            next_month_idx=month_idx,
            final_status=final_status,
        )
    finally:
        registry.release_dispatcher_lock(lock_token)
        snap = registry.get_run(pipeline_run_id) or {}
        log_structured(
            logger,
            "info",
            "CEAP dispatcher lock released.",
            pipeline_run_id=pipeline_run_id,
            lock_released=True,
            final_status=str(snap.get("status", "")),
        )
