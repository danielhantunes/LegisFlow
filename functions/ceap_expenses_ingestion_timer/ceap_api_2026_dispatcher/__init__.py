"""Timer: CEAP API dispatcher — daily moving window vs monthly reconciliation (day 25)."""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import azure.functions as func

from shared.adls_writer import AdlsRawWriter
from shared.api_client import CamaraApiClient
from shared.ceap_partition_state import CeapPartitionStateStore
from shared.ceap_run_registry import CeapRunRegistry
from shared.deputies_snapshot import (
    deputies_date_dir,
    find_latest_valid_snapshot,
    is_snapshot_manifest_valid,
    read_deputies_manifest,
    deputies_success_path,
    load_deputies_from_snapshot,
    write_deputies_manifest,
    write_deputies_metadata,
    write_deputies_success_marker,
)
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

# Chunk size used when iterating the in-memory deputies list during the enqueue phase.
# Kept aligned with the /deputados API default itens=100 to preserve the cursor semantics
# (next_pagina / next_idx) across in-flight runs.
_DEPUTIES_CHUNK_SIZE = 100


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
    reference_tz_name = os.getenv("CEAP_REFERENCE_TIMEZONE", "America/Sao_Paulo")
    try:
        reference_date_str = datetime.now(ZoneInfo(reference_tz_name)).date().isoformat()
    except Exception:
        reference_date_str = now.date().isoformat()
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
        log_structured(
            logger,
            "info",
            "[BEFORE_RUN_REFRESH]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
        )
        run = registry.get_run(pipeline_run_id)
        log_structured(
            logger,
            "info",
            "[AFTER_RUN_REFRESH]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            run_found=bool(run),
            run_status=str((run or {}).get("status", "")),
        )
        if run and str(run.get("status", "")).upper() == "COMPLETED":
            return
        if run and bool(run.get("enqueue_phase_complete")):
            return

        if not run:
            log_structured(
                logger,
                "info",
                "[BEFORE_FINAL_UPSERT]",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                upsert_stage="initialize_run",
            )
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
                    "deputies_pages_written": 0,
                    "deputies_records_count": 0,
                    "deputies_snapshot_status": "IN_PROGRESS",
                    "deputies_snapshot_first_execution_id": "",
                    "deputies_snapshot_date": "",
                    "deputies_snapshot_path": "",
                    "deputies_snapshot_record_count": 0,
                }
            )
            log_structured(
                logger,
                "info",
                "[AFTER_FINAL_UPSERT]",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                upsert_stage="initialize_run",
            )

        run = registry.get_run(pipeline_run_id) or {}
        pagina = int(run.get("next_pagina", 1))
        idx = int(run.get("next_idx", 0))
        month_idx = int(run.get("next_month_idx", 0))
        total_queued = int(run.get("total_tasks_queued", 0) or 0)
        idle_ticks = int(run.get("idle_enqueue_ticks", 0) or 0)

        api = CamaraApiClient()
        raw_writer = AdlsRawWriter(account_name=os.environ["RAW_STORAGE_ACCOUNT_NAME"])
        dispatch_execution_id = str(uuid.uuid4())
        remaining = max_per_tick
        hint_payload: dict[str, Any] | None = None

        prev_snapshot_exec = str(run.get("deputies_snapshot_first_execution_id", "") or "")
        snapshot_started_at = str(run.get("started_at", "") or now.isoformat())

        # =====================================================================
        # Phase A — Ensure deputies snapshot (reuse if COMPLETED, otherwise create)
        # =====================================================================
        snapshot_reused = False
        snapshot_created = False
        snapshot_in_use_date = ""
        snapshot_in_use_path = ""
        snapshot_in_use_record_count = 0
        snapshot_in_use_source = "none"
        snapshot_status_value = "IN_PROGRESS"
        snapshot_completed_at = ""
        total_dep_pages = 0
        total_dep_records = 0
        snapshot_execution_id = prev_snapshot_exec or dispatch_execution_id
        deputies: list[dict[str, Any]] = []

        def _raw_manifest_valid_for_date(reference_date: str) -> tuple[bool, dict[str, Any]]:
            manifest = read_deputies_manifest(raw_writer, reference_date) or {}
            success_exists = raw_writer.path_exists(deputies_success_path(reference_date))
            if not success_exists:
                return False, manifest
            return is_snapshot_manifest_valid(manifest), manifest

        today_record = registry.get_snapshot(reference_date_str) or {}
        today_completed, today_manifest = _raw_manifest_valid_for_date(reference_date_str)

        if today_completed:
            snapshot_path = str(today_manifest.get("raw_path", "") or deputies_date_dir(reference_date_str))
            try:
                deputies, pages_read = load_deputies_from_snapshot(raw_writer, snapshot_path)
            except Exception as load_err:
                deputies, pages_read = [], 0
                log_structured(
                    logger,
                    "warning",
                    "Failed to load completed deputies snapshot; will recreate.",
                    pipeline_run_id=pipeline_run_id,
                    snapshot_path=snapshot_path,
                    error=str(load_err),
                    error_type=type(load_err).__name__,
                )

            if deputies:
                snapshot_reused = True
                snapshot_in_use_date = reference_date_str
                snapshot_in_use_path = snapshot_path
                snapshot_in_use_record_count = int(today_manifest.get("record_count", 0) or 0)
                snapshot_in_use_source = "reused_today"
                snapshot_status_value = "COMPLETED"
                snapshot_completed_at = str(today_manifest.get("completed_at", "") or "")
                total_dep_pages = int(today_manifest.get("total_pages", 0) or pages_read)
                total_dep_records = snapshot_in_use_record_count or len(deputies)
                snapshot_execution_id = (
                    str(today_manifest.get("execution_id", "") or "")
                    or prev_snapshot_exec
                    or dispatch_execution_id
                )
                log_structured(
                    logger,
                    "info",
                    "Reusing today's deputies snapshot (no /deputados call).",
                    pipeline_run_id=pipeline_run_id,
                    mode=mode,
                    deputies_snapshot_date=snapshot_in_use_date,
                    deputies_snapshot_path=snapshot_in_use_path,
                    deputies_snapshot_record_count=snapshot_in_use_record_count,
                    snapshot_reused=True,
                    snapshot_created=False,
                )
            else:
                today_completed = False  # recreate path

        if not snapshot_reused and mode == "reconciliation":
            latest_raw = find_latest_valid_snapshot(
                raw_writer, before_reference_date=reference_date_str
            )
            if latest_raw:
                ref_dt = str(latest_raw.get("reference_date", ""))
                latest_manifest = latest_raw.get("manifest") or {}
                snapshot_path = str(latest_raw.get("path", "") or deputies_date_dir(ref_dt))
                try:
                    deputies, pages_read = load_deputies_from_snapshot(raw_writer, snapshot_path)
                except Exception as load_err:
                    deputies, pages_read = [], 0
                    log_structured(
                        logger,
                        "warning",
                        "Failed to load fallback deputies snapshot; will create one.",
                        pipeline_run_id=pipeline_run_id,
                        fallback_path=snapshot_path,
                        error=str(load_err),
                        error_type=type(load_err).__name__,
                    )
                if deputies:
                    snapshot_reused = True
                    snapshot_in_use_date = ref_dt
                    snapshot_in_use_path = snapshot_path
                    snapshot_in_use_record_count = int(latest_manifest.get("record_count", 0) or 0)
                    snapshot_in_use_source = "reused_fallback"
                    snapshot_status_value = "COMPLETED"
                    snapshot_completed_at = str(latest_manifest.get("completed_at", "") or "")
                    total_dep_pages = int(latest_manifest.get("total_pages", 0) or pages_read)
                    total_dep_records = snapshot_in_use_record_count or len(deputies)
                    snapshot_execution_id = (
                        str(latest_manifest.get("execution_id", "") or "")
                        or prev_snapshot_exec
                        or dispatch_execution_id
                    )
                    log_structured(
                        logger,
                        "warning",
                        "Today's deputies snapshot incomplete; reusing latest COMPLETED snapshot for reconciliation.",
                        pipeline_run_id=pipeline_run_id,
                        current_reference_date=reference_date_str,
                        deputies_snapshot_date=snapshot_in_use_date,
                        deputies_snapshot_path=snapshot_in_use_path,
                        deputies_snapshot_record_count=snapshot_in_use_record_count,
                        snapshot_reused=True,
                        snapshot_created=False,
                    )

        if not snapshot_reused:
            snapshot_execution_id = prev_snapshot_exec or dispatch_execution_id
            try:
                registry.upsert_snapshot(
                    reference_date_str,
                    {
                        "status": "IN_PROGRESS",
                        "execution_id": snapshot_execution_id,
                        "pipeline_run_id": pipeline_run_id,
                        "started_at": snapshot_started_at,
                        "raw_path": deputies_date_dir(reference_date_str),
                        "total_pages": 0,
                        "record_count": 0,
                        "last_error": "",
                    },
                )
            except Exception as snap_err:
                log_structured(
                    logger,
                    "warning",
                    "Failed to upsert IN_PROGRESS snapshot record.",
                    pipeline_run_id=pipeline_run_id,
                    reference_date=reference_date_str,
                    error=str(snap_err),
                    error_type=type(snap_err).__name__,
                )

            log_structured(
                logger,
                "info",
                "Creating new deputies snapshot (calling /deputados).",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                reference_date=reference_date_str,
                snapshot_execution_id=snapshot_execution_id,
            )

            collected: list[dict[str, Any]] = []
            pagina_api = 1
            pages_written = 0
            try:
                while True:
                    payload, http_status = api.list_deputies_page(page=pagina_api)
                    dados = payload.get("dados") or []
                    if dados and hint_payload is None:
                        hint_payload = payload
                    if not dados:
                        log_structured(
                            logger,
                            "info",
                            "Dispatcher reached end of deputy list (empty page).",
                            pipeline_run_id=pipeline_run_id,
                            http_status=http_status,
                            pagina=pagina_api,
                        )
                        break

                    raw_path = (
                        f"{deputies_date_dir(reference_date_str)}/"
                        f"pipeline_run_id={pipeline_run_id}/"
                        f"execution_id={snapshot_execution_id}/"
                        f"page_{pagina_api}.json"
                    )
                    raw_writer.write_json(raw_path, payload)
                    pages_written += 1
                    collected.extend([d for d in dados if isinstance(d, dict) and "id" in d])
                    log_structured(
                        logger,
                        "info",
                        "Deputies page persisted in raw.",
                        mode=mode,
                        pipeline_run_id=pipeline_run_id,
                        dispatch_execution_id=dispatch_execution_id,
                        snapshot_execution_id=snapshot_execution_id,
                        reference_date=reference_date_str,
                        reference_timezone=reference_tz_name,
                        page=pagina_api,
                        record_count=len(dados),
                        raw_path=raw_path,
                        http_status=http_status,
                    )
                    pagina_api += 1

                snapshot_completed_at = datetime.now(UTC).isoformat()
                deputies = collected
                total_dep_pages = pages_written
                total_dep_records = len(collected)
                snapshot_status_value = "COMPLETED" if total_dep_records > 0 else "FAILED"

                metadata_payload = {
                    "endpoint": "deputados",
                    "reference_date": reference_date_str,
                    "reference_timezone": reference_tz_name,
                    "pipeline_run_id": pipeline_run_id,
                    "execution_id": snapshot_execution_id,
                    "status": snapshot_status_value,
                    "total_pages": total_dep_pages,
                    "record_count": total_dep_records,
                    "started_at": snapshot_started_at,
                    "completed_at": snapshot_completed_at,
                    "files_written": total_dep_pages,
                    "error_message": "" if snapshot_status_value == "COMPLETED" else "Empty deputies response",
                }
                manifest_payload = {
                    "endpoint": "deputados",
                    "reference_date": reference_date_str,
                    "status": snapshot_status_value,
                    "total_pages": total_dep_pages,
                    "record_count": total_dep_records,
                    "started_at": snapshot_started_at,
                    "completed_at": snapshot_completed_at,
                    "raw_path": deputies_date_dir(reference_date_str),
                    "files_written": total_dep_pages,
                    "created_at": datetime.now(UTC).isoformat(),
                    "last_error": "" if snapshot_status_value == "COMPLETED" else "Empty deputies response",
                    "pipeline_run_id": pipeline_run_id,
                    "execution_id": snapshot_execution_id,
                }
                try:
                    write_deputies_metadata(raw_writer, reference_date_str, metadata_payload)
                except Exception as meta_err:
                    log_structured(
                        logger,
                        "warning",
                        "Failed to write deputies snapshot metadata.json.",
                        pipeline_run_id=pipeline_run_id,
                        reference_date=reference_date_str,
                        error=str(meta_err),
                        error_type=type(meta_err).__name__,
                    )
                try:
                    write_deputies_manifest(raw_writer, reference_date_str, manifest_payload)
                except Exception as manifest_err:
                    log_structured(
                        logger,
                        "warning",
                        "Failed to write deputies snapshot _metadata/run_summary.json.",
                        pipeline_run_id=pipeline_run_id,
                        reference_date=reference_date_str,
                        error=str(manifest_err),
                        error_type=type(manifest_err).__name__,
                    )

                if snapshot_status_value == "COMPLETED":
                    try:
                        write_deputies_success_marker(raw_writer, reference_date_str)
                    except Exception as success_err:
                        log_structured(
                            logger,
                            "warning",
                            "Failed to write deputies _SUCCESS marker.",
                            pipeline_run_id=pipeline_run_id,
                            reference_date=reference_date_str,
                            error=str(success_err),
                            error_type=type(success_err).__name__,
                        )

                try:
                    registry.upsert_snapshot(
                        reference_date_str,
                        {
                            "status": snapshot_status_value,
                            "execution_id": snapshot_execution_id,
                            "pipeline_run_id": pipeline_run_id,
                            "started_at": snapshot_started_at,
                            "completed_at": snapshot_completed_at,
                            "total_pages": total_dep_pages,
                            "record_count": total_dep_records,
                            "raw_path": deputies_date_dir(reference_date_str),
                            "last_error": "" if snapshot_status_value == "COMPLETED" else "Empty deputies response",
                        },
                    )
                except Exception as snap_err:
                    log_structured(
                        logger,
                        "warning",
                        "Failed to upsert COMPLETED snapshot record.",
                        pipeline_run_id=pipeline_run_id,
                        reference_date=reference_date_str,
                        error=str(snap_err),
                        error_type=type(snap_err).__name__,
                    )

                snapshot_in_use_date = reference_date_str
                snapshot_in_use_path = deputies_date_dir(reference_date_str)
                snapshot_in_use_record_count = total_dep_records
                snapshot_in_use_source = "created_today" if total_dep_records > 0 else "none"
                snapshot_created = True

                log_structured(
                    logger,
                    "info",
                    "Deputies snapshot collection finished.",
                    pipeline_run_id=pipeline_run_id,
                    mode=mode,
                    reference_date=reference_date_str,
                    snapshot_status=snapshot_status_value,
                    total_pages=total_dep_pages,
                    record_count=total_dep_records,
                    snapshot_reused=False,
                    snapshot_created=True,
                )
            except Exception as collect_err:
                try:
                    registry.upsert_snapshot(
                        reference_date_str,
                        {
                            "status": "FAILED",
                            "execution_id": snapshot_execution_id,
                            "pipeline_run_id": pipeline_run_id,
                            "started_at": snapshot_started_at,
                            "total_pages": pages_written,
                            "record_count": len(collected),
                            "raw_path": deputies_date_dir(reference_date_str),
                            "last_error": f"{type(collect_err).__name__}: {str(collect_err)}",
                        },
                    )
                except Exception:
                    pass
                raise

        if not deputies:
            log_structured(
                logger,
                "warning",
                "No deputies available after Phase A; skipping enqueue this tick.",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                snapshot_in_use_date=snapshot_in_use_date,
                snapshot_in_use_source=snapshot_in_use_source,
                snapshot_reused=snapshot_reused,
                snapshot_created=snapshot_created,
            )

        # =====================================================================
        # Phase B — Enqueue CEAP tasks from in-memory deputies list
        # =====================================================================
        total_deputies = len(deputies)
        snapshot_pages = max(
            1, (total_deputies + _DEPUTIES_CHUNK_SIZE - 1) // _DEPUTIES_CHUNK_SIZE
        ) if total_deputies > 0 else 0

        while remaining > 0 and total_deputies > 0:
            start_idx = (pagina - 1) * _DEPUTIES_CHUNK_SIZE
            if start_idx >= total_deputies:
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
                    "Dispatcher walked through whole deputies list.",
                    pipeline_run_id=pipeline_run_id,
                    pagina=pagina,
                    snapshot_pages=snapshot_pages,
                    total_deputies=total_deputies,
                )
                break

            end_idx = min(start_idx + _DEPUTIES_CHUNK_SIZE, total_deputies)
            chunk = deputies[start_idx:end_idx]

            while idx < len(chunk) and remaining > 0:
                try:
                    dep_id = int(chunk[idx]["id"])
                except (KeyError, TypeError, ValueError):
                    idx += 1
                    month_idx = 0
                    continue

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

            if idx >= len(chunk):
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

        enqueue_walked_full_list = (
            total_deputies > 0 and (pagina - 1) * _DEPUTIES_CHUNK_SIZE >= total_deputies
        )

        log_structured(
            logger,
            "info",
            "[AFTER_ENQUEUE_LOOP]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            queued_this_tick=queued_this_tick,
            total_queued=total_queued,
            remaining=remaining,
            pagina=pagina,
            idx=idx,
            month_idx=month_idx,
            total_deputies=total_deputies,
            snapshot_pages=snapshot_pages,
            enqueue_walked_full_list=enqueue_walked_full_list,
            snapshot_reused=snapshot_reused,
            snapshot_created=snapshot_created,
            snapshot_in_use_source=snapshot_in_use_source,
        )

        if queued_this_tick == 0:
            idle_ticks += 1
        else:
            idle_ticks = 0

        log_structured(
            logger,
            "info",
            "[BEFORE_RUN_REFRESH]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            refresh_stage="post_enqueue",
        )
        run_refresh = registry.get_run(pipeline_run_id) or {}
        log_structured(
            logger,
            "info",
            "[AFTER_RUN_REFRESH]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            refresh_stage="post_enqueue",
            run_status=str(run_refresh.get("status", "")),
            enqueue_phase_complete=bool(run_refresh.get("enqueue_phase_complete")),
        )
        enqueue_complete = bool(run_refresh.get("enqueue_phase_complete"))
        total_tasks_expected = int(run_refresh.get("total_tasks_expected", 0) or 0)

        if enqueue_walked_full_list:
            enqueue_complete = True
        if idle_ticks >= _IDLE_TICKS_ENQUEUE_DONE:
            enqueue_complete = True

        partition_counts = parts.count_statuses_by_run(pipeline_run_id)
        total_tasks_success = partition_counts["success"]
        total_tasks_failed = partition_counts["failed"]
        total_tasks_running = partition_counts["running"]
        total_tasks_pending = partition_counts["pending"]
        total_tasks_poison = partition_counts["poison"]
        total_tasks_queued_state = partition_counts["queued"]
        total_tasks_stale_state = partition_counts["stale"]
        total_tasks_other_state = partition_counts["other"]

        if enqueue_complete:
            total_tasks_expected = (
                total_tasks_queued_state
                + total_tasks_running
                + total_tasks_success
                + total_tasks_failed
                + total_tasks_pending
                + total_tasks_poison
                + total_tasks_stale_state
                + total_tasks_other_state
            )
        else:
            total_tasks_expected = max(total_queued, total_tasks_expected)

        all_finished = (
            total_tasks_running == 0
            and total_tasks_queued_state == 0
            and total_tasks_pending == 0
            and total_tasks_stale_state == 0
            and total_tasks_other_state == 0
        )

        completed_at_iso: str | None = None
        last_error_text: str | None = None
        if (
            enqueue_complete
            and total_tasks_expected > 0
            and total_tasks_success == total_tasks_expected
            and total_tasks_failed == 0
            and total_tasks_poison == 0
            and total_tasks_running == 0
            and total_tasks_queued_state == 0
            and total_tasks_pending == 0
        ):
            run_status = "COMPLETED"
            completed_at_iso = datetime.now(UTC).isoformat()
            last_error_text = ""
        elif enqueue_complete and total_tasks_expected == 0 and total_queued == 0:
            run_status = "COMPLETED"
            completed_at_iso = datetime.now(UTC).isoformat()
            last_error_text = ""
        elif enqueue_complete and all_finished and (total_tasks_failed > 0 or total_tasks_poison > 0):
            run_status = "PARTIAL" if total_tasks_success > 0 else "FAILED"
            completed_at_iso = datetime.now(UTC).isoformat()
            last_error_text = (
                f"failed={total_tasks_failed}, poison={total_tasks_poison}"
            )
        elif total_tasks_running > 0:
            run_status = "RUNNING"
        elif enqueue_complete:
            run_status = "QUEUED"
        else:
            run_status = "QUEUING"

        log_structured(
            logger,
            "info",
            "[BEFORE_FINAL_UPSERT]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            upsert_stage="final_summary",
            run_status=run_status,
            enqueue_phase_complete=enqueue_complete,
            total_tasks_expected=total_tasks_expected,
            total_tasks_queued=total_queued,
            total_tasks_success=total_tasks_success,
            total_tasks_failed=total_tasks_failed,
            total_tasks_running=total_tasks_running,
            total_tasks_pending=total_tasks_pending,
            total_tasks_poison=total_tasks_poison,
        )

        previous_status = str(run_refresh.get("status", "")).upper()
        previous_last_error = str(run_refresh.get("last_error", "") or "")
        previous_failed_at = str(run_refresh.get("failed_at", "") or "")

        upsert_payload: dict[str, Any] = {
            "pipeline_run_id": pipeline_run_id,
            "run_type": mode,
            "idle_enqueue_ticks": idle_ticks,
            "enqueue_phase_complete": enqueue_complete,
            "total_tasks_expected": total_tasks_expected,
            "total_tasks_queued": total_queued,
            "total_tasks_success": total_tasks_success,
            "total_tasks_failed": total_tasks_failed,
            "total_tasks_running": total_tasks_running,
            "total_tasks_pending": total_tasks_pending,
            "total_tasks_poison": total_tasks_poison,
            "status": run_status,
            "deputies_pages_written": total_dep_pages,
            "deputies_records_count": total_dep_records,
            "deputies_snapshot_status": snapshot_status_value,
            "deputies_snapshot_first_execution_id": snapshot_execution_id,
            "deputies_snapshot_date": snapshot_in_use_date,
            "deputies_snapshot_path": snapshot_in_use_path,
            "deputies_snapshot_record_count": snapshot_in_use_record_count,
            "deputies_snapshot_source": snapshot_in_use_source,
        }
        if snapshot_completed_at:
            upsert_payload["deputies_snapshot_completed_at"] = snapshot_completed_at
        if completed_at_iso is not None:
            upsert_payload["completed_at"] = completed_at_iso
        if last_error_text is not None:
            upsert_payload["last_error"] = last_error_text
        if run_status == "COMPLETED":
            # Run recovered/finished successfully: clear failure markers from previous attempts.
            upsert_payload["failed_at"] = ""
            upsert_payload["last_error"] = ""
            if (
                previous_failed_at
                or previous_last_error
                or previous_status in {"FAILED", "PARTIAL"}
            ):
                upsert_payload["last_recovered_at"] = datetime.now(UTC).isoformat()

        registry.upsert_run(upsert_payload)
        log_structured(
            logger,
            "info",
            "[AFTER_FINAL_UPSERT]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            upsert_stage="final_summary",
            run_status=run_status,
        )

        log_structured(
            logger,
            "info",
            "[BEFORE_RUN_REFRESH]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            refresh_stage="final_status",
        )
        run_done = registry.get_run(pipeline_run_id) or {}
        log_structured(
            logger,
            "info",
            "[AFTER_RUN_REFRESH]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            refresh_stage="final_status",
            run_status=str(run_done.get("status", "")),
        )
        final_status = str(run_done.get("status", ""))
        total_deputados = _deputados_total_hint(hint_payload)

        # Write Raw run manifest for downstream Bronze orchestration.
        raw_base_path = "raw/camara/ceap/api/despesas"
        run_meta_prefix = (
            f"{raw_base_path}/_metadata/runs/pipeline_run_id={pipeline_run_id}"
        )
        strict_completed = (
            bool(run_done.get("enqueue_phase_complete"))
            and int(run_done.get("total_tasks_expected", 0) or 0)
            == int(run_done.get("total_tasks_success", 0) or 0)
            and int(run_done.get("total_tasks_failed", 0) or 0) == 0
            and int(run_done.get("total_tasks_pending", 0) or 0) == 0
            and int(run_done.get("total_tasks_poison", 0) or 0) == 0
            and int(run_done.get("total_tasks_running", 0) or 0) == 0
        )
        run_summary_payload: dict[str, Any] = {
            "pipeline_run_id": pipeline_run_id,
            "run_type": mode,
            "status": final_status,
            "target_year": target_year,
            "months_to_process": run_done.get("months_to_process")
            or json.dumps(month_list),
            "started_at": run_done.get("started_at") or now.isoformat(),
            "completed_at": run_done.get("completed_at") or completed_at_iso or "",
            "total_tasks_expected": int(run_done.get("total_tasks_expected", 0) or 0),
            "total_tasks_queued": int(run_done.get("total_tasks_queued", 0) or 0),
            "total_tasks_success": int(run_done.get("total_tasks_success", 0) or 0),
            "total_tasks_failed": int(run_done.get("total_tasks_failed", 0) or 0),
            "total_tasks_pending": int(run_done.get("total_tasks_pending", 0) or 0),
            "total_tasks_poison": int(run_done.get("total_tasks_poison", 0) or 0),
            "total_tasks_running": int(run_done.get("total_tasks_running", 0) or 0),
            "enqueue_phase_complete": bool(run_done.get("enqueue_phase_complete")),
            "deputies_snapshot_date": run_done.get("deputies_snapshot_date")
            or snapshot_in_use_date,
            "deputies_snapshot_path": run_done.get("deputies_snapshot_path")
            or snapshot_in_use_path,
            "deputies_snapshot_record_count": int(
                run_done.get("deputies_snapshot_record_count", 0)
                or snapshot_in_use_record_count
                or 0
            ),
            "deputies_snapshot_status": run_done.get("deputies_snapshot_status")
            or snapshot_status_value,
            "raw_base_path": raw_base_path,
            "created_at": datetime.now(UTC).isoformat(),
        }
        run_summary_path = f"{run_meta_prefix}/run_summary.json"
        run_success_path = f"{run_meta_prefix}/_SUCCESS"
        try:
            raw_writer.write_json(run_summary_path, run_summary_payload)
            if final_status == "COMPLETED" and strict_completed:
                raw_writer.write_text(run_success_path, "")
            log_structured(
                logger,
                "info",
                "CEAP run manifest persisted in raw.",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                run_summary_path=run_summary_path,
                run_success_path=run_success_path if (final_status == "COMPLETED" and strict_completed) else "",
                final_status=final_status,
                strict_completed=strict_completed,
            )
        except Exception as manifest_err:
            log_structured(
                logger,
                "warning",
                "Failed to persist CEAP run manifest in raw.",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                run_summary_path=run_summary_path,
                final_status=final_status,
                error=str(manifest_err),
                error_type=type(manifest_err).__name__,
            )

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
            total_tasks_success=total_tasks_success,
            total_tasks_failed=total_tasks_failed,
            total_tasks_running=total_tasks_running,
            total_tasks_pending=total_tasks_pending,
            total_tasks_poison=total_tasks_poison,
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
            completed_at=completed_at_iso,
            next_pagina=pagina,
            next_idx=idx,
            next_month_idx=month_idx,
            final_status=final_status,
            deputies_pages_written=total_dep_pages,
            deputies_records_count=total_dep_records,
            deputies_snapshot_status=snapshot_status_value,
            deputies_snapshot_date=snapshot_in_use_date,
            deputies_snapshot_path=snapshot_in_use_path,
            deputies_snapshot_record_count=snapshot_in_use_record_count,
            deputies_snapshot_source=snapshot_in_use_source,
            deputies_snapshot_completed_at=snapshot_completed_at,
            snapshot_reused=snapshot_reused,
            snapshot_created=snapshot_created,
            ceap_max_tasks_per_dispatch=max_per_tick,
        )
        log_structured(
            logger,
            "info",
            "[DISPATCH_TICK_FINISHED]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            status=final_status,
            queued_this_tick=queued_this_tick,
            total_queued=total_queued,
            total_tasks_expected=total_tasks_expected,
            total_tasks_success=total_tasks_success,
            total_tasks_failed=total_tasks_failed,
            total_tasks_running=total_tasks_running,
            total_tasks_pending=total_tasks_pending,
            total_tasks_poison=total_tasks_poison,
            remaining=remaining,
            pagina=pagina,
            idx=idx,
            month_idx=month_idx,
            idle_ticks=idle_ticks,
            enqueue_phase_complete=enqueue_complete,
            completed_at=completed_at_iso,
            final_status=final_status,
            snapshot_reused=snapshot_reused,
            snapshot_created=snapshot_created,
            deputies_snapshot_date=snapshot_in_use_date,
            deputies_snapshot_path=snapshot_in_use_path,
            deputies_snapshot_record_count=snapshot_in_use_record_count,
            ceap_max_tasks_per_dispatch=max_per_tick,
        )
    except Exception as e:
        log_structured(
            logger,
            "error",
            "CEAP dispatcher failed.",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            error=str(e),
            error_type=type(e).__name__,
            queued_this_tick=queued_this_tick,
            total_queued=locals().get("total_queued", 0),
            remaining=locals().get("remaining"),
        )
        try:
            log_structured(
                logger,
                "info",
                "[BEFORE_FINAL_UPSERT]",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                upsert_stage="failed_exception",
            )
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "status": "FAILED",
                    "last_error": f"{type(e).__name__}: {str(e)}",
                    "failed_at": datetime.now(UTC).isoformat(),
                }
            )
            log_structured(
                logger,
                "info",
                "[AFTER_FINAL_UPSERT]",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                upsert_stage="failed_exception",
            )
        except Exception as upsert_error:
            log_structured(
                logger,
                "error",
                "Failed to upsert FAILED state for dispatcher run.",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                error=str(upsert_error),
                error_type=type(upsert_error).__name__,
            )
        raise
    finally:
        log_structured(
            logger,
            "info",
            "[BEFORE_RELEASE_LOCK]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
        )
        registry.release_dispatcher_lock(lock_token)
        log_structured(
            logger,
            "info",
            "[AFTER_RELEASE_LOCK]",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
        )
        snap = registry.get_run(pipeline_run_id) or {}
        log_structured(
            logger,
            "info",
            "CEAP dispatcher lock released.",
            pipeline_run_id=pipeline_run_id,
            lock_released=True,
            final_status=str(snap.get("status", "")),
        )
