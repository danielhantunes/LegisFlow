"""Timer: CEAP API dispatcher — daily (mês atual) vs reconciliação semanal (domingo UTC).

* **Daily** (default): ``ceap_daily_YYYYMMDD``, meses de
  ``CEAP_DAILY_LOOKBACK_MONTHS`` (0 = só o mês corrente no ano alvo).
* **Reconciliation** (default): domingo (``weekday()==6`` em UTC), janela
  **mês anterior + mês atual** no ``CEAP_TARGET_YEAR`` via
  :func:`shared.dispatch_months.months_reconciliation_current_and_previous`.
* **Legado**: ``CEAP_RECONCILIATION_LEGACY_FULL_YEAR=true`` restaura o dia
  ``CEAP_RECONCILIATION_DAY`` com varredura ``CEAP_RECONCILIATION_START_MONTH``..mês máximo.
"""

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
    build_deputies_snapshot_metadata,
    deputies_date_dir,
    deputies_run_dir,
    deputies_success_path,
    find_latest_valid_snapshot,
    is_snapshot_manifest_valid,
    load_deputies_from_snapshot,
    persist_deputies_snapshot_metadata,
    read_deputies_manifest,
    write_deputies_manifest,
)
from shared.dispatch_months import (
    max_dispatch_month,
    months_daily_moving_window,
    months_reconciliation_current_and_previous,
    months_reconciliation_window,
)
from shared.logger import get_logger, log_structured
from shared.queue_helpers import (
    prepare_queue_client_for_dispatch,
    send_json_message_with_client,
)
from shared.raw_audit import enrich_deputies_page_payload, now_utc_iso
from shared.ceap_raw_manifest import (
    build_ceap_dispatcher_run_metadata,
    ceap_run_metadata_path,
    ceap_run_success_path,
    persist_ceap_dispatcher_run_metadata,
)
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


def _reconcile_run_manifest_from_table(
    *,
    parts: CeapPartitionStateStore,
    registry: CeapRunRegistry,
    raw_writer: AdlsRawWriter,
    pipeline_run_id: str,
    mode: str,
    target_year: int,
    months_to_process_json: str,
    max_tasks_per_dispatch: int | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Brings the Raw ``metadata.json`` (and registry status) in sync with IngestionState.

    Always overwrites ``metadata.json`` with counters derived from
    ``count_statuses_by_run`` (`current_pipeline_run_id` OR
    `last_pipeline_run_id`). If every partition is ``SUCCESS`` and the enqueue
    phase is complete:

    * the run is upserted as ``COMPLETED`` in ``IngestionControlApi2026`` (with
      ``completed_at``, cleared ``failed_at``/``last_error``);
    * the ``_SUCCESS`` marker is written under the run's manifest folder.

    Returns ``(strict_completed, run_after_reconcile)``.
    """
    pc = parts.count_statuses_by_run(pipeline_run_id)
    run = registry.get_run(pipeline_run_id) or {}
    enq_done = bool(run.get("enqueue_phase_complete"))

    te = (
        pc["queued"]
        + pc["running"]
        + pc["success"]
        + pc["failed"]
        + pc["poison"]
        + pc["pending"]
        + pc["stale"]
        + pc["other"]
        if enq_done
        else int(run.get("total_tasks_expected", 0) or 0)
    )
    rec_sum = int(pc.get("record_count_sum", 0) or 0)
    pages_sum = int(pc.get("pages_written_sum", 0) or 0)

    strict_completed = (
        enq_done
        and te > 0
        and pc["success"] == te
        and pc["failed"] == 0
        and pc["pending"] == 0
        and pc["poison"] == 0
        and pc["running"] == 0
        and pc["queued"] == 0
        and pc["stale"] == 0
        and pc["other"] == 0
    )

    completed_iso = str(run.get("completed_at") or "").strip()
    if strict_completed and not completed_iso:
        completed_iso = datetime.now(UTC).isoformat()

    if strict_completed and str(run.get("status", "")).upper() != "COMPLETED":
        prev_failed_at = str(run.get("failed_at", "") or "")
        prev_last_error = str(run.get("last_error", "") or "")
        prev_status = str(run.get("status", "")).upper()
        recover_patch: dict[str, Any] = {
            "pipeline_run_id": pipeline_run_id,
            "status": "COMPLETED",
            "completed_at": completed_iso,
            "failed_at": "",
            "last_error": "",
            "total_tasks_expected": te,
            "total_tasks_success": pc["success"],
            "total_tasks_failed": 0,
            "total_tasks_pending": 0,
            "total_tasks_poison": 0,
            "total_tasks_running": 0,
            "total_tasks_queued": 0,
        }
        if prev_failed_at or prev_last_error or prev_status in {"FAILED", "PARTIAL"}:
            recover_patch["last_recovered_at"] = datetime.now(UTC).isoformat()
        try:
            registry.upsert_run(recover_patch)
            run = registry.get_run(pipeline_run_id) or run
        except Exception as recover_err:
            log_structured(
                logger,
                "warning",
                "Could not upsert COMPLETED run state during pre-flight reconcile.",
                pipeline_run_id=pipeline_run_id,
                error=str(recover_err),
                error_type=type(recover_err).__name__,
            )

    status_final = (
        "COMPLETED"
        if strict_completed
        else (str(run.get("status") or "").upper() or "UNKNOWN")
    )
    months_json = (
        str(run.get("months_to_process"))
        if run.get("months_to_process")
        else months_to_process_json
    )
    started_at = str(
        run.get("started_at")
        or completed_iso
        or datetime.now(UTC).isoformat()
    )
    failed_at = None
    if status_final in ("FAILED", "PARTIAL", "PARTIALLY_COMPLETED"):
        fa = str(run.get("failed_at") or "").strip()
        failed_at = fa if fa else None

    err_type = None if status_final == "COMPLETED" else run.get("error_type")
    err_msg = None if status_final == "COMPLETED" else run.get("last_error")

    doc = build_ceap_dispatcher_run_metadata(
        pipeline_run_id=pipeline_run_id,
        mode=mode,
        status=status_final,
        started_at_utc=started_at,
        finished_at_utc=completed_iso if status_final == "COMPLETED" else None,
        failed_at_utc=failed_at,
        total_tasks_expected=te,
        total_tasks_queued=pc["queued"],
        total_tasks_pending=pc["pending"],
        target_year=target_year,
        months_to_process_json=months_json,
        enqueue_phase_complete=enq_done,
        deputies_snapshot_date=str(run.get("deputies_snapshot_date") or ""),
        deputies_snapshot_path=str(run.get("deputies_snapshot_path") or ""),
        deputies_snapshot_record_count=int(
            run.get("deputies_snapshot_record_count", 0) or 0
        ),
        deputies_snapshot_status=str(run.get("deputies_snapshot_status") or ""),
        deputies_snapshot_pipeline_run_id=str(
            run.get("deputies_snapshot_pipeline_run_id") or ""
        ),
        error_type=str(err_type) if err_type else None,
        error_message=str(err_msg) if err_msg else None,
        total_tasks_success=pc["success"],
        total_tasks_failed=pc["failed"],
        total_tasks_poison=pc["poison"],
        total_tasks_running=pc["running"],
        max_tasks_per_dispatch=max_tasks_per_dispatch,
        total_raw_files_written=pages_sum,
        total_records_collected=rec_sum,
    )
    try:
        manifest_path, _ = persist_ceap_dispatcher_run_metadata(
            raw_writer,
            doc,
            write_success_marker_now=strict_completed,
        )
        log_structured(
            logger,
            "info",
            "CEAP run manifest reconciled from IngestionState.",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            manifest_path=manifest_path,
            success_marker_written=strict_completed,
            manifest_status=status_final,
            total_tasks_expected=te,
            total_tasks_success=pc["success"],
            total_tasks_failed=pc["failed"],
            total_tasks_pending=pc["pending"],
            total_tasks_poison=pc["poison"],
            total_tasks_running=pc["running"],
            total_tasks_queued=pc["queued"],
        )
    except Exception as persist_err:
        log_structured(
            logger,
            "warning",
            "Failed to persist reconciled CEAP run manifest.",
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            error=str(persist_err),
            error_type=type(persist_err).__name__,
        )

    return strict_completed, run


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
    lookback = int(os.getenv("CEAP_DAILY_LOOKBACK_MONTHS", "0"))
    start_month = int(os.getenv("CEAP_RECONCILIATION_START_MONTH", "1"))
    legacy_full_year_recon = str(
        os.getenv("CEAP_RECONCILIATION_LEGACY_FULL_YEAR", "")
    ).lower() in ("1", "true", "yes")
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

    if legacy_full_year_recon and now.day == recon_day:
        mode = "reconciliation"
        pipeline_run_id = f"ceap_reconciliation_{now:%Y%m%d}"
        month_list = months_reconciliation_window(
            target_year=target_year, now=now, start_month=start_month
        )
    elif (not legacy_full_year_recon) and now.weekday() == 6:
        mode = "reconciliation"
        pipeline_run_id = f"ceap_reconciliation_{now:%Y%m%d}"
        month_list = months_reconciliation_current_and_previous(
            target_year=target_year, now=now
        )
    else:
        mode = "daily"
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

    # Pre-flight manifest reconcile: every tick that finds an existing run
    # rewrites Raw metadata.json from IngestionState ground truth and (when
    # strictly complete) creates _SUCCESS even if the dispatcher will skip
    # the rest of the tick. Skipped only when _SUCCESS already exists, since
    # the manifest then matches the contract by definition.
    if run:
        try:
            raw_writer_pre = AdlsRawWriter(
                account_name=os.environ["RAW_STORAGE_ACCOUNT_NAME"]
            )
            if not raw_writer_pre.path_exists(ceap_run_success_path(pipeline_run_id)):
                _reconcile_run_manifest_from_table(
                    parts=parts,
                    registry=registry,
                    raw_writer=raw_writer_pre,
                    pipeline_run_id=pipeline_run_id,
                    mode=mode,
                    target_year=target_year,
                    months_to_process_json=json.dumps(month_list),
                    max_tasks_per_dispatch=max_per_tick,
                )
                run = registry.get_run(pipeline_run_id) or run
        except Exception as recon_err:
            log_structured(
                logger,
                "warning",
                "Pre-flight CEAP manifest reconcile failed.",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                error=str(recon_err),
                error_type=type(recon_err).__name__,
            )

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
    abort_enqueue = False
    enqueue_abort_error: BaseException | None = None

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
        snapshot_in_use_pipeline_run_id = ""
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
                snapshot_in_use_pipeline_run_id = str(
                    today_manifest.get("pipeline_run_id") or pipeline_run_id
                )
                snapshot_in_use_path = deputies_run_dir(
                    reference_date_str, snapshot_in_use_pipeline_run_id
                )
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
                    snapshot_in_use_pipeline_run_id = str(
                        latest_manifest.get("pipeline_run_id") or pipeline_run_id
                    )
                    snapshot_in_use_path = deputies_run_dir(
                        ref_dt, snapshot_in_use_pipeline_run_id
                    )
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

            try:
                running_meta = build_deputies_snapshot_metadata(
                    pipeline_run_id=pipeline_run_id,
                    execution_id=snapshot_execution_id,
                    reference_date=reference_date_str,
                    reference_timezone=reference_tz_name,
                    status="RUNNING",
                    started_at_utc=snapshot_started_at,
                    completed_at_utc=None,
                    total_pages=0,
                    record_count=0,
                    error_message=None,
                )
                persist_deputies_snapshot_metadata(
                    raw_writer,
                    reference_date_str,
                    running_meta,
                    write_success_marker_now=False,
                )
                log_structured(
                    logger,
                    "info",
                    "Deputies snapshot metadata persisted (RUNNING).",
                    pipeline_run_id=pipeline_run_id,
                    reference_date=reference_date_str,
                    snapshot_execution_id=snapshot_execution_id,
                )
            except Exception as init_meta_err:
                log_structured(
                    logger,
                    "warning",
                    "Failed to persist initial deputies snapshot metadata.",
                    pipeline_run_id=pipeline_run_id,
                    reference_date=reference_date_str,
                    error=str(init_meta_err),
                    error_type=type(init_meta_err).__name__,
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
                    enriched_dep_payload = enrich_deputies_page_payload(
                        payload,
                        pipeline_run_id=pipeline_run_id,
                        execution_id=snapshot_execution_id,
                        reference_date=reference_date_str,
                        page=pagina_api,
                        raw_path=raw_path,
                        ingested_at_utc=now_utc_iso(),
                    )
                    raw_writer.write_json(raw_path, enriched_dep_payload)
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
                snapshot_error_message = (
                    None
                    if snapshot_status_value == "COMPLETED"
                    else "Empty deputies response"
                )

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
                    final_meta = build_deputies_snapshot_metadata(
                        pipeline_run_id=pipeline_run_id,
                        execution_id=snapshot_execution_id,
                        reference_date=reference_date_str,
                        reference_timezone=reference_tz_name,
                        status=snapshot_status_value,
                        started_at_utc=snapshot_started_at,
                        completed_at_utc=snapshot_completed_at,
                        total_pages=total_dep_pages,
                        record_count=total_dep_records,
                        error_message=snapshot_error_message,
                    )
                    persist_deputies_snapshot_metadata(
                        raw_writer,
                        reference_date_str,
                        final_meta,
                        write_success_marker_now=(snapshot_status_value == "COMPLETED"),
                    )
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
                snapshot_in_use_pipeline_run_id = pipeline_run_id
                snapshot_in_use_path = deputies_run_dir(
                    reference_date_str, pipeline_run_id
                )
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
                try:
                    failed_status = (
                        "PARTIALLY_COMPLETED" if pages_written > 0 else "FAILED"
                    )
                    failed_meta = build_deputies_snapshot_metadata(
                        pipeline_run_id=pipeline_run_id,
                        execution_id=snapshot_execution_id,
                        reference_date=reference_date_str,
                        reference_timezone=reference_tz_name,
                        status=failed_status,
                        started_at_utc=snapshot_started_at,
                        completed_at_utc=None,
                        total_pages=pages_written,
                        record_count=len(collected),
                        error_message=f"{type(collect_err).__name__}: {str(collect_err)}",
                    )
                    persist_deputies_snapshot_metadata(
                        raw_writer,
                        reference_date_str,
                        failed_meta,
                        write_success_marker_now=False,
                    )
                except Exception as failed_meta_err:
                    log_structured(
                        logger,
                        "warning",
                        "Failed to persist FAILED deputies snapshot metadata.",
                        pipeline_run_id=pipeline_run_id,
                        reference_date=reference_date_str,
                        error=str(failed_meta_err),
                        error_type=type(failed_meta_err).__name__,
                    )
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

        months_payload_json = json.dumps(month_list)
        run_for_ceap_manifest = registry.get_run(pipeline_run_id) or {}
        started_manifest_utc = str(run_for_ceap_manifest.get("started_at") or now.isoformat())
        manifest_path_early = ceap_run_metadata_path(pipeline_run_id)

        if total_deputies > 0:
            try:
                projected_init_expected = total_deputies * len(month_list) if month_list else 0
                init_te = max(
                    int(run_for_ceap_manifest.get("total_tasks_expected", 0) or 0),
                    int(total_queued or 0),
                    int(projected_init_expected),
                )
                init_ceap_meta = build_ceap_dispatcher_run_metadata(
                    pipeline_run_id=pipeline_run_id,
                    mode=mode,
                    status="RUNNING",
                    started_at_utc=started_manifest_utc,
                    finished_at_utc=None,
                    failed_at_utc=None,
                    total_tasks_expected=init_te,
                    total_tasks_queued=total_queued,
                    total_tasks_pending=0,
                    target_year=target_year,
                    months_to_process_json=months_payload_json,
                    enqueue_phase_complete=False,
                    deputies_snapshot_date=snapshot_in_use_date,
                    deputies_snapshot_path=snapshot_in_use_path,
                    deputies_snapshot_record_count=snapshot_in_use_record_count,
                    deputies_snapshot_status=snapshot_status_value,
                    deputies_snapshot_pipeline_run_id=snapshot_in_use_pipeline_run_id,
                    total_tasks_success=int(
                        run_for_ceap_manifest.get("total_tasks_success", 0) or 0
                    ),
                    total_tasks_failed=int(
                        run_for_ceap_manifest.get("total_tasks_failed", 0) or 0
                    ),
                    total_tasks_poison=int(
                        run_for_ceap_manifest.get("total_tasks_poison", 0) or 0
                    ),
                    total_tasks_running=int(
                        run_for_ceap_manifest.get("total_tasks_running", 0) or 0
                    ),
                    max_tasks_per_dispatch=max_per_tick,
                )
                mp_initial, _ = persist_ceap_dispatcher_run_metadata(
                    raw_writer,
                    init_ceap_meta,
                    write_success_marker_now=False,
                )
                log_structured(
                    logger,
                    "info",
                    "CEAP run manifest initial persisted in raw.",
                    pipeline_run_id=pipeline_run_id,
                    manifest_path=mp_initial,
                    mode=mode,
                )
            except Exception as mw_err:
                log_structured(
                    logger,
                    "warning",
                    "Failed to persist CEAP run manifest in raw.",
                    pipeline_run_id=pipeline_run_id,
                    manifest_path=manifest_path_early,
                    mode=mode,
                    manifest_stage="initial",
                    error=str(mw_err),
                    error_type=type(mw_err).__name__,
                )

        queue_client = None
        if total_deputies > 0:
            queue_client = prepare_queue_client_for_dispatch(
                queue_name,
                logger=logger,
                pipeline_run_id=pipeline_run_id,
                mode=mode,
            )

        while remaining > 0 and total_deputies > 0 and not abort_enqueue:
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

            while idx < len(chunk) and remaining > 0 and not abort_enqueue:
                try:
                    dep_id = int(chunk[idx]["id"])
                except (KeyError, TypeError, ValueError):
                    idx += 1
                    month_idx = 0
                    continue

                while month_idx < len(month_list) and remaining > 0 and not abort_enqueue:
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
                    assert queue_client is not None
                    try:
                        send_json_message_with_client(
                            queue_client,
                            wm.to_json(),
                            logger=logger,
                            pipeline_run_id=pipeline_run_id,
                            mode=mode,
                            queue_name=queue_name,
                            id_deputado=dep_id,
                            ano=target_year,
                            mes=mes,
                        )
                    except Exception as send_err:
                        enqueue_abort_error = send_err
                        abort_enqueue = True
                        break

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

                if abort_enqueue:
                    break

                if month_idx >= len(month_list):
                    month_idx = 0
                    idx += 1

            if abort_enqueue:
                break

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

        if abort_enqueue and enqueue_abort_error is not None:
            et = type(enqueue_abort_error).__name__
            emessage = str(enqueue_abort_error)
            pause_status = "PARTIAL" if total_queued > 0 else "FAILED"
            raw_manifest_fail_status = (
                "PARTIALLY_COMPLETED" if total_queued > 0 else "FAILED"
            )
            log_structured(
                logger,
                "error",
                "Enqueue phase aborted after queue send failure; run will resume on next tick.",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                enqueue_aborted=True,
                error_type=et,
                error_message=emessage,
                total_queued=total_queued,
                queued_this_tick=queued_this_tick,
                remaining=remaining,
                next_pagina=pagina,
                next_idx=idx,
                next_month_idx=month_idx,
            )
            try:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "status": pause_status,
                        "enqueue_phase_complete": False,
                        "next_pagina": pagina,
                        "next_idx": idx,
                        "next_month_idx": month_idx,
                        "total_tasks_queued": total_queued,
                        "last_error": f"{et}: {emessage}",
                        "error_type": et,
                        "error_message": emessage,
                        "failed_at": datetime.now(UTC).isoformat(),
                        "queued_this_tick": queued_this_tick,
                        "remaining": remaining,
                    }
                )
            except Exception as upsert_err:
                log_structured(
                    logger,
                    "error",
                    "Failed to persist PARTIAL/FAILED state after enqueue abort.",
                    pipeline_run_id=pipeline_run_id,
                    error=str(upsert_err),
                    error_type=type(upsert_err).__name__,
                )

            failed_wall = datetime.now(UTC).isoformat()
            abort_manifest_path_default = manifest_path_early
            try:
                rrf_abort = registry.get_run(pipeline_run_id) or {}
                pc_abort = parts.count_statuses_by_run(pipeline_run_id)
                te_abort = int(rrf_abort.get("total_tasks_expected", 0) or 0)
                if te_abort <= 0:
                    te_abort = (
                        pc_abort["queued"]
                        + pc_abort["running"]
                        + pc_abort["success"]
                        + pc_abort["failed"]
                        + pc_abort["pending"]
                        + pc_abort["poison"]
                        + pc_abort["stale"]
                        + pc_abort["other"]
                    )
                tp_abort = int(pc_abort.get("pending", 0) or 0)

                abort_meta = build_ceap_dispatcher_run_metadata(
                    pipeline_run_id=pipeline_run_id,
                    mode=mode,
                    status=raw_manifest_fail_status,
                    started_at_utc=str(rrf_abort.get("started_at") or started_manifest_utc),
                    finished_at_utc=None,
                    failed_at_utc=failed_wall,
                    total_tasks_expected=te_abort,
                    total_tasks_queued=total_queued,
                    total_tasks_pending=tp_abort,
                    target_year=target_year,
                    months_to_process_json=months_payload_json,
                    enqueue_phase_complete=False,
                    deputies_snapshot_date=snapshot_in_use_date,
                    deputies_snapshot_path=snapshot_in_use_path,
                    deputies_snapshot_record_count=snapshot_in_use_record_count,
                    deputies_snapshot_status=snapshot_status_value,
                    deputies_snapshot_pipeline_run_id=snapshot_in_use_pipeline_run_id,
                    error_type=et,
                    error_message=emessage,
                    total_tasks_success=int(rrf_abort.get("total_tasks_success", 0) or 0),
                    total_tasks_failed=int(rrf_abort.get("total_tasks_failed", 0) or 0),
                    total_tasks_poison=int(rrf_abort.get("total_tasks_poison", 0) or 0),
                    total_tasks_running=int(rrf_abort.get("total_tasks_running", 0) or 0),
                    max_tasks_per_dispatch=max_per_tick,
                    total_raw_files_written=int(pc_abort.get("pages_written_sum", 0) or 0),
                    total_records_collected=int(pc_abort.get("record_count_sum", 0) or 0),
                )
                mp_fail, _ = persist_ceap_dispatcher_run_metadata(
                    raw_writer,
                    abort_meta,
                    write_success_marker_now=False,
                )
                log_structured(
                    logger,
                    "warning",
                    "CEAP run manifest updated as failed in raw.",
                    pipeline_run_id=pipeline_run_id,
                    manifest_path=mp_fail,
                    mode=mode,
                    manifest_status=raw_manifest_fail_status,
                    error_type=et,
                    error_message=emessage,
                )
            except Exception as raw_abort_err:
                log_structured(
                    logger,
                    "warning",
                    "Failed to persist CEAP run manifest in raw.",
                    pipeline_run_id=pipeline_run_id,
                    manifest_path=abort_manifest_path_default,
                    mode=mode,
                    manifest_stage="enqueue_abort_failure",
                    error=str(raw_abort_err),
                    error_type=type(raw_abort_err).__name__,
                )

        enqueue_walked_full_list = (
            (not abort_enqueue)
            and total_deputies > 0
            and (pagina - 1) * _DEPUTIES_CHUNK_SIZE >= total_deputies
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
            enqueue_aborted=abort_enqueue,
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

        if abort_enqueue:
            enqueue_complete = False

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
            # While still enqueuing, prefer the universe size (deputies × months).
            # ``total_queued`` only reflects the messages dispatched so far in
            # this tick, which underestimates ``total_tasks_expected`` early on
            # and made metadata.json show 1000 (= max-per-tick) instead of 1026
            # for daily/513×2.
            projected_expected = (
                int(total_deputies) * len(month_list)
                if total_deputies > 0 and month_list
                else 0
            )
            total_tasks_expected = max(
                total_queued, total_tasks_expected, projected_expected
            )

        all_finished = (
            total_tasks_running == 0
            and total_tasks_queued_state == 0
            and total_tasks_pending == 0
            and total_tasks_stale_state == 0
            and total_tasks_other_state == 0
        )

        completed_at_iso: str | None = None
        last_error_text: str | None = None
        run_status = str(run_refresh.get("status", "QUEUING"))

        if not abort_enqueue:
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
                # Persist current QUEUED partition count from Table, not cumulative enqueue depth.
                "total_tasks_queued": total_tasks_queued_state,
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
                "deputies_snapshot_pipeline_run_id": snapshot_in_use_pipeline_run_id,
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
        else:
            log_structured(
                logger,
                "info",
                "[BEFORE_FINAL_UPSERT]",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                upsert_stage="post_abort_counters",
                run_status=str(run_refresh.get("status", "")),
                enqueue_aborted=True,
                total_tasks_expected=total_tasks_expected,
                total_tasks_success=total_tasks_success,
                total_tasks_failed=total_tasks_failed,
            )
            try:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "idle_enqueue_ticks": idle_ticks,
                        "enqueue_phase_complete": False,
                        "total_tasks_expected": total_tasks_expected,
                        "total_tasks_success": total_tasks_success,
                        "total_tasks_failed": total_tasks_failed,
                        "total_tasks_running": total_tasks_running,
                        "total_tasks_pending": total_tasks_pending,
                        "total_tasks_poison": total_tasks_poison,
                    }
                )
            except Exception as counter_err:
                log_structured(
                    logger,
                    "warning",
                    "Failed merge of partition counters after enqueue abort.",
                    pipeline_run_id=pipeline_run_id,
                    error=str(counter_err),
                    error_type=type(counter_err).__name__,
                )
            log_structured(
                logger,
                "info",
                "[AFTER_FINAL_UPSERT]",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                upsert_stage="post_abort_counters",
                run_status=str(run_refresh.get("status", "")),
                enqueue_aborted=True,
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

        # ---------------------------------------------------------------------
        # Raw manifest: always derive task counters from IngestionState ground
        # truth (current OR last pipeline_run_id). Registry rows can lag one tick
        # behind workers; a second Table scan here keeps metadata.json aligned.
        # ---------------------------------------------------------------------
        pc_manifest = parts.count_statuses_by_run(pipeline_run_id)
        eq_done_m = bool(run_done.get("enqueue_phase_complete"))
        te_manifest = (
            pc_manifest["queued"]
            + pc_manifest["running"]
            + pc_manifest["success"]
            + pc_manifest["failed"]
            + pc_manifest["poison"]
            + pc_manifest["pending"]
            + pc_manifest["stale"]
            + pc_manifest["other"]
            if eq_done_m
            else int(run_done.get("total_tasks_expected", 0) or 0)
        )

        strict_completed = (
            eq_done_m
            and te_manifest > 0
            and pc_manifest["success"] == te_manifest
            and pc_manifest["failed"] == 0
            and pc_manifest["pending"] == 0
            and pc_manifest["poison"] == 0
            and pc_manifest["running"] == 0
            and pc_manifest["queued"] == 0
            and pc_manifest["stale"] == 0
            and pc_manifest["other"] == 0
        )
        manifest_status_final = (
            "COMPLETED"
            if strict_completed
            else (str(final_status or "").upper() or "UNKNOWN")
        )
        fin_iso = str(
            completed_at_iso or run_done.get("completed_at") or ""
        ).strip()
        if strict_completed and not fin_iso:
            fin_iso = datetime.now(UTC).isoformat()
        fai_iso = str(run_done.get("failed_at", "") or "").strip()
        finished_at_utc_final = (
            fin_iso
            if (manifest_status_final == "COMPLETED" and fin_iso)
            else None
        )
        failed_at_utc_final = None
        if manifest_status_final in ("FAILED", "PARTIAL", "PARTIALLY_COMPLETED"):
            failed_at_utc_final = fai_iso if fai_iso else None

        # Align _runs with Table truth when workers finished but status was stuck (e.g. QUEUED).
        if strict_completed and str(run_done.get("status", "")).upper() != "COMPLETED":
            prev_failed_at = str(run_done.get("failed_at", "") or "")
            prev_last_error = str(run_done.get("last_error", "") or "")
            prev_status = str(run_done.get("status", "")).upper()
            recover_patch: dict[str, Any] = {
                "pipeline_run_id": pipeline_run_id,
                "status": "COMPLETED",
                "completed_at": fin_iso,
                "failed_at": "",
                "last_error": "",
                "total_tasks_expected": te_manifest,
                "total_tasks_success": pc_manifest["success"],
                "total_tasks_failed": 0,
                "total_tasks_pending": 0,
                "total_tasks_poison": 0,
                "total_tasks_running": 0,
                "total_tasks_queued": pc_manifest["queued"],
            }
            if prev_failed_at or prev_last_error or prev_status in {"FAILED", "PARTIAL"}:
                recover_patch["last_recovered_at"] = datetime.now(UTC).isoformat()
            try:
                registry.upsert_run(recover_patch)
                run_done = registry.get_run(pipeline_run_id) or run_done
            except Exception as recover_err:
                log_structured(
                    logger,
                    "warning",
                    "Could not upsert COMPLETED run state after Table-grounded manifest check.",
                    pipeline_run_id=pipeline_run_id,
                    error=str(recover_err),
                    error_type=type(recover_err).__name__,
                )

        months_table_json = (
            str(run_done.get("months_to_process"))
            if run_done.get("months_to_process")
            else months_payload_json
        )
        err_type_final = run_done.get("error_type")
        err_msg_final = run_done.get("last_error")
        if manifest_status_final == "COMPLETED":
            err_type_final = None
            err_msg_final = None

        write_success_now = (
            manifest_status_final == "COMPLETED" and strict_completed
        )
        final_mp_default = ceap_run_metadata_path(pipeline_run_id)
        try:
            final_ceap_doc = build_ceap_dispatcher_run_metadata(
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                status=manifest_status_final,
                started_at_utc=str(
                    run_done.get("started_at") or started_manifest_utc
                ),
                finished_at_utc=finished_at_utc_final,
                failed_at_utc=failed_at_utc_final,
                total_tasks_expected=int(te_manifest),
                total_tasks_queued=int(pc_manifest["queued"]),
                total_tasks_pending=int(pc_manifest["pending"]),
                target_year=target_year,
                months_to_process_json=months_table_json,
                enqueue_phase_complete=bool(
                    run_done.get("enqueue_phase_complete")
                ),
                deputies_snapshot_date=str(
                    run_done.get("deputies_snapshot_date")
                    or snapshot_in_use_date
                    or ""
                ),
                deputies_snapshot_path=str(
                    run_done.get("deputies_snapshot_path")
                    or snapshot_in_use_path
                    or ""
                ),
                deputies_snapshot_record_count=int(
                    run_done.get("deputies_snapshot_record_count", 0)
                    or snapshot_in_use_record_count
                    or 0
                ),
                deputies_snapshot_status=str(
                    run_done.get("deputies_snapshot_status")
                    or snapshot_status_value
                    or ""
                ),
                deputies_snapshot_pipeline_run_id=str(
                    run_done.get("deputies_snapshot_pipeline_run_id")
                    or snapshot_in_use_pipeline_run_id
                    or ""
                ),
                error_type=str(err_type_final) if err_type_final else None,
                error_message=str(err_msg_final) if err_msg_final else None,
                total_tasks_success=int(pc_manifest["success"]),
                total_tasks_failed=int(pc_manifest["failed"]),
                total_tasks_poison=int(pc_manifest["poison"]),
                total_tasks_running=int(pc_manifest["running"]),
                max_tasks_per_dispatch=max_per_tick,
                total_raw_files_written=int(pc_manifest.get("pages_written_sum", 0) or 0),
                total_records_collected=int(pc_manifest.get("record_count_sum", 0) or 0),
            )
            mp_fin, _sp_fin = persist_ceap_dispatcher_run_metadata(
                raw_writer,
                final_ceap_doc,
                write_success_marker_now=write_success_now,
            )
            log_structured(
                logger,
                "info",
                "CEAP run manifest persisted in raw.",
                pipeline_run_id=pipeline_run_id,
                mode=mode,
                manifest_path=mp_fin,
                success_marker_written=write_success_now,
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
                manifest_path=final_mp_default,
                manifest_stage="final_summary",
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
            enqueue_aborted=abort_enqueue,
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
            enqueue_aborted=abort_enqueue,
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
            exc_t = type(e).__name__
            exc_msg = str(e)
            tq_exc = int(locals().get("total_queued") or 0)
            qtick_exc = int(locals().get("queued_this_tick") or 0)
            rem_exc = locals().get("remaining")
            exc_status = "PARTIAL" if tq_exc > 0 else "FAILED"
            fail_patch: dict[str, Any] = {
                "pipeline_run_id": pipeline_run_id,
                "status": exc_status,
                "last_error": f"{exc_t}: {exc_msg}",
                "error_type": exc_t,
                "error_message": exc_msg,
                "failed_at": datetime.now(UTC).isoformat(),
                "total_tasks_queued": tq_exc,
                "queued_this_tick": qtick_exc,
                "remaining": rem_exc,
            }
            loc = locals()
            for cursor_key, cursor_var in (
                ("next_pagina", "pagina"),
                ("next_idx", "idx"),
                ("next_month_idx", "month_idx"),
            ):
                if cursor_var not in loc:
                    continue
                cv = loc[cursor_var]
                if cv is not None:
                    fail_patch[cursor_key] = cv
            registry.upsert_run(fail_patch)
            fail_wall = str(fail_patch.get("failed_at") or datetime.now(UTC).isoformat())
            raw_stat_exc = (
                "PARTIALLY_COMPLETED" if tq_exc > 0 else "FAILED"
            )
            try:
                exc_store = os.environ.get("RAW_STORAGE_ACCOUNT_NAME")
                if exc_store:
                    loc_exc = locals()
                    rw_ex = loc_exc.get("raw_writer")
                    if rw_ex is None:
                        rw_ex = AdlsRawWriter(account_name=exc_store)
                    mj_ex = loc_exc.get("months_payload_json") or json.dumps(
                        month_list
                    )
                    r_ent = registry.get_run(pipeline_run_id) or {}
                    st_ex_mt = str(
                        loc_exc.get("started_manifest_utc")
                        or r_ent.get("started_at")
                        or datetime.now(UTC).isoformat()
                    )
                    exc_manifest = build_ceap_dispatcher_run_metadata(
                        pipeline_run_id=pipeline_run_id,
                        mode=mode,
                        status=raw_stat_exc,
                        started_at_utc=st_ex_mt,
                        finished_at_utc=None,
                        failed_at_utc=fail_wall,
                        total_tasks_expected=max(
                            int(r_ent.get("total_tasks_expected", 0) or 0),
                            tq_exc,
                        ),
                        total_tasks_queued=tq_exc,
                        total_tasks_pending=int(
                            r_ent.get("total_tasks_pending", 0) or 0
                        ),
                        target_year=target_year,
                        months_to_process_json=mj_ex,
                        enqueue_phase_complete=False,
                        deputies_snapshot_date=str(
                            loc_exc.get("snapshot_in_use_date") or ""
                        ),
                        deputies_snapshot_path=str(
                            loc_exc.get("snapshot_in_use_path") or ""
                        ),
                        deputies_snapshot_record_count=int(
                            loc_exc.get("snapshot_in_use_record_count") or 0
                        ),
                        deputies_snapshot_status=str(
                            loc_exc.get("snapshot_status_value") or ""
                        ),
                        deputies_snapshot_pipeline_run_id=str(
                            loc_exc.get("snapshot_in_use_pipeline_run_id")
                            or r_ent.get("deputies_snapshot_pipeline_run_id")
                            or ""
                        ),
                        error_type=exc_t,
                        error_message=exc_msg,
                        total_tasks_success=int(
                            r_ent.get("total_tasks_success", 0) or 0
                        ),
                        total_tasks_failed=int(
                            r_ent.get("total_tasks_failed", 0) or 0
                        ),
                        total_tasks_poison=int(
                            r_ent.get("total_tasks_poison", 0) or 0
                        ),
                        total_tasks_running=int(
                            r_ent.get("total_tasks_running", 0) or 0
                        ),
                        max_tasks_per_dispatch=max_per_tick,
                    )
                    mp_ex, _ = persist_ceap_dispatcher_run_metadata(
                        rw_ex,
                        exc_manifest,
                        write_success_marker_now=False,
                    )
                    log_structured(
                        logger,
                        "warning",
                        "CEAP run manifest updated as failed in raw.",
                        pipeline_run_id=pipeline_run_id,
                        manifest_path=mp_ex,
                        mode=mode,
                        manifest_status=raw_stat_exc,
                        manifest_stage="unhandled_exception",
                        error_type=exc_t,
                        error_message=exc_msg,
                    )
            except Exception as raw_exc_err:
                log_structured(
                    logger,
                    "warning",
                    "Failed to persist CEAP run manifest in raw.",
                    pipeline_run_id=pipeline_run_id,
                    manifest_path=ceap_run_metadata_path(pipeline_run_id),
                    mode=mode,
                    manifest_stage="unhandled_exception",
                    error=str(raw_exc_err),
                    error_type=type(raw_exc_err).__name__,
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
