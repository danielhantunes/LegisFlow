"""Daily eventos dispatcher tick: list ``/eventos`` once per UTC day, hash-aware fanout."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, time, timedelta
from typing import Any

from .adls_writer import AdlsRawWriter
from .api_client import CamaraApiClient
from .domain_catalog import EVENTOS_DOMAIN, eventos_daily_run_id
from .eventos_raw_manifest import (
    EVENTO_SUB_ENDPOINTS,
    EVENTOS_LIST_PREFIX,
    build_eventos_dispatcher_run_metadata,
    persist_eventos_run_metadata,
)
from .generic_partition_state import GenericPartitionStateStore
from .logger import get_logger, log_structured
from .queue_helpers import prepare_queue_client_for_dispatch, send_json_message_with_client
from .queue_messages import DomainWorkMessage
from .raw_audit import enrich_generic_page_payload
from .run_registry import GenericRunRegistry
from .votacoes_api_dispatcher_logic import list_item_uid_hash

logger = get_logger()


def _state_row_key(endpoint_name: str, evento_id: str) -> str:
    return f"{endpoint_name}|{evento_id}"


def _daily_api_date_window(*, now: datetime) -> tuple[str, str, datetime, datetime]:
    """API date range: today through today + ``EVENTOS_DAILY_FUTURE_DAYS`` (inclusive)."""
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    today = now_utc.date()
    future_days = max(0, int(os.getenv("EVENTOS_DAILY_FUTURE_DAYS", "7")))
    end_d = today + timedelta(days=future_days)
    date_start = today.isoformat()
    date_end = end_d.isoformat()
    window_start = datetime.combine(today, time.min, tzinfo=UTC)
    window_end = datetime.combine(
        end_d, time.max.replace(microsecond=999999), tzinfo=UTC
    )
    return date_start, date_end, window_start, window_end


def execute_eventos_daily_tick(*, now: datetime) -> None:
    domain = EVENTOS_DOMAIN
    now = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    pipeline_run_id = eventos_daily_run_id(now.strftime("%Y-%m-%d"))
    date_start, date_end, window_start, window_end = _daily_api_date_window(now=now)

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("EVENTOS_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    lock_ttl = int(
        os.getenv("EVENTOS_LOCK_TTL_MINUTES", str(domain.lock_ttl_minutes))
    )
    max_messages_per_tick = max(
        1,
        int(os.getenv("EVENTOS_MAX_MESSAGES_PER_TICK", "1000")),
    )
    max_list_pages = int(os.getenv("EVENTOS_MAX_LIST_PAGES", "200"))

    registry = GenericRunRegistry.from_connection_string(
        conn,
        control_table,
        runs_partition_key=domain.runs_partition_key,
        locks_partition_key=domain.locks_partition_key,
        lock_row_key=domain.lock_row_key,
    )
    parts = GenericPartitionStateStore.from_connection_string(
        conn, state_table, partition_key=domain.state_partition_key
    )

    run = registry.get_run(pipeline_run_id) or {}
    run_status = str(run.get("status", "")).upper()
    if run_status == "COMPLETED":
        log_structured(
            logger,
            "debug",
            "Eventos daily dispatch skipped: run already COMPLETED.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    acquired, lock_token = registry.try_acquire_dispatcher_lock(
        mode="daily",
        pipeline_run_id=pipeline_run_id,
        ttl_minutes=lock_ttl,
    )
    if not acquired:
        log_structured(
            logger,
            "debug",
            "Eventos daily dispatch skipped: dispatcher lock held.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    started_at = str(run.get("started_at") or now.isoformat())
    enqueued_now = 0
    skipped_already_queued = 0
    skipped_same_list_hash = 0
    list_pages_written = 0
    list_records_collected = 0
    detected_ids: set[str] = set()
    fingerprints_by_id: dict[str, str] = {}

    api = CamaraApiClient(base_url=domain.api_base_url)
    raw_writer = AdlsRawWriter(account_name=raw_account)
    list_endpoint = domain.endpoint("eventos")
    sub_endpoints = [domain.endpoint(name) for name in EVENTO_SUB_ENDPOINTS]
    list_dir = (
        f"{EVENTOS_LIST_PREFIX}/pipeline_run_id={pipeline_run_id}/"
        f"execution_id={pipeline_run_id}"
    )
    bkf = list_endpoint.business_key_fields or ("id",)
    sub_count = len(sub_endpoints)

    try:
        if not run:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "run_type": "daily",
                    "status": "STARTED",
                    "domain": domain.name,
                    "window_start_utc": window_start.isoformat(),
                    "window_end_utc": window_end.isoformat(),
                    "started_at": started_at,
                    "total_tasks_expected": 0,
                    "total_tasks_queued": 0,
                    "total_tasks_success": 0,
                    "total_tasks_failed": 0,
                    "total_tasks_pending": 0,
                    "total_tasks_poison": 0,
                    "total_tasks_running": 0,
                    "total_eventos_detected": 0,
                    "enqueue_phase_complete": False,
                    "sub_endpoints": json.dumps(list(EVENTO_SUB_ENDPOINTS)),
                    "hash_strategy": domain.hash_strategy,
                    "audit_fields_applied": json.dumps(list(domain.audit_fields)),
                    "api_date_start": date_start,
                    "api_date_end": date_end,
                }
            )

        running_meta = build_eventos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="daily",
            status="RUNNING",
            started_at_utc=started_at,
            finished_at_utc=None,
            failed_at_utc=None,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            total_eventos_detected=0,
            total_tasks_expected=0,
            total_tasks_queued=0,
            total_tasks_pending=0,
            total_tasks_success=0,
            total_tasks_failed=0,
            total_tasks_poison=0,
            total_tasks_running=0,
            enqueue_phase_complete=False,
            api_base_url=domain.api_base_url,
            source_system=domain.source_system,
            hash_strategy=domain.hash_strategy,
            audit_fields_applied=domain.audit_fields,
        )
        persist_eventos_run_metadata(
            raw_writer,
            pipeline_run_id,
            running_meta,
            write_success_marker_now=False,
        )

        page = 1
        while page <= max_list_pages:
            payload, _http = api.list_eventos_page(
                page=page,
                itens=list_endpoint.items_per_page,
                date_start=date_start,
                date_end=date_end,
            )
            dados = payload.get("dados") or []
            list_records_collected += len(dados)
            raw_path = f"{list_dir}/page_{page}.json"
            enriched = enrich_generic_page_payload(
                payload,
                pipeline_run_id=pipeline_run_id,
                execution_id=pipeline_run_id,
                domain=domain.name,
                entity=list_endpoint.name,
                endpoint=list_endpoint.name,
                api_path=list_endpoint.path_template,
                raw_path=raw_path,
                page=page,
                business_key_fields=bkf,
                source_system=domain.source_system,
                api_base_url=domain.api_base_url,
                extra_audit={
                    "_window_start_utc": window_start.isoformat(),
                    "_window_end_utc": window_end.isoformat(),
                    "_api_date_start": date_start,
                    "_api_date_end": date_end,
                },
            )
            raw_writer.write_json(raw_path, enriched)
            list_pages_written += 1
            for item in dados:
                if isinstance(item, dict) and item.get("id") is not None:
                    eid = str(item.get("id"))
                    detected_ids.add(eid)
                    _uid, hsh = list_item_uid_hash(
                        domain,
                        endpoint_name=list_endpoint.name,
                        business_key_fields=bkf,
                        item=item,
                    )
                    fingerprints_by_id[eid] = hsh
            links = payload.get("links") or []
            has_next = any(
                isinstance(li, dict) and li.get("rel") == "next" for li in links
            )
            if not has_next:
                break
            page += 1

        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )

        for eid in sorted(detected_ids):
            list_hash = fingerprints_by_id.get(eid) or ""
            for sub_ep in sub_endpoints:
                if enqueued_now >= max_messages_per_tick:
                    break
                row = _state_row_key(sub_ep.name, eid)
                state = parts.get_partition(row) or {}
                cur_pid = str(state.get("current_pipeline_run_id", "") or "")
                cur_status = str(state.get("status", "")).upper()
                if cur_pid == pipeline_run_id and cur_status in ("QUEUED", "RUNNING"):
                    skipped_already_queued += 1
                    continue
                if cur_status == "SUCCESS" and list_hash and str(
                    state.get("last_list_item_hash") or ""
                ) == list_hash:
                    skipped_same_list_hash += 1
                    continue

                execution_id = str(uuid.uuid4())
                dispatched_at = datetime.now(UTC).isoformat()
                wm = DomainWorkMessage(
                    domain=domain.name,
                    endpoint=sub_ep.name,
                    pipeline_run_id=pipeline_run_id,
                    run_type="daily",
                    payload={
                        "evento_id": eid,
                        "window_start_utc": window_start.isoformat(),
                        "window_end_utc": window_end.isoformat(),
                        "list_item_hash": list_hash,
                    },
                    execution_id=execution_id,
                    dispatched_at=dispatched_at,
                )
                send_json_message_with_client(
                    queue_client,
                    wm.to_json(),
                    logger=logger,
                    domain=domain.name,
                    pipeline_run_id=pipeline_run_id,
                    endpoint=sub_ep.name,
                    evento_id=eid,
                )
                patch: dict[str, Any] = {
                    "endpoint": sub_ep.name,
                    "evento_id": eid,
                    "status": "QUEUED",
                    "current_pipeline_run_id": pipeline_run_id,
                    "last_pipeline_run_id": cur_pid,
                    "last_dispatched_at": dispatched_at,
                    "last_execution_id": execution_id,
                    "attempt_count": int(state.get("attempt_count", 0) or 0),
                    "last_error": "",
                }
                parts.upsert_partition(row, patch)
                enqueued_now += 1

        pc = parts.count_statuses_by_run(pipeline_run_id)
        total_seen = (
            pc["queued"]
            + pc["running"]
            + pc["success"]
            + pc["failed"]
            + pc["poison"]
            + pc["pending"]
            + pc["stale"]
            + pc["other"]
        )
        total_expected = max(len(detected_ids) * sub_count, total_seen)
        fanout_target = len(detected_ids) * sub_count
        fanout_decisions = (
            enqueued_now + skipped_already_queued + skipped_same_list_hash
        )
        hit_cap = enqueued_now >= max_messages_per_tick
        if not detected_ids:
            enqueue_phase_complete = bool(list_pages_written > 0)
        else:
            enqueue_phase_complete = (
                list_pages_written > 0
                and not hit_cap
                and fanout_decisions >= fanout_target
                and enqueued_now == 0
                and skipped_already_queued == 0
            )

        all_subtasks_skipped_hash = (
            enqueue_phase_complete
            and fanout_target > 0
            and skipped_same_list_hash >= fanout_target
        )

        if (
            enqueue_phase_complete
            and total_expected == 0
            and pc["failed"] == 0
            and pc["poison"] == 0
            and pc["running"] == 0
            and pc["queued"] == 0
            and pc["pending"] == 0
        ) or all_subtasks_skipped_hash:
            run_status_final = "COMPLETED"
            finished_at_utc = datetime.now(UTC).isoformat()
        elif (
            enqueue_phase_complete
            and total_expected > 0
            and pc["success"] >= total_expected
            and pc["failed"] == 0
            and pc["poison"] == 0
            and pc["running"] == 0
            and pc["queued"] == 0
            and pc["pending"] == 0
        ):
            run_status_final = "COMPLETED"
            finished_at_utc = datetime.now(UTC).isoformat()
        elif pc["running"] + pc["queued"] > 0 or enqueued_now > 0:
            run_status_final = "RUNNING"
            finished_at_utc = None
        else:
            run_status_final = "QUEUING"
            finished_at_utc = None

        registry.upsert_run(
            {
                "pipeline_run_id": pipeline_run_id,
                "status": run_status_final,
                "enqueue_phase_complete": enqueue_phase_complete,
                "total_eventos_detected": len(detected_ids),
                "total_tasks_expected": total_expected,
                "total_tasks_queued": pc["queued"],
                "total_tasks_success": pc["success"],
                "total_tasks_failed": pc["failed"],
                "total_tasks_pending": pc["pending"],
                "total_tasks_poison": pc["poison"],
                "total_tasks_running": pc["running"],
                "list_pages_written": list_pages_written,
                "list_records_collected": list_records_collected,
                "window_start_utc": window_start.isoformat(),
                "window_end_utc": window_end.isoformat(),
                "api_date_start": date_start,
                "api_date_end": date_end,
            }
        )

        agg_meta = build_eventos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="daily",
            status=run_status_final,
            started_at_utc=started_at,
            finished_at_utc=finished_at_utc,
            failed_at_utc=None,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            total_eventos_detected=len(detected_ids),
            total_tasks_expected=total_expected,
            total_tasks_queued=pc["queued"],
            total_tasks_pending=pc["pending"],
            total_tasks_success=pc["success"],
            total_tasks_failed=pc["failed"],
            total_tasks_poison=pc["poison"],
            total_tasks_running=pc["running"],
            enqueue_phase_complete=enqueue_phase_complete,
            api_base_url=domain.api_base_url,
            source_system=domain.source_system,
            hash_strategy=domain.hash_strategy,
            audit_fields_applied=domain.audit_fields,
            total_raw_files_written=list_pages_written,
            total_records_collected=list_records_collected,
        )
        persist_eventos_run_metadata(
            raw_writer,
            pipeline_run_id,
            agg_meta,
            write_success_marker_now=(run_status_final == "COMPLETED"),
        )

        log_structured(
            logger,
            "info",
            "eventos_daily completed",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            api_date_start=date_start,
            api_date_end=date_end,
            records_seen=list_records_collected,
            eventos_distinct=len(detected_ids),
            messages_enqueued=enqueued_now,
            messages_skipped_hash=skipped_same_list_hash,
            messages_skipped_queued=skipped_already_queued,
            list_pages_written=list_pages_written,
            total_tasks_expected=total_expected,
            run_status_final=run_status_final,
            enqueue_phase_complete=enqueue_phase_complete,
        )
    except Exception as exc:
        failed_at = datetime.now(UTC).isoformat()
        log_structured(
            logger,
            "error",
            "Eventos daily dispatcher failed.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            error=str(exc)[:500],
            error_type=type(exc).__name__,
        )
        try:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "status": "FAILED" if enqueued_now == 0 else "PARTIAL",
                    "last_error": f"{type(exc).__name__}: {str(exc)[:512]}",
                    "error_type": type(exc).__name__,
                    "failed_at": failed_at,
                }
            )
        except Exception:  # noqa: BLE001
            pass
        try:
            failed_meta = build_eventos_dispatcher_run_metadata(
                pipeline_run_id=pipeline_run_id,
                mode="daily",
                status="FAILED",
                started_at_utc=started_at,
                finished_at_utc=None,
                failed_at_utc=failed_at,
                window_start_utc=window_start.isoformat(),
                window_end_utc=window_end.isoformat(),
                total_eventos_detected=len(detected_ids),
                total_tasks_expected=len(detected_ids) * sub_count,
                total_tasks_queued=0,
                total_tasks_pending=0,
                total_tasks_success=0,
                total_tasks_failed=0,
                total_tasks_poison=0,
                total_tasks_running=0,
                enqueue_phase_complete=False,
                error_type=type(exc).__name__,
                error_message=str(exc)[:1024],
                api_base_url=domain.api_base_url,
                source_system=domain.source_system,
                hash_strategy=domain.hash_strategy,
                audit_fields_applied=domain.audit_fields,
                total_raw_files_written=list_pages_written,
                total_records_collected=list_records_collected,
            )
            persist_eventos_run_metadata(
                raw_writer,
                pipeline_run_id,
                failed_meta,
                write_success_marker_now=False,
            )
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        registry.release_dispatcher_lock(lock_token)
        _ = raw_account
