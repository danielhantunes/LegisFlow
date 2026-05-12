"""Timer: eventos dispatcher (microbatch + fanout to 4 sub-endpoints).

Each tick:

1. Computes ``eventos_microbatch_YYYYMMDDHHMM`` from the current minute
   (rounded down to ``EVENTOS_DISPATCH_GRANULARITY_MIN``).
2. Acquires the eventos dispatcher lock.
3. Lists ``/eventos`` over ``[now - lookback, now]`` (window applies to
   ``dataInicio``/``dataFim`` = event start/end).
4. Persists list pages under ``raw/camara/eventos/api/list/...`` with full
   ``_audit`` envelope.
5. For every evento_id detected, enqueues 4 ``DomainWorkMessage`` (one per
   sub-endpoint).
6. Reconciles run counters from ``IngestionState`` and writes the aggregate
   ``metadata.json`` (+ ``_SUCCESS`` only when strictly completed).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import azure.functions as func

from shared.adls_writer import AdlsRawWriter
from shared.api_client import CamaraApiClient
from shared.domain_catalog import (
    DEFAULT_MICROBATCH_LOOKBACK_MINUTES,
    EVENTOS_DOMAIN,
    eventos_microbatch_run_id,
)
from shared.eventos_raw_manifest import (
    EVENTO_SUB_ENDPOINTS,
    EVENTOS_LIST_PREFIX,
    build_eventos_dispatcher_run_metadata,
    persist_eventos_run_metadata,
)
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.queue_helpers import (
    prepare_queue_client_for_dispatch,
    send_json_message_with_client,
)
from shared.queue_messages import DomainWorkMessage
from shared.raw_audit import enrich_generic_page_payload
from shared.run_registry import GenericRunRegistry

logger = get_logger()


def _state_row_key(endpoint_name: str, evento_id: str) -> str:
    """One IngestionState row per (sub_endpoint, evento_id)."""
    return f"{endpoint_name}|{evento_id}"


def _round_minute_down(now: datetime, granularity_min: int) -> datetime:
    minute = (now.minute // granularity_min) * granularity_min
    return now.replace(minute=minute, second=0, microsecond=0)


def _fmt_window_param(dt: datetime) -> str:
    return dt.date().isoformat()


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    domain = EVENTOS_DOMAIN
    now = datetime.now(UTC)
    granularity = max(
        1, int(os.getenv("EVENTOS_DISPATCH_GRANULARITY_MIN", "20"))
    )
    lookback_min = max(
        granularity,
        int(
            os.getenv(
                "EVENTOS_LOOKBACK_MINUTES",
                str(DEFAULT_MICROBATCH_LOOKBACK_MINUTES),
            )
        ),
    )
    anchor = _round_minute_down(now, granularity)
    pipeline_run_id = eventos_microbatch_run_id(
        anchor.strftime("%Y-%m-%dT%H:%M")
    )
    window_end = now
    window_start = window_end - timedelta(minutes=lookback_min)

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
            "info",
            "Eventos dispatch skipped: run already COMPLETED.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    acquired, lock_token = registry.try_acquire_dispatcher_lock(
        mode="microbatch",
        pipeline_run_id=pipeline_run_id,
        ttl_minutes=lock_ttl,
    )
    if not acquired:
        log_structured(
            logger,
            "info",
            "Eventos dispatch skipped: dispatcher lock held.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    started_at = str(run.get("started_at") or now.isoformat())
    enqueued_now = 0
    skipped_already_queued = 0
    skipped_already_success = 0
    list_pages_written = 0
    list_records_collected = 0
    detected_ids: set[str] = set()

    api = CamaraApiClient(base_url=domain.api_base_url)
    raw_writer = AdlsRawWriter(account_name=raw_account)
    list_endpoint = domain.endpoint("eventos")
    sub_endpoints = [domain.endpoint(name) for name in EVENTO_SUB_ENDPOINTS]
    list_dir = (
        f"{EVENTOS_LIST_PREFIX}/pipeline_run_id={pipeline_run_id}/"
        f"execution_id={pipeline_run_id}"
    )
    date_start = _fmt_window_param(window_start)
    date_end = _fmt_window_param(window_end)
    sub_count = len(sub_endpoints)

    try:
        if not run:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "run_type": "microbatch",
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
                }
            )

        running_meta = build_eventos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="microbatch",
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

        # 1) Listagem paginada de /eventos na janela.
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
                business_key_fields=list_endpoint.business_key_fields or ("id",),
                source_system=domain.source_system,
                api_base_url=domain.api_base_url,
                extra_audit={
                    "_window_start_utc": window_start.isoformat(),
                    "_window_end_utc": window_end.isoformat(),
                },
            )
            raw_writer.write_json(raw_path, enriched)
            list_pages_written += 1
            for item in dados:
                if isinstance(item, dict):
                    eid = item.get("id")
                    if eid is not None:
                        detected_ids.add(str(eid))
            links = payload.get("links") or []
            has_next = any(
                isinstance(li, dict) and li.get("rel") == "next" for li in links
            )
            if not has_next:
                break
            page += 1

        # 2) Fanout: enqueue 4 messages por evento.
        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        for eid in sorted(detected_ids):
            if enqueued_now >= max_messages_per_tick:
                break
            for sub_ep in sub_endpoints:
                if enqueued_now >= max_messages_per_tick:
                    break
                row = _state_row_key(sub_ep.name, eid)
                state = parts.get_partition(row) or {}
                cur_pid = str(state.get("current_pipeline_run_id", "") or "")
                cur_status = str(state.get("status", "")).upper()
                if cur_pid == pipeline_run_id and cur_status == "SUCCESS":
                    skipped_already_success += 1
                    continue
                if cur_pid == pipeline_run_id and cur_status in ("QUEUED", "RUNNING"):
                    skipped_already_queued += 1
                    continue
                execution_id = str(uuid.uuid4())
                dispatched_at = datetime.now(UTC).isoformat()
                wm = DomainWorkMessage(
                    domain=domain.name,
                    endpoint=sub_ep.name,
                    pipeline_run_id=pipeline_run_id,
                    run_type="microbatch",
                    payload={
                        "evento_id": eid,
                        "window_start_utc": window_start.isoformat(),
                        "window_end_utc": window_end.isoformat(),
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

        # 3) Reconciliar contadores.
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
        # Each detected evento yields ``sub_count`` sub-tasks.
        total_expected = max(len(detected_ids) * sub_count, total_seen)
        enqueue_phase_complete = (
            list_pages_written > 0
            and enqueued_now == 0
            and skipped_already_queued == 0
            and total_seen >= len(detected_ids) * sub_count
        )
        if (
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
            }
        )

        # 4) Manifest agregado + _SUCCESS quando estritamente concluído.
        agg_meta = build_eventos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="microbatch",
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
            "Eventos dispatch tick finished.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            list_pages_written=list_pages_written,
            list_records_collected=list_records_collected,
            total_detected=len(detected_ids),
            total_expected=total_expected,
            enqueued_now=enqueued_now,
            skipped_already_queued=skipped_already_queued,
            skipped_already_success=skipped_already_success,
            total_tasks_success=pc["success"],
            total_tasks_failed=pc["failed"],
            total_tasks_running=pc["running"],
            total_tasks_queued=pc["queued"],
            run_status_final=run_status_final,
            enqueue_phase_complete=enqueue_phase_complete,
        )
    except Exception as exc:
        failed_at = datetime.now(UTC).isoformat()
        log_structured(
            logger,
            "error",
            "Eventos dispatcher failed.",
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
                mode="microbatch",
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
