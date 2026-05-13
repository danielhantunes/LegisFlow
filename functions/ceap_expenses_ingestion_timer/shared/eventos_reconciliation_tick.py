"""Weekly / manual reconciliation tick for eventos (wide ``/eventos`` date window)."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from .adls_writer import AdlsRawWriter
from .api_client import CamaraApiClient
from .domain_catalog import EVENTOS_DOMAIN, eventos_reconciliation_run_id
from .eventos_raw_manifest import (
    EVENTO_SUB_ENDPOINTS,
    EVENTOS_LIST_PREFIX,
    build_eventos_dispatcher_run_metadata,
    persist_eventos_run_metadata,
    eventos_run_manifest_prefix,
)
from .generic_partition_state import GenericPartitionStateStore
from .logger import get_logger, log_structured
from .queue_helpers import prepare_queue_client_for_dispatch, send_json_message_with_client
from .queue_messages import DomainWorkMessage
from .raw_audit import enrich_generic_page_payload
from .reconciliation_fingerprints import load_fingerprints, save_fingerprints
from .run_registry import GenericRunRegistry
from .votacoes_api_dispatcher_logic import list_item_uid_hash

logger = get_logger()

_SUB_COUNT = len(EVENTO_SUB_ENDPOINTS)


def _state_row_key(endpoint_name: str, evento_id: str) -> str:
    return f"{endpoint_name}|{evento_id}"


def count_eventos_in_date_range_dry_run(
    *,
    api: CamaraApiClient,
    list_endpoint: Any,
    date_start: str,
    date_end: str,
    max_pages: int,
) -> tuple[int, int, list[str]]:
    warnings: list[str] = []
    seen: set[str] = set()
    page = 1
    while page <= max_pages:
        payload, _http = api.list_eventos_page(
            page=page,
            itens=list_endpoint.items_per_page,
            date_start=date_start,
            date_end=date_end,
        )
        dados = payload.get("dados") or []
        for item in dados:
            if isinstance(item, dict) and item.get("id") is not None:
                seen.add(str(item.get("id")))
        links = payload.get("links") or []
        has_next = any(
            isinstance(li, dict) and li.get("rel") == "next" for li in links
        )
        if not has_next:
            break
        page += 1
    if page >= max_pages:
        warnings.append("max_list_pages_reached_during_count")
    return len(seen), page, warnings


def execute_eventos_reconciliation_tick(
    *,
    now: datetime,
    date_start: str,
    date_end: str,
    recon_day: int,
    lookback_days: int,
) -> None:
    def _as_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    now_utc = _as_utc(now)
    domain = EVENTOS_DOMAIN
    pipeline_run_id = eventos_reconciliation_run_id(now_utc.strftime("%Y-%m-%d"))
    window_start = datetime.fromisoformat(f"{date_start}T00:00:00+00:00")
    window_end_cap = datetime.fromisoformat(f"{date_end}T23:59:59.999999+00:00")
    window_end = min(now_utc, window_end_cap)

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("EVENTOS_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    lock_ttl = int(os.getenv("EVENTOS_LOCK_TTL_MINUTES", str(domain.lock_ttl_minutes)))
    max_messages_per_tick = max(
        1, int(os.getenv("EVENTOS_MAX_MESSAGES_PER_TICK", "1000"))
    )
    max_list_pages = int(os.getenv("EVENTOS_MAX_LIST_PAGES", "200"))
    max_pages_tick = max(1, int(os.getenv("EVENTOS_RECON_MAX_PAGES_PER_TICK", "40")))

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

    run_pre = registry.get_run(pipeline_run_id) or {}
    if str(run_pre.get("status", "")).upper() == "COMPLETED":
        log_structured(
            logger,
            "info",
            "Eventos reconciliation skipped: run already COMPLETED.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    acquired, lock_token = registry.try_acquire_dispatcher_lock(
        mode="reconciliation",
        pipeline_run_id=pipeline_run_id,
        ttl_minutes=lock_ttl,
    )
    if not acquired:
        log_structured(
            logger,
            "info",
            "Eventos reconciliation skipped: dispatcher lock held.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    run = registry.get_run(pipeline_run_id) or {}
    if str(run.get("status", "")).upper() == "COMPLETED":
        registry.release_dispatcher_lock(lock_token)
        log_structured(
            logger,
            "info",
            "Eventos reconciliation skipped after lock: COMPLETED.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    started_at = str(run.get("started_at") or now_utc.isoformat())
    listing_already = bool(run.get("recon_listing_complete"))
    resume_page = int(run.get("recon_list_next_page") or 1)
    prev_list_pages = int(run.get("list_pages_written") or 0)
    prev_list_recs = int(run.get("list_records_collected") or 0)

    enqueued_now = 0
    skipped_already_queued = 0
    skipped_same_list_hash = 0
    list_pages_this_tick = 0
    list_recs_this_tick = 0

    api = CamaraApiClient(base_url=domain.api_base_url)
    raw_writer = AdlsRawWriter(account_name=raw_account)
    list_endpoint = domain.endpoint("eventos")
    sub_endpoints = [domain.endpoint(name) for name in EVENTO_SUB_ENDPOINTS]
    list_dir = (
        f"{EVENTOS_LIST_PREFIX}/pipeline_run_id={pipeline_run_id}/"
        f"execution_id={pipeline_run_id}"
    )
    manifest_prefix = eventos_run_manifest_prefix(pipeline_run_id)
    fingerprints: dict[str, dict[str, str]] = dict(
        load_fingerprints(raw_writer, manifest_prefix)
    )

    try:
        if not run:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "run_type": "reconciliation",
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
                    "recon_listing_complete": False,
                    "recon_list_next_page": 1,
                    "date_start": date_start,
                    "date_end": date_end,
                    "reconciliation_lookback_days": lookback_days,
                }
            )

        extras_base = {
            "target_year": None,
            "date_start": date_start,
            "date_end": date_end,
            "reconciliation_day": recon_day,
            "reconciliation_lookback_days": lookback_days,
            "reconciliation_past_days": int(
                os.getenv("EVENTOS_RECONCILIATION_PAST_DAYS", "7")
            ),
            "reconciliation_future_days": int(
                os.getenv("EVENTOS_RECONCILIATION_FUTURE_DAYS", "30")
            ),
        }
        running_meta = build_eventos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="reconciliation",
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
            manifest_extras=extras_base,
        )
        persist_eventos_run_metadata(
            raw_writer,
            pipeline_run_id,
            running_meta,
            write_success_marker_now=False,
        )

        listing_complete = listing_already
        last_page_fetched = resume_page - 1
        bkf = list_endpoint.business_key_fields or ("id",)

        if not listing_already:
            page = max(1, resume_page)
            end_page_limit = min(max_list_pages, page + max_pages_tick - 1)
            while page <= end_page_limit:
                payload, _http = api.list_eventos_page(
                    page=page,
                    itens=list_endpoint.items_per_page,
                    date_start=date_start,
                    date_end=date_end,
                )
                dados = payload.get("dados") or []
                list_recs_this_tick += len(dados)
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
                    },
                )
                raw_writer.write_json(raw_path, enriched)
                list_pages_this_tick += 1
                for item in dados:
                    if isinstance(item, dict) and item.get("id") is not None:
                        eid = str(item.get("id"))
                        uid, hsh = list_item_uid_hash(
                            domain,
                            endpoint_name=list_endpoint.name,
                            business_key_fields=bkf,
                            item=item,
                        )
                        fingerprints[eid] = {"uid": uid, "hash": hsh}
                links = payload.get("links") or []
                has_next = any(
                    isinstance(li, dict) and li.get("rel") == "next" for li in links
                )
                last_page_fetched = page
                if not has_next:
                    listing_complete = True
                    break
                page += 1

            if listing_complete:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "recon_listing_complete": True,
                        "recon_list_next_page": 1,
                    }
                )
            else:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "recon_listing_complete": False,
                        "recon_list_next_page": last_page_fetched + 1,
                    }
                )

        if fingerprints:
            save_fingerprints(raw_writer, manifest_prefix, fingerprints)

        total_list_pages = prev_list_pages + list_pages_this_tick
        total_list_recs = prev_list_recs + list_recs_this_tick

        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )

        all_ids = sorted(fingerprints.keys())
        for eid in all_ids:
            if enqueued_now >= max_messages_per_tick:
                break
            fp = fingerprints.get(eid) or {}
            list_hash = str(fp.get("hash") or "")
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
                    run_type="reconciliation",
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
                parts.upsert_partition(
                    row,
                    {
                        "endpoint": sub_ep.name,
                        "evento_id": eid,
                        "status": "QUEUED",
                        "current_pipeline_run_id": pipeline_run_id,
                        "last_pipeline_run_id": cur_pid,
                        "last_dispatched_at": dispatched_at,
                        "last_execution_id": execution_id,
                        "attempt_count": int(state.get("attempt_count", 0) or 0),
                        "last_error": "",
                    },
                )
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
        n_ids = len(all_ids)
        total_expected = max(n_ids * _SUB_COUNT, total_seen)
        fanout_target = n_ids * _SUB_COUNT
        fanout_decisions = (
            enqueued_now + skipped_already_queued + skipped_same_list_hash
        )
        hit_cap = enqueued_now >= max_messages_per_tick

        if not listing_complete:
            enqueue_phase_complete = False
        elif n_ids == 0:
            enqueue_phase_complete = True
        else:
            enqueue_phase_complete = (
                not hit_cap
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
                "total_eventos_detected": n_ids,
                "total_tasks_expected": total_expected,
                "total_tasks_queued": pc["queued"],
                "total_tasks_success": pc["success"],
                "total_tasks_failed": pc["failed"],
                "total_tasks_pending": pc["pending"],
                "total_tasks_poison": pc["poison"],
                "total_tasks_running": pc["running"],
                "list_pages_written": total_list_pages,
                "list_records_collected": total_list_recs,
                "window_start_utc": window_start.isoformat(),
                "window_end_utc": window_end.isoformat(),
                "recon_listing_complete": listing_complete,
            }
        )

        agg_meta = build_eventos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="reconciliation",
            status=run_status_final,
            started_at_utc=started_at,
            finished_at_utc=finished_at_utc,
            failed_at_utc=None,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            total_eventos_detected=n_ids,
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
            total_raw_files_written=total_list_pages,
            total_records_collected=total_list_recs,
            manifest_extras=extras_base,
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
            "Eventos reconciliation tick finished.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            date_start=date_start,
            date_end=date_end,
            listing_complete=listing_complete,
            total_detected=n_ids,
            enqueued_now=enqueued_now,
            skipped_same_list_hash=skipped_same_list_hash,
            run_status_final=run_status_final,
        )
    except Exception as exc:
        failed_at = datetime.now(UTC).isoformat()
        log_structured(
            logger,
            "error",
            "Eventos reconciliation failed.",
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
                    "failed_at": failed_at,
                }
            )
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        registry.release_dispatcher_lock(lock_token)
        _ = raw_account
