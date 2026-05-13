"""Single-tick execution for votações dispatcher (timer + manual reconciliation).

Shared by ``votacoes_api_dispatcher`` and ``fn_start_votacoes_reconciliation``.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Any

from .adls_writer import AdlsRawWriter
from .api_client import CamaraApiClient
from .domain_catalog import DomainSpec
from .generic_partition_state import GenericPartitionStateStore
from .logger import get_logger, log_structured
from .queue_helpers import prepare_queue_client_for_dispatch, send_json_message_with_client
from .queue_messages import DomainWorkMessage
from .raw_audit import enrich_generic_page_payload
from .run_registry import GenericRunRegistry
from .votacoes_api_dispatcher_logic import list_item_uid_hash, reenqueue_stale_votacoes_tasks
from .votacoes_microbatch_cursor import (
    last_processed_votacao_id_int,
    votacao_id_sort_key,
)
from .votacoes_raw_manifest import (
    VOTACOES_LIST_PREFIX,
    build_votacoes_dispatcher_run_metadata,
    load_votacoes_discovered_fingerprints,
    persist_votacoes_run_metadata,
    save_votacoes_discovered_fingerprints,
    votacoes_run_metadata_path,
    votacoes_run_success_path,
)

logger = get_logger()


def state_row_key_votacao_votos(votacao_id: str) -> str:
    return f"votacao_votos|{votacao_id}"


def execute_votacoes_ingestion_tick(
    *,
    domain: DomainSpec,
    now: datetime,
    registry: GenericRunRegistry,
    parts: GenericPartitionStateStore,
    raw_account: str,
    queue_name: str,
    lock_ttl: int,
    max_messages_per_tick: int,
    max_list_pages: int,
    max_pages_tick: int,
    stale_after: int,
    pipeline_run_id: str,
    mode: str,
    run_type_label: str,
    date_start: str,
    date_end: str,
    window_start: datetime,
    window_end: datetime,
    target_year: int,
    recon_day: int,
) -> dict[str, Any]:
    """Acquires dispatcher lock, runs list + fanout + metadata, releases lock.

    Returns a small summary dict (useful for HTTP responses). Raises on failure
    after marking the run PARTIAL/FAILED when applicable.
    """
    run = registry.get_run(pipeline_run_id) or {}
    run_status = str(run.get("status", "")).upper()
    if run_status == "COMPLETED":
        log_structured(
            logger,
            "info",
            "Votacoes dispatch skipped: run already COMPLETED.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            mode=mode,
        )
        return {"skipped": True, "reason": "already_completed"}

    acquired, lock_token = registry.try_acquire_dispatcher_lock(
        mode=mode,
        pipeline_run_id=pipeline_run_id,
        ttl_minutes=lock_ttl,
    )
    if not acquired:
        log_structured(
            logger,
            "info",
            "Votacoes dispatch skipped: dispatcher lock held.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            mode=mode,
        )
        return {"skipped": True, "reason": "lock_held"}

    started_at = str(run.get("started_at") or now.isoformat())
    enqueued_now = 0
    skipped_already_queued = 0
    skipped_unchanged_hash = 0
    list_pages_written = 0
    list_records_collected = 0
    last_seen_votacao_id = str(run.get("last_seen_votacao_id") or "")
    last_seen_dthr = str(run.get("last_seen_dataHoraRegistro") or "")
    lp_int = last_processed_votacao_id_int(parts) if mode == "microbatch" else 0

    resume_page = int(run.get("recon_list_next_page") or 1) if mode == "reconciliation" else 1

    api = CamaraApiClient(base_url=domain.api_base_url)
    raw_writer = AdlsRawWriter(account_name=raw_account)
    list_endpoint = domain.endpoint("votacoes")
    votos_endpoint = domain.endpoint("votacao_votos")
    reference_date = date_end
    list_dir = (
        f"{VOTACOES_LIST_PREFIX}/reference_date={reference_date}/"
        f"pipeline_run_id={pipeline_run_id}/execution_id={pipeline_run_id}"
    )

    fingerprints: dict[str, dict[str, str]] = {}
    if mode == "reconciliation":
        fingerprints = dict(load_votacoes_discovered_fingerprints(raw_writer, pipeline_run_id))

    run_status_final = "RUNNING"
    try:
        if not run:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "run_type": run_type_label,
                    "status": "STARTED",
                    "domain": domain.name,
                    "window_start_utc": window_start.isoformat(),
                    "window_end_utc": window_end.isoformat(),
                    "started_at": started_at,
                    "target_year": target_year,
                    "date_start": date_start,
                    "date_end": date_end,
                    "total_tasks_expected": 0,
                    "total_tasks_queued": 0,
                    "total_tasks_success": 0,
                    "total_tasks_failed": 0,
                    "total_tasks_pending": 0,
                    "total_tasks_poison": 0,
                    "total_tasks_running": 0,
                    "total_votacoes_detected": 0,
                    "enqueue_phase_complete": False,
                    "hash_strategy": domain.hash_strategy,
                    "audit_fields_applied": json.dumps(list(domain.audit_fields)),
                    "recon_listing_complete": mode != "reconciliation",
                    "recon_list_next_page": resume_page if mode == "reconciliation" else 1,
                }
            )

        running_meta = build_votacoes_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            status="RUNNING",
            started_at_utc=started_at,
            finished_at_utc=None,
            failed_at_utc=None,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            total_votacoes_detected=0,
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
            manifest_extras={
                "target_year": target_year,
                "date_start": date_start,
                "date_end": date_end,
                "microbatch_date_window_days": int(
                    os.getenv("VOTACOES_MICROBATCH_DATE_WINDOW_DAYS", "2")
                )
                if mode == "microbatch"
                else None,
                "microbatch_max_list_pages": int(
                    os.getenv("VOTACOES_MICROBATCH_MAX_LIST_PAGES", "15")
                )
                if mode == "microbatch"
                else None,
                "last_processed_votacao_id_cursor": lp_int if mode == "microbatch" else None,
                "reconciliation_day": recon_day if mode == "reconciliation" else None,
                "watermark_field": "dataHoraRegistro",
                "offset_field": "id",
            },
        )
        persist_votacoes_run_metadata(
            raw_writer,
            pipeline_run_id,
            running_meta,
            write_success_marker_now=False,
        )

        listing_complete = False
        last_page_fetched = resume_page - 1
        if mode == "reconciliation":
            page = max(1, resume_page)
            end_page_limit = min(max_list_pages, page + max_pages_tick - 1)
        else:
            page = 1
            mb_cap = max(1, int(os.getenv("VOTACOES_MICROBATCH_MAX_LIST_PAGES", "15")))
            end_page_limit = min(max_list_pages, mb_cap)

        bkf = list_endpoint.business_key_fields or ("id",)

        while page <= end_page_limit:
            ordenar = "id" if mode == "microbatch" else "dataHoraRegistro"
            ordem = "ASC" if mode == "microbatch" else "DESC"
            payload, _http = api.list_votacoes_page(
                page=page,
                itens=list_endpoint.items_per_page,
                date_start=date_start,
                date_end=date_end,
                ordenar_por=ordenar,
                ordem=ordem,
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
                },
            )
            raw_writer.write_json(raw_path, enriched)
            list_pages_written += 1
            for item in dados:
                if isinstance(item, dict) and item.get("id") is not None:
                    vid_s = str(item.get("id"))
                    if mode == "microbatch" and vid_s.isdigit() and int(vid_s) <= lp_int:
                        continue
                    uid, hsh = list_item_uid_hash(
                        domain,
                        endpoint_name=list_endpoint.name,
                        business_key_fields=bkf,
                        item=item,
                    )
                    fingerprints[vid_s] = {"uid": uid, "hash": hsh}
                    last_seen_votacao_id = vid_s
                    dhr = item.get("dataHoraRegistro") or item.get("data")
                    if dhr:
                        last_seen_dthr = str(dhr)
            links = payload.get("links") or []
            has_next = any(
                isinstance(li, dict) and li.get("rel") == "next" for li in links
            )
            last_page_fetched = page
            if not has_next:
                listing_complete = True
                break
            page += 1

        if mode == "reconciliation":
            recon_listing_complete = listing_complete
            if recon_listing_complete:
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
        else:
            recon_listing_complete = True

        if fingerprints:
            save_votacoes_discovered_fingerprints(raw_writer, pipeline_run_id, fingerprints)

        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )

        stale_requeued = reenqueue_stale_votacoes_tasks(
            parts=parts,
            queue_client=queue_client,
            pipeline_run_id=pipeline_run_id,
            votos_endpoint_name=votos_endpoint.name,
            stale_after_minutes=stale_after,
            now=now,
            logger_=logger,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            run_type=run_type_label,
        )

        all_ids = sorted(fingerprints.keys(), key=votacao_id_sort_key)
        for vid in all_ids:
            if enqueued_now >= max_messages_per_tick:
                break
            row = state_row_key_votacao_votos(vid)
            state = parts.get_partition(row) or {}
            cur_pid = str(state.get("current_pipeline_run_id", "") or "")
            cur_status = str(state.get("status", "")).upper()
            if cur_pid == pipeline_run_id and cur_status in ("QUEUED", "RUNNING"):
                skipped_already_queued += 1
                continue
            fp_entry = fingerprints.get(vid) or {}
            list_uid = str(fp_entry.get("uid", "") or "")
            list_hash = str(fp_entry.get("hash", "") or "")
            if (
                cur_status == "SUCCESS"
                and str(state.get("last_votacao_list_record_uid") or "") == list_uid
                and str(state.get("last_votacao_list_record_hash") or "") == list_hash
            ):
                skipped_unchanged_hash += 1
                continue

            execution_id = str(uuid.uuid4())
            dispatched_at = now.isoformat()
            wm = DomainWorkMessage(
                domain=domain.name,
                endpoint=votos_endpoint.name,
                pipeline_run_id=pipeline_run_id,
                run_type=run_type_label,
                payload={
                    "votacao_id": vid,
                    "window_start_utc": window_start.isoformat(),
                    "window_end_utc": window_end.isoformat(),
                    "list_record_uid": list_uid,
                    "list_record_hash": list_hash,
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
                endpoint=votos_endpoint.name,
                votacao_id=vid,
            )
            parts.upsert_partition(
                row,
                {
                    "endpoint": votos_endpoint.name,
                    "votacao_id": vid,
                    "status": "QUEUED",
                    "current_pipeline_run_id": pipeline_run_id,
                    "last_pipeline_run_id": cur_pid,
                    "last_dispatched_at": dispatched_at,
                    "last_execution_id": execution_id,
                    "attempt_count": int(state.get("attempt_count", 0) or 0),
                    "last_error": "",
                    "last_mode": run_type_label,
                    "last_votacao_list_record_uid": list_uid,
                    "last_votacao_list_record_hash": list_hash,
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
        if not all_ids and total_seen > 0:
            log_structured(
                logger,
                "warning",
                "Votacoes dispatch: /votacoes listing found no ids but IngestionState "
                "still has rows tied to this pipeline_run_id; counters may need cleanup.",
                domain=domain.name,
                pipeline_run_id=pipeline_run_id,
                total_state_rows_for_run=total_seen,
                per_status=pc,
            )
        total_detected = max(len(all_ids), total_seen)

        listing_done = recon_listing_complete if mode == "reconciliation" else listing_complete
        n_fanout = len(all_ids)
        fanout_decisions = (
            enqueued_now + skipped_already_queued + skipped_unchanged_hash
        )
        hit_cap = enqueued_now >= max_messages_per_tick
        if n_fanout == 0 and listing_done:
            enqueue_phase_complete = True
        else:
            enqueue_phase_complete = (
                listing_done
                and not hit_cap
                and fanout_decisions >= n_fanout
                and enqueued_now == 0
                and skipped_already_queued == 0
            )

        all_skipped_unchanged = (
            enqueue_phase_complete
            and n_fanout > 0
            and skipped_unchanged_hash >= n_fanout
        )

        if (
            (
                enqueue_phase_complete
                and total_detected == 0
                and pc["failed"] == 0
                and pc["poison"] == 0
                and pc["running"] == 0
                and pc["queued"] == 0
                and pc["pending"] == 0
            )
            or all_skipped_unchanged
        ):
            run_status_final = "COMPLETED"
            finished_at_utc = now.isoformat()
        elif (
            enqueue_phase_complete
            and total_detected > 0
            and pc["success"] >= total_detected
            and pc["failed"] == 0
            and pc["poison"] == 0
            and pc["running"] == 0
            and pc["queued"] == 0
            and pc["pending"] == 0
        ):
            run_status_final = "COMPLETED"
            finished_at_utc = now.isoformat()
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
                "total_votacoes_detected": total_detected,
                "total_tasks_expected": total_detected,
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
                "last_seen_votacao_id": last_seen_votacao_id or None,
                "last_seen_dataHoraRegistro": last_seen_dthr or None,
                "recon_listing_complete": recon_listing_complete if mode == "reconciliation" else True,
            }
        )

        meta_path = votacoes_run_metadata_path(pipeline_run_id)
        success_path = votacoes_run_success_path(pipeline_run_id)
        agg_meta = build_votacoes_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            status=run_status_final,
            started_at_utc=started_at,
            finished_at_utc=finished_at_utc,
            failed_at_utc=None,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            total_votacoes_detected=total_detected,
            total_tasks_expected=total_detected,
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
            manifest_extras={
                "target_year": target_year,
                "date_start": date_start,
                "date_end": date_end,
                "last_seen_votacao_id": last_seen_votacao_id or None,
                "last_seen_dataHoraRegistro": last_seen_dthr or None,
                "microbatch_date_window_days": int(
                    os.getenv("VOTACOES_MICROBATCH_DATE_WINDOW_DAYS", "2")
                )
                if mode == "microbatch"
                else None,
                "last_processed_votacao_id_cursor": lp_int if mode == "microbatch" else None,
                "skipped_unchanged_hash": skipped_unchanged_hash,
                "reconciliation_day": recon_day if mode == "reconciliation" else None,
                "metadata_path": meta_path,
                "success_marker_path": success_path,
            },
        )
        persist_votacoes_run_metadata(
            raw_writer,
            pipeline_run_id,
            agg_meta,
            write_success_marker_now=(run_status_final == "COMPLETED"),
        )

        log_structured(
            logger,
            "info",
            "Votacoes API dispatch tick finished.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            run_type=run_type_label,
            mode=mode,
            date_start=date_start,
            date_end=date_end,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            list_pages_written=list_pages_written,
            list_records_collected=list_records_collected,
            total_detected=total_detected,
            enqueued_now=enqueued_now,
            stale_requeued=stale_requeued,
            skipped_already_queued=skipped_already_queued,
            skipped_unchanged_hash=skipped_unchanged_hash,
            total_tasks_success=pc["success"],
            total_tasks_failed=pc["failed"],
            total_tasks_running=pc["running"],
            total_tasks_queued=pc["queued"],
            run_status_final=run_status_final,
            enqueue_phase_complete=enqueue_phase_complete,
            last_seen_votacao_id=last_seen_votacao_id,
            last_seen_dataHoraRegistro=last_seen_dthr,
            metadata_path=meta_path,
            success_marker_path=success_path,
            listing_complete=listing_complete,
            recon_listing_complete=recon_listing_complete,
        )
        return {
            "skipped": False,
            "pipeline_run_id": pipeline_run_id,
            "run_status_final": run_status_final,
            "messages_enqueued": enqueued_now,
            "total_tasks_expected": total_detected,
            "enqueue_phase_complete": enqueue_phase_complete,
            "metadata_path": meta_path,
            "success_marker_path": success_path,
        }
    except Exception as exc:
        failed_at = now.isoformat()
        log_structured(
            logger,
            "error",
            "Votacoes API dispatcher failed.",
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
        raise
    finally:
        registry.release_dispatcher_lock(lock_token)
        _ = raw_account
