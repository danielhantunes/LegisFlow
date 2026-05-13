"""Daily discursos dispatcher tick: list ``/deputados``, single JSONL snapshot, hash-aware fanout."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, time, timedelta
from typing import Any

from .adls_writer import AdlsRawWriter
from .api_client import CamaraApiClient
from .discursos_raw_manifest import (
    DISCURSOS_BASE_PREFIX,
    build_discursos_dispatcher_run_metadata,
    persist_discursos_run_metadata,
)
from .domain_catalog import DISCURSOS_DOMAIN, discursos_daily_run_id
from .generic_partition_state import GenericPartitionStateStore
from .logger import get_logger, log_structured
from .queue_helpers import prepare_queue_client_for_dispatch, send_json_message_with_client
from .queue_messages import DomainWorkMessage
from .raw_audit import enrich_generic_page_payload
from .run_registry import GenericRunRegistry
from .votacoes_api_dispatcher_logic import list_item_uid_hash

logger = get_logger()


def _state_row_key(deputado_id: str) -> str:
    return f"deputado_discursos|{deputado_id}"


def _daily_api_date_window(*, now: datetime) -> tuple[str, str, datetime, datetime]:
    """Calendar window for ``/deputados/{id}/discursos`` date filters (inclusive days)."""
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    end_d = now_utc.date()
    span = max(
        1,
        min(366, int(os.getenv("DISCURSOS_DAILY_LOOKBACK_DAYS", "7"))),
    )
    start_d = end_d - timedelta(days=span - 1)
    date_start = start_d.isoformat()
    date_end = end_d.isoformat()
    window_start = datetime.combine(start_d, time.min, tzinfo=UTC)
    window_end = datetime.combine(
        end_d, time.max.replace(microsecond=999999), tzinfo=UTC
    )
    return date_start, date_end, window_start, window_end


def execute_discursos_daily_tick(*, now: datetime) -> None:
    domain = DISCURSOS_DOMAIN
    now = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    pipeline_run_id = discursos_daily_run_id(now.strftime("%Y-%m-%d"))
    date_start, date_end, window_start, window_end = _daily_api_date_window(now=now)

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("DISCURSOS_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    lock_ttl = int(
        os.getenv("DISCURSOS_LOCK_TTL_MINUTES", str(domain.lock_ttl_minutes))
    )
    max_messages_per_tick = max(
        1, int(os.getenv("DISCURSOS_MAX_MESSAGES_PER_TICK", "1000"))
    )
    max_list_pages = max(1, int(os.getenv("DISCURSOS_MAX_LIST_PAGES", "20")))

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
            "Discursos daily dispatch skipped: run already COMPLETED.",
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
            "Discursos daily dispatch skipped: dispatcher lock held.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    started_at = str(run.get("started_at") or now.isoformat())
    enqueued_now = 0
    skipped_already_queued = 0
    skipped_same_list_hash = 0
    list_lines: list[str] = []
    list_records_collected = 0
    logical_pages = 0
    detected_ids: set[str] = set()
    fingerprints_by_id: dict[str, str] = {}

    api = CamaraApiClient(base_url=domain.api_base_url)
    raw_writer = AdlsRawWriter(account_name=raw_account)
    discursos_endpoint = domain.endpoint("deputado_discursos")
    deputies_dir = (
        f"{DISCURSOS_BASE_PREFIX}/deputies_snapshot/"
        f"pipeline_run_id={pipeline_run_id}/execution_id={pipeline_run_id}"
    )
    jsonl_path = f"{deputies_dir}/deputies_list.jsonl"
    bkf = ("id",)

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
                    "total_deputados_detected": 0,
                    "enqueue_phase_complete": False,
                    "deputies_snapshot_pipeline_run_id": pipeline_run_id,
                    "hash_strategy": domain.hash_strategy,
                    "audit_fields_applied": json.dumps(list(domain.audit_fields)),
                    "api_date_start": date_start,
                    "api_date_end": date_end,
                }
            )

        running_meta = build_discursos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="daily",
            status="RUNNING",
            started_at_utc=started_at,
            finished_at_utc=None,
            failed_at_utc=None,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            total_deputados_detected=0,
            total_tasks_expected=0,
            total_tasks_queued=0,
            total_tasks_pending=0,
            total_tasks_success=0,
            total_tasks_failed=0,
            total_tasks_poison=0,
            total_tasks_running=0,
            enqueue_phase_complete=False,
            deputies_snapshot_path=deputies_dir,
            deputies_snapshot_pipeline_run_id=pipeline_run_id,
            api_base_url=domain.api_base_url,
            source_system=domain.source_system,
            hash_strategy=domain.hash_strategy,
            audit_fields_applied=domain.audit_fields,
            manifest_extras={
                "api_date_start": date_start,
                "api_date_end": date_end,
                "deputies_list_jsonl": jsonl_path,
            },
        )
        persist_discursos_run_metadata(
            raw_writer,
            pipeline_run_id,
            running_meta,
            write_success_marker_now=False,
        )

        page = 1
        while page <= max_list_pages:
            payload, _http = api.list_endpoint_page("/deputados", page=page, itens=100)
            dados = payload.get("dados") or []
            list_records_collected += len(dados)
            logical_pages += 1
            enriched = enrich_generic_page_payload(
                payload,
                pipeline_run_id=pipeline_run_id,
                execution_id=pipeline_run_id,
                domain=domain.name,
                entity="deputies_snapshot",
                endpoint="deputies_snapshot",
                api_path="/deputados",
                raw_path=f"{deputies_dir}/#jsonl_page_{page}",
                page=page,
                business_key_fields=bkf,
                source_system=domain.source_system,
                api_base_url=domain.api_base_url,
            )
            list_lines.append(json.dumps(enriched, ensure_ascii=True))
            for item in dados:
                if isinstance(item, dict) and item.get("id") is not None:
                    did = str(item.get("id"))
                    detected_ids.add(did)
                    _uid, hsh = list_item_uid_hash(
                        domain,
                        endpoint_name="deputies_snapshot",
                        business_key_fields=bkf,
                        item=item,
                    )
                    fingerprints_by_id[did] = hsh
            links = payload.get("links") or []
            has_next = any(
                isinstance(li, dict) and li.get("rel") == "next" for li in links
            )
            if not has_next:
                break
            page += 1

        list_artifact_written = False
        if list_lines:
            raw_writer.write_text(jsonl_path, "\n".join(list_lines) + "\n")
            list_artifact_written = True

        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )

        for did in sorted(detected_ids):
            list_hash = fingerprints_by_id.get(did) or ""
            if enqueued_now >= max_messages_per_tick:
                break
            row = _state_row_key(did)
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
                endpoint=discursos_endpoint.name,
                pipeline_run_id=pipeline_run_id,
                run_type="daily",
                payload={
                    "deputado_id": did,
                    "window_start_utc": window_start.isoformat(),
                    "window_end_utc": window_end.isoformat(),
                    "date_start": date_start,
                    "date_end": date_end,
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
                endpoint=discursos_endpoint.name,
                deputado_id=did,
            )
            patch: dict[str, Any] = {
                "endpoint": discursos_endpoint.name,
                "deputado_id": did,
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
        sub_count = 1
        total_expected = max(len(detected_ids) * sub_count, total_seen)
        fanout_target = len(detected_ids) * sub_count
        fanout_decisions = (
            enqueued_now + skipped_already_queued + skipped_same_list_hash
        )
        hit_cap = enqueued_now >= max_messages_per_tick

        if not detected_ids:
            enqueue_phase_complete = list_artifact_written
        else:
            enqueue_phase_complete = (
                list_artifact_written
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
                "total_deputados_detected": len(detected_ids),
                "total_tasks_expected": total_expected,
                "total_tasks_queued": pc["queued"],
                "total_tasks_success": pc["success"],
                "total_tasks_failed": pc["failed"],
                "total_tasks_pending": pc["pending"],
                "total_tasks_poison": pc["poison"],
                "total_tasks_running": pc["running"],
                "deputies_snapshot_path": deputies_dir,
                "deputies_snapshot_pipeline_run_id": pipeline_run_id,
                "list_pages_written": logical_pages,
                "list_records_collected": list_records_collected,
                "window_start_utc": window_start.isoformat(),
                "window_end_utc": window_end.isoformat(),
            }
        )

        raw_files = 1 if list_artifact_written else 0

        agg_meta = build_discursos_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="daily",
            status=run_status_final,
            started_at_utc=started_at,
            finished_at_utc=finished_at_utc,
            failed_at_utc=None,
            window_start_utc=window_start.isoformat(),
            window_end_utc=window_end.isoformat(),
            total_deputados_detected=len(detected_ids),
            total_tasks_expected=total_expected,
            total_tasks_queued=pc["queued"],
            total_tasks_pending=pc["pending"],
            total_tasks_success=pc["success"],
            total_tasks_failed=pc["failed"],
            total_tasks_poison=pc["poison"],
            total_tasks_running=pc["running"],
            enqueue_phase_complete=enqueue_phase_complete,
            deputies_snapshot_path=deputies_dir,
            deputies_snapshot_pipeline_run_id=pipeline_run_id,
            api_base_url=domain.api_base_url,
            source_system=domain.source_system,
            hash_strategy=domain.hash_strategy,
            audit_fields_applied=domain.audit_fields,
            total_raw_files_written=raw_files,
            total_records_collected=list_records_collected,
            manifest_extras={
                "api_date_start": date_start,
                "api_date_end": date_end,
                "deputies_list_jsonl": jsonl_path if list_lines else "",
                "records_skipped_same_hash": skipped_same_list_hash,
            },
        )
        persist_discursos_run_metadata(
            raw_writer,
            pipeline_run_id,
            agg_meta,
            write_success_marker_now=(run_status_final == "COMPLETED"),
        )

        log_structured(
            logger,
            "info",
            "discursos_daily completed",
            pipeline_run_id=pipeline_run_id,
            records_seen=list_records_collected,
            deputados_distinct=len(detected_ids),
            messages_enqueued=enqueued_now,
            messages_skipped_hash=skipped_same_list_hash,
            messages_skipped_queued=skipped_already_queued,
            list_jsonl_lines=len(list_lines),
            run_status_final=run_status_final,
        )
    except Exception as exc:
        failed_at = datetime.now(UTC).isoformat()
        log_structured(
            logger,
            "error",
            "Discursos daily dispatcher failed.",
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
            failed_meta = build_discursos_dispatcher_run_metadata(
                pipeline_run_id=pipeline_run_id,
                mode="daily",
                status="FAILED",
                started_at_utc=started_at,
                finished_at_utc=None,
                failed_at_utc=failed_at,
                window_start_utc=window_start.isoformat(),
                window_end_utc=window_end.isoformat(),
                total_deputados_detected=len(detected_ids),
                total_tasks_expected=len(detected_ids),
                total_tasks_queued=0,
                total_tasks_pending=0,
                total_tasks_success=0,
                total_tasks_failed=0,
                total_tasks_poison=0,
                total_tasks_running=0,
                enqueue_phase_complete=False,
                deputies_snapshot_path=deputies_dir,
                deputies_snapshot_pipeline_run_id=pipeline_run_id,
                error_type=type(exc).__name__,
                error_message=str(exc)[:1024],
                api_base_url=domain.api_base_url,
                source_system=domain.source_system,
                hash_strategy=domain.hash_strategy,
                audit_fields_applied=domain.audit_fields,
                total_raw_files_written=0,
                total_records_collected=list_records_collected,
            )
            persist_discursos_run_metadata(
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
