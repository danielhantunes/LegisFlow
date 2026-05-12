"""Timer: institucional dispatcher (daily fanout).

Each tick:

1. Computes ``institucional_daily_YYYYMMDD`` for the current UTC date.
2. Acquires the institucional dispatcher lock.
3. Lists 4 parent endpoints (``/orgaos``, ``/partidos``, ``/frentes``,
   ``/legislaturas``) and persists the listing pages.
4. For each parent_id detected, enqueues messages for the relevant sub-endpoint(s):
   - orgao_id        -> orgao_membros
   - partido_id      -> partido_membros
   - frente_id       -> frente_membros
   - legislatura_id  -> legislatura_lideres + legislatura_mesa
5. Reconciles run counters from ``IngestionState`` and writes the aggregate
   ``metadata.json`` (+ ``_SUCCESS`` only when strictly completed).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any, Callable

import azure.functions as func

from shared.adls_writer import AdlsRawWriter
from shared.api_client import CamaraApiClient
from shared.domain_catalog import (
    INSTITUCIONAL_DOMAIN,
    institucional_daily_run_id,
)
from shared.generic_partition_state import GenericPartitionStateStore
from shared.institucional_raw_manifest import (
    PARENT_ENDPOINTS,
    WORKER_ENDPOINTS,
    build_institucional_dispatcher_run_metadata,
    parent_endpoint_kind,
    parent_listing_dir,
    persist_institucional_run_metadata,
)
from shared.logger import get_logger, log_structured
from shared.queue_helpers import (
    prepare_queue_client_for_dispatch,
    send_json_message_with_client,
)
from shared.queue_messages import DomainWorkMessage
from shared.raw_audit import enrich_generic_page_payload
from shared.run_registry import GenericRunRegistry

logger = get_logger()


# Mapping from parent endpoint name -> list of worker endpoints derived from it.
_PARENT_TO_WORKERS: dict[str, tuple[str, ...]] = {
    "orgaos_parent":       ("orgao_membros",),
    "partidos_parent":     ("partido_membros",),
    "frentes_parent":      ("frente_membros",),
    "legislaturas_parent": ("legislatura_lideres", "legislatura_mesa"),
}


def _state_row_key(endpoint_name: str, parent_id: str) -> str:
    return f"{endpoint_name}|{parent_id}"


def _parent_fetcher(
    api: CamaraApiClient, parent_endpoint_name: str
) -> Callable[[int, int], tuple[dict[str, Any], int]]:
    if parent_endpoint_name == "orgaos_parent":
        def f(page: int, itens: int) -> tuple[dict[str, Any], int]:
            return api.list_orgaos_page(page=page, itens=itens)
    elif parent_endpoint_name == "partidos_parent":
        def f(page: int, itens: int) -> tuple[dict[str, Any], int]:
            return api.list_partidos_page(page=page, itens=itens)
    elif parent_endpoint_name == "frentes_parent":
        def f(page: int, itens: int) -> tuple[dict[str, Any], int]:
            return api.list_frentes_page(page=page, itens=itens)
    elif parent_endpoint_name == "legislaturas_parent":
        def f(page: int, itens: int) -> tuple[dict[str, Any], int]:
            return api.list_legislaturas_page(page=page, itens=itens)
    else:
        raise KeyError(parent_endpoint_name)
    return f


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    domain = INSTITUCIONAL_DOMAIN
    now = datetime.now(UTC)
    pipeline_run_id = institucional_daily_run_id(now.date().isoformat())

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("INSTITUCIONAL_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    lock_ttl = int(
        os.getenv(
            "INSTITUCIONAL_LOCK_TTL_MINUTES", str(domain.lock_ttl_minutes)
        )
    )
    max_messages_per_tick = max(
        1, int(os.getenv("INSTITUCIONAL_MAX_MESSAGES_PER_TICK", "5000"))
    )
    max_list_pages = int(os.getenv("INSTITUCIONAL_MAX_LIST_PAGES", "200"))

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
            "Institucional dispatch skipped: run already COMPLETED.",
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
            "info",
            "Institucional dispatch skipped: dispatcher lock held.",
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
    parents_detected: dict[str, set[str]] = {p: set() for p in PARENT_ENDPOINTS}

    api = CamaraApiClient(base_url=domain.api_base_url)
    raw_writer = AdlsRawWriter(account_name=raw_account)
    sub_endpoint_specs = {name: domain.endpoint(name) for name in WORKER_ENDPOINTS}

    try:
        if not run:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "run_type": "daily",
                    "status": "STARTED",
                    "domain": domain.name,
                    "reference_date": now.date().isoformat(),
                    "started_at": started_at,
                    "total_tasks_expected": 0,
                    "total_tasks_queued": 0,
                    "total_tasks_success": 0,
                    "total_tasks_failed": 0,
                    "total_tasks_pending": 0,
                    "total_tasks_poison": 0,
                    "total_tasks_running": 0,
                    "total_parents_detected": 0,
                    "enqueue_phase_complete": False,
                    "sub_endpoints": json.dumps(list(WORKER_ENDPOINTS)),
                    "hash_strategy": domain.hash_strategy,
                    "audit_fields_applied": json.dumps(list(domain.audit_fields)),
                }
            )

        running_meta = build_institucional_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="daily",
            status="RUNNING",
            started_at_utc=started_at,
            finished_at_utc=None,
            failed_at_utc=None,
            parents_detected={p: 0 for p in PARENT_ENDPOINTS},
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
        persist_institucional_run_metadata(
            raw_writer,
            pipeline_run_id,
            running_meta,
            write_success_marker_now=False,
        )

        # 1) List each parent endpoint and persist Raw pages.
        for parent_endpoint_name in PARENT_ENDPOINTS:
            parent_spec = domain.endpoint(parent_endpoint_name)
            kind = parent_endpoint_kind(parent_endpoint_name)
            list_dir = (
                f"{parent_listing_dir(parent_endpoint_name, pipeline_run_id)}/"
                f"execution_id={pipeline_run_id}"
            )
            fetch = _parent_fetcher(api, parent_endpoint_name)
            page = 1
            while page <= max_list_pages:
                payload, _http = fetch(page, parent_spec.items_per_page)
                dados = payload.get("dados") or []
                list_records_collected += len(dados)
                raw_path = f"{list_dir}/page_{page}.json"
                enriched = enrich_generic_page_payload(
                    payload,
                    pipeline_run_id=pipeline_run_id,
                    execution_id=pipeline_run_id,
                    domain=domain.name,
                    entity=parent_spec.name,
                    endpoint=parent_spec.name,
                    api_path=parent_spec.path_template,
                    raw_path=raw_path,
                    page=page,
                    business_key_fields=parent_spec.business_key_fields or ("id",),
                    source_system=domain.source_system,
                    api_base_url=domain.api_base_url,
                    extra_audit={"_parent_kind": kind},
                )
                raw_writer.write_json(raw_path, enriched)
                list_pages_written += 1
                for item in dados:
                    if isinstance(item, dict):
                        pid = item.get("id")
                        if pid is not None:
                            parents_detected[parent_endpoint_name].add(str(pid))
                links = payload.get("links") or []
                has_next = any(
                    isinstance(li, dict) and li.get("rel") == "next"
                    for li in links
                )
                if not has_next:
                    break
                page += 1

        # 2) Fanout: enqueue worker messages for each (parent_id, sub_endpoint).
        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        # Iterate parents in stable order for deterministic enqueue.
        for parent_endpoint_name in PARENT_ENDPOINTS:
            workers = _PARENT_TO_WORKERS[parent_endpoint_name]
            for parent_id in sorted(parents_detected[parent_endpoint_name]):
                if enqueued_now >= max_messages_per_tick:
                    break
                for worker_name in workers:
                    if enqueued_now >= max_messages_per_tick:
                        break
                    sub_spec = sub_endpoint_specs[worker_name]
                    row = _state_row_key(sub_spec.name, parent_id)
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
                        endpoint=sub_spec.name,
                        pipeline_run_id=pipeline_run_id,
                        run_type="daily",
                        payload={"parent_id": parent_id},
                        execution_id=execution_id,
                        dispatched_at=dispatched_at,
                    )
                    send_json_message_with_client(
                        queue_client,
                        wm.to_json(),
                        logger=logger,
                        domain=domain.name,
                        pipeline_run_id=pipeline_run_id,
                        endpoint=sub_spec.name,
                        parent_id=parent_id,
                    )
                    patch: dict[str, Any] = {
                        "endpoint": sub_spec.name,
                        "parent_id": parent_id,
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

        # 3) Reconcile.
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
        # Each parent contributes len(_PARENT_TO_WORKERS[parent]) tasks.
        expected_tasks_total = sum(
            len(parents_detected[p]) * len(_PARENT_TO_WORKERS[p])
            for p in PARENT_ENDPOINTS
        )
        total_expected = max(expected_tasks_total, total_seen)
        enqueue_phase_complete = (
            list_pages_written > 0
            and enqueued_now == 0
            and skipped_already_queued == 0
            and total_seen >= expected_tasks_total
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

        parents_counts = {p: len(parents_detected[p]) for p in PARENT_ENDPOINTS}
        registry.upsert_run(
            {
                "pipeline_run_id": pipeline_run_id,
                "status": run_status_final,
                "enqueue_phase_complete": enqueue_phase_complete,
                "total_parents_detected": sum(parents_counts.values()),
                "parents_detected": json.dumps(parents_counts),
                "total_tasks_expected": total_expected,
                "total_tasks_queued": pc["queued"],
                "total_tasks_success": pc["success"],
                "total_tasks_failed": pc["failed"],
                "total_tasks_pending": pc["pending"],
                "total_tasks_poison": pc["poison"],
                "total_tasks_running": pc["running"],
                "list_pages_written": list_pages_written,
                "list_records_collected": list_records_collected,
            }
        )

        agg_meta = build_institucional_dispatcher_run_metadata(
            pipeline_run_id=pipeline_run_id,
            mode="daily",
            status=run_status_final,
            started_at_utc=started_at,
            finished_at_utc=finished_at_utc,
            failed_at_utc=None,
            parents_detected=parents_counts,
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
        persist_institucional_run_metadata(
            raw_writer,
            pipeline_run_id,
            agg_meta,
            write_success_marker_now=(run_status_final == "COMPLETED"),
        )

        log_structured(
            logger,
            "info",
            "Institucional dispatch tick finished.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            list_pages_written=list_pages_written,
            list_records_collected=list_records_collected,
            parents_detected=parents_counts,
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
            "Institucional dispatcher failed.",
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
            parents_counts = {
                p: len(parents_detected[p]) for p in PARENT_ENDPOINTS
            }
            failed_meta = build_institucional_dispatcher_run_metadata(
                pipeline_run_id=pipeline_run_id,
                mode="daily",
                status="FAILED",
                started_at_utc=started_at,
                finished_at_utc=None,
                failed_at_utc=failed_at,
                parents_detected=parents_counts,
                total_tasks_expected=0,
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
            persist_institucional_run_metadata(
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
