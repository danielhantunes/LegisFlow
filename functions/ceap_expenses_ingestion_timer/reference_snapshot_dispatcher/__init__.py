"""Timer: reference snapshot dispatcher.

For each declared endpoint in ``REFERENCE_DOMAIN`` (partidos, legislaturas,
deputados, frentes, orgaos), enqueue ONE work message under the daily
``reference_snapshot_YYYYMMDD`` pipeline_run_id. Subsequent ticks short-circuit
once enqueueing is complete (idempotent).

The actual page-by-page snapshot work happens in the queue worker
(``reference_snapshot_worker``), keeping each Function execution short.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import azure.functions as func

from shared.domain_catalog import REFERENCE_DOMAIN, reference_run_id_for_date
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.queue_helpers import (
    prepare_queue_client_for_dispatch,
    send_json_message_with_client,
)
from shared.queue_messages import DomainWorkMessage
from shared.run_registry import GenericRunRegistry

logger = get_logger()


def _state_row_key(endpoint_name: str, reference_date: str) -> str:
    return f"{endpoint_name}|{reference_date}"


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    domain = REFERENCE_DOMAIN
    now = datetime.now(UTC)
    reference_tz_name = os.getenv("REFERENCE_TIMEZONE", "America/Sao_Paulo")
    try:
        reference_date = datetime.now(ZoneInfo(reference_tz_name)).date().isoformat()
    except Exception:
        reference_date = now.date().isoformat()

    pipeline_run_id = reference_run_id_for_date(reference_date)
    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("REFERENCE_SNAPSHOT_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    lock_ttl = int(
        os.getenv("REFERENCE_LOCK_TTL_MINUTES", str(domain.lock_ttl_minutes))
    )

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
    enq_done = bool(run.get("enqueue_phase_complete"))

    if run_status == "COMPLETED":
        log_structured(
            logger,
            "info",
            "Reference dispatch skipped: run already COMPLETED.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            reference_date=reference_date,
            skipped_already_completed=True,
        )
        return

    if enq_done:
        log_structured(
            logger,
            "info",
            "Reference dispatch skipped: enqueue phase already complete.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            reference_date=reference_date,
            total_tasks_queued=int(run.get("total_tasks_queued", 0) or 0),
        )
        return

    acquired, lock_token = registry.try_acquire_dispatcher_lock(
        mode="snapshot",
        pipeline_run_id=pipeline_run_id,
        ttl_minutes=lock_ttl,
    )
    if not acquired:
        log_structured(
            logger,
            "info",
            "Reference dispatch skipped: dispatcher lock held.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        return

    started_at = str(run.get("started_at") or now.isoformat())
    enqueued_now = 0
    skipped_already_queued = 0
    skipped_already_success = 0
    endpoints_total = len(domain.endpoints)

    try:
        if not run:
            registry.upsert_run(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "run_type": "snapshot",
                    "status": "STARTED",
                    "domain": domain.name,
                    "reference_date": reference_date,
                    "reference_timezone": reference_tz_name,
                    "started_at": started_at,
                    "total_tasks_expected": endpoints_total,
                    "total_tasks_queued": 0,
                    "total_tasks_success": 0,
                    "total_tasks_failed": 0,
                    "total_tasks_pending": 0,
                    "total_tasks_poison": 0,
                    "total_tasks_running": 0,
                    "enqueue_phase_complete": False,
                    "endpoints": json.dumps([ep.name for ep in domain.endpoints]),
                    "hash_strategy": domain.hash_strategy,
                    "audit_fields_applied": json.dumps(list(domain.audit_fields)),
                }
            )

        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            reference_date=reference_date,
        )

        for ep in domain.endpoints:
            row_key = _state_row_key(ep.name, reference_date)
            state = parts.get_partition(row_key) or {}
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
                endpoint=ep.name,
                pipeline_run_id=pipeline_run_id,
                run_type="snapshot",
                payload={
                    "reference_date": reference_date,
                    "reference_timezone": reference_tz_name,
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
                endpoint=ep.name,
                reference_date=reference_date,
            )

            prev_pid = cur_pid
            patch: dict[str, Any] = {
                "endpoint": ep.name,
                "reference_date": reference_date,
                "status": "QUEUED",
                "current_pipeline_run_id": pipeline_run_id,
                "last_pipeline_run_id": prev_pid,
                "last_dispatched_at": dispatched_at,
                "last_execution_id": execution_id,
                "attempt_count": int(state.get("attempt_count", 0) or 0),
                "last_error": "",
            }
            parts.upsert_partition(row_key, patch)
            enqueued_now += 1

        # Re-read state to derive accurate counters from the source of truth.
        pc = parts.count_statuses_by_run(pipeline_run_id)
        all_seen = (
            pc["queued"]
            + pc["running"]
            + pc["success"]
            + pc["failed"]
            + pc["poison"]
            + pc["pending"]
            + pc["stale"]
            + pc["other"]
        )
        enqueue_phase_complete = all_seen >= endpoints_total
        run_status_final = "COMPLETED" if (
            enqueue_phase_complete
            and pc["success"] == endpoints_total
            and pc["failed"] == 0
            and pc["poison"] == 0
            and pc["running"] == 0
            and pc["queued"] == 0
            and pc["pending"] == 0
        ) else ("RUNNING" if pc["running"] + pc["queued"] > 0 else "QUEUING")

        registry.upsert_run(
            {
                "pipeline_run_id": pipeline_run_id,
                "status": run_status_final,
                "enqueue_phase_complete": enqueue_phase_complete,
                "total_tasks_expected": endpoints_total,
                "total_tasks_queued": pc["queued"],
                "total_tasks_success": pc["success"],
                "total_tasks_failed": pc["failed"],
                "total_tasks_pending": pc["pending"],
                "total_tasks_poison": pc["poison"],
                "total_tasks_running": pc["running"],
            }
        )

        log_structured(
            logger,
            "info",
            "Reference dispatch tick finished.",
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
            reference_date=reference_date,
            endpoints_total=endpoints_total,
            enqueued_now=enqueued_now,
            skipped_already_queued=skipped_already_queued,
            skipped_already_success=skipped_already_success,
            run_status_final=run_status_final,
            enqueue_phase_complete=enqueue_phase_complete,
            total_tasks_success=pc["success"],
            total_tasks_failed=pc["failed"],
        )
    except Exception as exc:
        log_structured(
            logger,
            "error",
            "Reference dispatcher failed.",
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
                    "failed_at": datetime.now(UTC).isoformat(),
                }
            )
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        registry.release_dispatcher_lock(lock_token)
        # ``raw_account`` is required by every worker; surface it now to fail fast
        # if mis-configured.
        _ = raw_account
