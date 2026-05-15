"""Current-year backfill for proposições: list ``/proposicoes`` + selective fanout.

Reuses ``proposicoes-api-work``, :class:`shared.queue_messages.DomainWorkMessage`,
and the same per-sub-endpoint Table rows as daily/reconciliation dispatchers.

API note: ``list_proposicoes_page`` uses ``dataInicio`` / ``dataFim`` on the
Câmara API (última movimentação na tramitação), not ``dataApresentacao``; that
matches existing ingestion semantics in ``CamaraApiClient.list_proposicoes_page``.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from .adls_writer import AdlsRawWriter
from .api_client import CamaraApiClient
from .current_year_backfill_contract import CurrentYearBackfillRequest
from .domain_catalog import PROPOSICOES_DOMAIN
from .generic_partition_state import GenericPartitionStateStore
from .proposicoes_list_batch_paths import (
    build_list_operation_manifest,
    list_operation_manifest_path,
    persist_operation_manifest_json,
    proposicoes_list_partition_run_dir,
)
from .queue_helpers import prepare_queue_client_for_dispatch, send_json_message_with_client
from .queue_messages import DomainWorkMessage
from .run_registry import GenericRunRegistry
from .logger import get_logger, log_structured

logger = get_logger()

_SUB_ENDPOINTS = ("proposicao_autores", "proposicao_tramitacoes")


def _state_row_key(endpoint_name: str, proposicao_id: str) -> str:
    return f"{endpoint_name}|{proposicao_id}"


def execute_proposicoes_current_year_backfill(
    *,
    request: CurrentYearBackfillRequest,
    pipeline_run_id: str,
    now: datetime,
) -> dict[str, Any]:
    """Run one proposições backfill pass. Mutates queue/state/registry unless dry_run."""
    domain = PROPOSICOES_DOMAIN
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    date_start = request.start_date
    date_end = request.end_date
    window_start = datetime.fromisoformat(f"{date_start}T00:00:00+00:00")
    window_end = datetime.fromisoformat(f"{date_end}T23:59:59.999999+00:00")

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("PROPOSICOES_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    max_list_pages = max(
        1,
        int(
            os.getenv(
                "PROPOSICOES_BACKFILL_MAX_LIST_PAGES",
                os.getenv("PROPOSICOES_MANUAL_RECONCILIATION_MAX_LIST_PAGES", "5000"),
            )
        ),
    )

    list_endpoint = domain.endpoint("proposicoes")
    bkf = list_endpoint.business_key_fields or ("id",)

    parts = GenericPartitionStateStore.from_connection_string(
        conn, state_table, partition_key=domain.state_partition_key
    )
    registry = GenericRunRegistry.from_connection_string(
        conn,
        control_table,
        runs_partition_key=domain.runs_partition_key,
        locks_partition_key=domain.locks_partition_key,
        lock_row_key=domain.lock_row_key,
    )
    raw_writer = AdlsRawWriter(account_name=raw_account)
    run_dir = proposicoes_list_partition_run_dir(run_anchor=now_utc, pipeline_run_id=pipeline_run_id)
    manifest_path = list_operation_manifest_path(run_dir)

    records_seen = 0
    messages_enqueued = 0
    records_skipped_same_hash = 0
    records_skipped_same_run = 0
    would_enqueue = 0
    warnings: list[str] = []
    limit_reached = False
    stop_scan = False

    api = CamaraApiClient(base_url=domain.api_base_url)
    queue_client = None
    if not request.dry_run:
        queue_client = prepare_queue_client_for_dispatch(
            queue_name,
            logger=logger,
            domain=domain.name,
            pipeline_run_id=pipeline_run_id,
        )
        registry.upsert_run(
            {
                "pipeline_run_id": pipeline_run_id,
                "run_type": "current_year_backfill",
                "status": "STARTED",
                "domain": domain.name,
                "window_start_utc": window_start.isoformat(),
                "window_end_utc": window_end.isoformat(),
                "started_at": now_utc.isoformat(),
                "total_tasks_expected": 0,
                "total_tasks_queued": 0,
                "total_tasks_success": 0,
                "total_tasks_failed": 0,
                "total_tasks_pending": 0,
                "total_tasks_poison": 0,
                "total_tasks_running": 0,
                "enqueue_phase_complete": False,
                "backfill_force": json.dumps(request.force),
            }
        )

    started_at = now_utc.isoformat()
    page = 1

    try:
        while page <= max_list_pages and not stop_scan:
            payload, _http = api.list_proposicoes_page(
                page=page,
                itens=list_endpoint.items_per_page,
                date_start=date_start,
                date_end=date_end,
            )
            dados = payload.get("dados") or []
            for item in dados:
                if stop_scan:
                    break
                if not isinstance(item, dict) or item.get("id") is None:
                    continue
                pid = str(item.get("id"))
                records_seen += 1
                _uid, list_hash = list_item_uid_hash(
                    domain,
                    endpoint_name="proposicoes",
                    business_key_fields=bkf,
                    item=item,
                )
                for sub_ep_name in _SUB_ENDPOINTS:
                    budget_used = would_enqueue if request.dry_run else messages_enqueued
                    if budget_used >= request.max_tasks:
                        limit_reached = True
                        stop_scan = True
                        break

                    row_key = _state_row_key(sub_ep_name, pid)
                    state = parts.get_partition(row_key) or {}
                    cur_pid = str(state.get("current_pipeline_run_id", "") or "")
                    cur_status = str(state.get("status", "")).upper()
                    stored_hash = str(state.get("last_list_item_hash") or "")

                    if cur_pid == pipeline_run_id and cur_status in ("QUEUED", "RUNNING"):
                        records_skipped_same_run += 1
                        continue

                    hash_unchanged = (
                        cur_status == "SUCCESS"
                        and list_hash
                        and stored_hash == list_hash
                    )
                    if hash_unchanged and not request.force:
                        records_skipped_same_hash += 1
                        continue

                    if request.dry_run:
                        would_enqueue += 1
                        continue

                    assert queue_client is not None
                    execution_id = str(uuid.uuid4())
                    dispatched_at = datetime.now(UTC).isoformat()
                    pl: dict[str, Any] = {
                        "proposicao_id": pid,
                        "window_start_utc": window_start.isoformat(),
                        "window_end_utc": window_end.isoformat(),
                        "list_item_hash": list_hash,
                    }
                    if request.force:
                        pl["force_reprocess"] = True

                    wm = DomainWorkMessage(
                        domain=domain.name,
                        endpoint=sub_ep_name,
                        pipeline_run_id=pipeline_run_id,
                        run_type="current_year_backfill",
                        payload=pl,
                        execution_id=execution_id,
                        dispatched_at=dispatched_at,
                    )
                    send_json_message_with_client(
                        queue_client,
                        wm.to_json(),
                        logger=logger,
                        domain=domain.name,
                        pipeline_run_id=pipeline_run_id,
                        endpoint=sub_ep_name,
                        proposicao_id=pid,
                    )
                    parts.upsert_partition(
                        row_key,
                        {
                            "endpoint": sub_ep_name,
                            "proposicao_id": pid,
                            "status": "QUEUED",
                            "current_pipeline_run_id": pipeline_run_id,
                            "last_pipeline_run_id": cur_pid,
                            "last_dispatched_at": dispatched_at,
                            "last_execution_id": execution_id,
                            "attempt_count": int(state.get("attempt_count", 0) or 0),
                            "last_error": "",
                        },
                    )
                    messages_enqueued += 1

            if stop_scan:
                warnings.append("max_tasks_reached_scan_stopped")
                break

            links = payload.get("links") or []
            has_next = any(
                isinstance(li, dict) and li.get("rel") == "next" for li in links
            )
            if not has_next:
                break
            page += 1

        if page >= max_list_pages and not stop_scan:
            warnings.append("max_list_pages_reached")

        finished_at = datetime.now(UTC).isoformat()
        if not request.dry_run:
            if messages_enqueued == 0:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "status": "COMPLETED",
                        "total_tasks_queued": 0,
                        "total_tasks_expected": 0,
                        "total_tasks_success": 0,
                        "total_tasks_failed": 0,
                        "enqueue_phase_complete": True,
                        "finished_at": finished_at,
                    }
                )
            else:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "status": "RUNNING",
                        "total_tasks_queued": messages_enqueued,
                        "total_tasks_expected": messages_enqueued,
                        "enqueue_phase_complete": True,
                        "finished_at": finished_at,
                    }
                )

        manifest_status = "dry_run_completed" if request.dry_run else (
            "LIMIT_REACHED" if limit_reached else "success"
        )
        op_manifest = build_list_operation_manifest(
            source="camara_api",
            endpoint="/proposicoes",
            pipeline_name="current_year_backfill",
            run_id=pipeline_run_id,
            window_start=date_start,
            window_end=date_end,
            records_seen=records_seen,
            records_written=messages_enqueued if not request.dry_run else would_enqueue,
            records_skipped_same_hash=records_skipped_same_hash,
            records_failed=0,
            status="success" if manifest_status in ("success", "dry_run_completed") else "partial",
            raw_path=run_dir,
            started_at=started_at,
            finished_at=finished_at,
            extras={
                "domains_requested": ["proposicoes"],
                "dry_run": request.dry_run,
                "force": request.force,
                "max_tasks": request.max_tasks,
                "limit_reached": limit_reached,
                "messages_enqueued": messages_enqueued,
                "would_enqueue": would_enqueue,
                "warnings": warnings,
            },
        )
        if not request.dry_run:
            persist_operation_manifest_json(raw_writer, manifest_path, op_manifest)

        domain_status = "SUCCESS"
        if limit_reached:
            domain_status = "LIMIT_REACHED"
        if request.dry_run:
            domain_status = "DRY_RUN"

        return {
            "records_seen": records_seen,
            "messages_enqueued": messages_enqueued,
            "records_skipped_same_hash": records_skipped_same_hash,
            "records_skipped_same_run": records_skipped_same_run,
            "would_enqueue": would_enqueue,
            "status": domain_status,
            "warnings": warnings,
            "manifest_path": manifest_path,
            "manifest": op_manifest if request.dry_run else None,
            "limit_reached": limit_reached,
        }
    except Exception as exc:
        finished_at = datetime.now(UTC).isoformat()
        err = f"{type(exc).__name__}: {str(exc)[:500]}"
        log_structured(
            logger,
            "warning",
            "current_year_backfill failed",
            run_id=pipeline_run_id,
            domain="proposicoes",
            error=err,
        )
        if not request.dry_run:
            try:
                registry.upsert_run(
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "status": "FAILED",
                        "last_error": err,
                        "failed_at": finished_at,
                    }
                )
            except Exception:  # noqa: BLE001
                pass
        if not request.dry_run:
            try:
                op_manifest = build_list_operation_manifest(
                    source="camara_api",
                    endpoint="/proposicoes",
                    pipeline_name="current_year_backfill",
                    run_id=pipeline_run_id,
                    window_start=date_start,
                    window_end=date_end,
                    records_seen=records_seen,
                    records_written=messages_enqueued,
                    records_skipped_same_hash=records_skipped_same_hash,
                    records_failed=1,
                    status="failed",
                    raw_path=run_dir,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_summary=err,
                    extras={"dry_run": request.dry_run, "force": request.force},
                )
                persist_operation_manifest_json(raw_writer, manifest_path, op_manifest)
            except Exception:  # noqa: BLE001
                pass
        raise
