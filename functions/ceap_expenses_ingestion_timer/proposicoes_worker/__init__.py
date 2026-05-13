"""Queue trigger: proposições worker.

Consumes messages from ``proposicoes-api-work``. Handles
``proposicao_autores`` and ``proposicao_tramitacoes``; the endpoint name in
the message selects the API path. Idempotent skip when ``list_item_hash``
matches ``last_list_item_hash`` on a SUCCESS row (cross-run guard).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import azure.functions as func

from shared.adls_writer import AdlsRawWriter
from shared.api_client import CamaraApiClient
from shared.domain_catalog import PROPOSICOES_DOMAIN
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.proposicoes_run import run_proposicao_sub_snapshot
from shared.queue_messages import DomainWorkMessage
from shared.run_registry import GenericRunRegistry

logger = get_logger()


def _state_row_key(endpoint_name: str, proposicao_id: str) -> str:
    return f"{endpoint_name}|{proposicao_id}"


def main(msg: func.QueueMessage) -> None:
    domain = PROPOSICOES_DOMAIN
    wm = DomainWorkMessage.from_queue_body(msg.get_body())
    dequeue_count = int(getattr(msg, "dequeue_count", None) or 1)

    if wm.domain != domain.name:
        log_structured(
            logger,
            "warning",
            "Proposicoes worker received message for a different domain.",
            received_domain=wm.domain,
            expected_domain=domain.name,
            pipeline_run_id=wm.pipeline_run_id,
        )
        return

    try:
        endpoint = domain.endpoint(wm.endpoint)
    except KeyError:
        log_structured(
            logger,
            "error",
            "Proposicoes worker received unknown endpoint; dropping message.",
            endpoint=wm.endpoint,
            pipeline_run_id=wm.pipeline_run_id,
        )
        return

    if endpoint.name not in ("proposicao_autores", "proposicao_tramitacoes"):
        log_structured(
            logger,
            "warning",
            "Proposicoes worker only handles autores/tramitacoes sub-endpoints.",
            endpoint=endpoint.name,
            pipeline_run_id=wm.pipeline_run_id,
        )
        return

    payload = wm.payload or {}
    proposicao_id = str(payload.get("proposicao_id", "")).strip()
    if not proposicao_id:
        log_structured(
            logger,
            "error",
            "Proposicoes worker missing proposicao_id in payload.",
            pipeline_run_id=wm.pipeline_run_id,
        )
        return

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]

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

    state_row = _state_row_key(endpoint.name, proposicao_id)
    state_now = parts.get_partition(state_row) or {}
    list_hash = str((payload.get("list_item_hash") or "")).strip()
    if (
        str(state_now.get("status", "")).upper() == "SUCCESS"
        and list_hash
        and str(state_now.get("last_list_item_hash") or "") == list_hash
    ):
        log_structured(
            logger,
            "info",
            "Proposicoes worker skipped: SUCCESS with unchanged list_item_hash.",
            domain=domain.name,
            endpoint=endpoint.name,
            proposicao_id=proposicao_id,
            pipeline_run_id=wm.pipeline_run_id,
            dequeue_count=dequeue_count,
        )
        return

    same_run = (
        str(state_now.get("current_pipeline_run_id", "") or "")
        == wm.pipeline_run_id
    )
    if same_run and str(state_now.get("status", "")).upper() == "SUCCESS":
        log_structured(
            logger,
            "info",
            "Proposicoes worker skipped: sub-endpoint already SUCCESS for this run.",
            domain=domain.name,
            endpoint=endpoint.name,
            proposicao_id=proposicao_id,
            pipeline_run_id=wm.pipeline_run_id,
            dequeue_count=dequeue_count,
        )
        return

    started_at = datetime.now(UTC).isoformat()
    attempt = int(state_now.get("attempt_count", 0) or 0) + 1
    parts.upsert_partition(
        state_row,
        {
            "endpoint": endpoint.name,
            "proposicao_id": proposicao_id,
            "status": "RUNNING",
            "current_pipeline_run_id": wm.pipeline_run_id,
            "last_pipeline_run_id": str(
                state_now.get("current_pipeline_run_id", "") or ""
            ),
            "last_started_at": started_at,
            "attempt_count": attempt,
            "last_execution_id": wm.execution_id,
            "last_error": "",
        },
    )

    api = CamaraApiClient(base_url=domain.api_base_url)
    raw_writer = AdlsRawWriter(account_name=raw_account)

    if endpoint.name == "proposicao_autores":
        def _fetch(page: int) -> tuple[dict, int]:
            return api.list_proposicao_autores_page(
                proposicao_id, page=page, itens=endpoint.items_per_page
            )
    else:
        def _fetch(page: int) -> tuple[dict, int]:
            return api.list_proposicao_tramitacoes_page(
                proposicao_id, page=page, itens=endpoint.items_per_page
            )

    try:
        result = run_proposicao_sub_snapshot(
            domain=domain,
            endpoint=endpoint,
            pipeline_run_id=wm.pipeline_run_id,
            execution_id=wm.execution_id or started_at,
            proposicao_id=proposicao_id,
            started_at_utc=started_at,
            raw_writer=raw_writer,
            page_fetcher=_fetch,
        )
    except Exception as exc:
        finished = datetime.now(UTC).isoformat()
        parts.upsert_partition(
            state_row,
            {
                "endpoint": endpoint.name,
                "proposicao_id": proposicao_id,
                "status": "RUNNING",
                "current_pipeline_run_id": wm.pipeline_run_id,
                "last_error": str(exc)[:1024],
                "last_finished_at": finished,
            },
        )
        log_structured(
            logger,
            "error",
            "Proposicoes worker raised; queue may retry.",
            domain=domain.name,
            endpoint=endpoint.name,
            proposicao_id=proposicao_id,
            pipeline_run_id=wm.pipeline_run_id,
            dequeue_count=dequeue_count,
            error=str(exc)[:500],
            error_type=type(exc).__name__,
        )
        raise

    finished = datetime.now(UTC).isoformat()
    if result.final_status == "COMPLETED":
        parts.upsert_partition(
            state_row,
            {
                "endpoint": endpoint.name,
                "proposicao_id": proposicao_id,
                "status": "SUCCESS",
                "current_pipeline_run_id": wm.pipeline_run_id,
                "last_finished_at": finished,
                "last_success_at": finished,
                "record_count": result.record_count,
                "pages_written": result.pages_written,
                "raw_path": result.last_raw_path,
                "last_list_item_hash": list_hash,
                "last_error": "",
            },
        )
        registry.merge_run_counters(wm.pipeline_run_id, success_delta=1)
    else:
        parts.upsert_partition(
            state_row,
            {
                "endpoint": endpoint.name,
                "proposicao_id": proposicao_id,
                "status": "FAILED",
                "current_pipeline_run_id": wm.pipeline_run_id,
                "last_finished_at": finished,
                "record_count": result.record_count,
                "pages_written": result.pages_written,
                "raw_path": result.last_raw_path,
                "last_error": (result.error_message or "")[:1024],
                "error_type": result.error_type or "",
            },
        )
        registry.merge_run_counters(wm.pipeline_run_id, failed_delta=1)

    log_structured(
        logger,
        "info",
        "Proposicoes worker finished.",
        domain=domain.name,
        endpoint=endpoint.name,
        proposicao_id=proposicao_id,
        pipeline_run_id=wm.pipeline_run_id,
        execution_id=wm.execution_id,
        final_status=result.final_status,
        record_count=result.record_count,
        pages_written=result.pages_written,
        raw_path=result.last_raw_path,
        dequeue_count=dequeue_count,
    )
