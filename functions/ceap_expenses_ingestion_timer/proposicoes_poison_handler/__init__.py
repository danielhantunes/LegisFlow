"""Poison queue: mark proposicao sub-endpoint partition POISON."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import azure.functions as func

from shared.domain_catalog import PROPOSICOES_DOMAIN
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.queue_messages import DomainWorkMessage
from shared.run_registry import GenericRunRegistry

logger = get_logger()


def _state_row_key(endpoint_name: str, proposicao_id: str) -> str:
    return f"{endpoint_name}|{proposicao_id}"


def main(msg: func.QueueMessage) -> None:
    domain = PROPOSICOES_DOMAIN
    try:
        wm = DomainWorkMessage.from_queue_body(msg.get_body())
    except Exception as exc:  # noqa: BLE001
        log_structured(
            logger,
            "error",
            "Proposicoes poison: could not parse message body.",
            error=str(exc)[:500],
        )
        return

    if wm.domain != domain.name:
        log_structured(
            logger,
            "warning",
            "Proposicoes poison handler received message for a different domain.",
            received_domain=wm.domain,
            expected_domain=domain.name,
        )
        return

    payload = wm.payload or {}
    proposicao_id = str(payload.get("proposicao_id", "")).strip()
    if not proposicao_id:
        log_structured(
            logger,
            "error",
            "Proposicoes poison: missing proposicao_id in payload.",
            pipeline_run_id=wm.pipeline_run_id,
            endpoint=wm.endpoint,
        )
        return

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")

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

    state_row = _state_row_key(wm.endpoint, proposicao_id)
    state = parts.get_partition(state_row) or {}
    repro = int(state.get("reprocess_count", 0) or 0) + 1
    finished = datetime.now(UTC).isoformat()
    parts.upsert_partition(
        state_row,
        {
            "endpoint": wm.endpoint,
            "proposicao_id": proposicao_id,
            "status": "POISON",
            "current_pipeline_run_id": wm.pipeline_run_id,
            "last_finished_at": finished,
            "last_error": "Message landed on proposicoes poison queue.",
            "reprocess_count": repro,
        },
    )
    if domain.is_pipeline_run_id_owned_here(wm.pipeline_run_id):
        registry.merge_run_counters(wm.pipeline_run_id, failed_delta=1)

    log_structured(
        logger,
        "error",
        "Proposicoes poison handled.",
        domain=domain.name,
        endpoint=wm.endpoint,
        proposicao_id=proposicao_id,
        pipeline_run_id=wm.pipeline_run_id,
        dequeue_count=int(getattr(msg, "dequeue_count", None) or 1),
    )
