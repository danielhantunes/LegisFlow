"""Poison queue: mark IngestionState POISON and bump automated run failure counters."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import azure.functions as func

from shared.ceap_partition_state import CeapPartitionStateStore
from shared.ceap_run_registry import CeapRunRegistry, pipeline_run_updates_registry
from shared.logger import get_logger, log_structured
from shared.work_message import CeapApiWorkMessage

logger = get_logger()


def main(msg: func.QueueMessage) -> None:
    wm = CeapApiWorkMessage.from_queue_body(msg.get_body())
    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")

    parts = CeapPartitionStateStore.from_connection_string(conn, state_table)
    registry = CeapRunRegistry.from_connection_string(conn, control_table)

    dequeue = int(getattr(msg, "dequeue_count", None) or 1)
    finished = datetime.now(UTC).isoformat()
    err = "Message landed on poison queue after repeated failures (see Application Insights)."
    pipeline_run_id = (wm.pipeline_run_id or "").strip()

    prev = parts.get_partition(wm.id_deputado, wm.ano, wm.mes)
    repro = int(prev.get("reprocess_count", 0) or 0) + 1 if prev else 1

    parts.upsert_partition(
        {
            "id_deputado": wm.id_deputado,
            "ano": wm.ano,
            "mes": wm.mes,
            "endpoint": wm.endpoint,
            "status": "POISON",
            "last_mode": wm.mode,
            "current_pipeline_run_id": pipeline_run_id,
            "last_error": err,
            "last_finished_at": finished,
            "updated_at": finished,
            "reprocess_count": repro,
        }
    )

    if pipeline_run_updates_registry(pipeline_run_id):
        registry.merge_run_counters(pipeline_run_id, failed_delta=1)

    log_structured(
        logger,
        "error",
        "Poison queue handled CEAP API partition.",
        endpoint=wm.endpoint,
        id_deputado=wm.id_deputado,
        ano=wm.ano,
        mes=wm.mes,
        mode=wm.mode,
        pipeline_run_id=pipeline_run_id,
        dequeue_count=dequeue,
    )
