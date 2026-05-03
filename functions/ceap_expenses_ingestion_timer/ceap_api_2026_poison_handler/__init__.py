"""Poison queue: mark control row failed after Functions exhausts main-queue retries."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import azure.functions as func

from shared.control_api_store import IngestionControlApi2026Store
from shared.logger import get_logger, log_structured
from shared.work_message import CeapApiWorkMessage

logger = get_logger()


def main(msg: func.QueueMessage) -> None:
    wm = CeapApiWorkMessage.from_queue_body(msg.get_body())
    table_name = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    store = IngestionControlApi2026Store.from_connection_string(os.environ["AzureWebJobsStorage"], table_name)
    dequeue = int(getattr(msg, "dequeue_count", None) or 1)

    store.upsert_unit(
        {
            "ano": wm.ano,
            "mes": wm.mes,
            "id_deputado": wm.id_deputado,
            "endpoint": wm.endpoint,
            "source_system": "camara_dados_abertos",
            "status": "failed",
            "error_message": "Message landed on poison queue after repeated failures (see Application Insights).",
            "finished_at": datetime.now(UTC).isoformat(),
            "retry_count": max(0, dequeue - 1),
        }
    )

    log_structured(
        logger,
        "error",
        "Poison queue handled CEAP API unit.",
        endpoint=wm.endpoint,
        id_deputado=wm.id_deputado,
        ano=wm.ano,
        mes=wm.mes,
        dequeue_count=dequeue,
    )
