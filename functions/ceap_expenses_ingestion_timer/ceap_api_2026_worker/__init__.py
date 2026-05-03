"""Queue trigger: ingest one CEAP despesas unit (deputado + ano + mês) with paging + checkpoints."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import azure.functions as func
import requests

from shared.adls_writer import AdlsRawWriter
from shared.api_client import CamaraApiClient
from shared.control_api_store import IngestionControlApi2026Store
from shared.logger import get_logger, log_structured
from shared.work_message import CeapApiWorkMessage

logger = get_logger()


def _has_next_link(payload: dict) -> bool:
    links = payload.get("links", [])
    return any(link.get("rel") == "next" for link in links)


def main(msg: func.QueueMessage) -> None:
    wm = CeapApiWorkMessage.from_queue_body(msg.get_body())
    dequeue_count = int(getattr(msg, "dequeue_count", None) or 1)
    execution_id = str(uuid.uuid4())
    table_name = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    reprocess = os.getenv("CEAP_REPROCESS_QUEUE", "false").lower() == "true"

    store = IngestionControlApi2026Store.from_connection_string(os.environ["AzureWebJobsStorage"], table_name)
    existing = store.get_unit(wm.ano, wm.mes, wm.id_deputado)

    if existing and str(existing.get("status", "")).lower() == "success" and not reprocess:
        log_structured(
            logger,
            "info",
            "Skipping unit already success.",
            execution_id=execution_id,
            endpoint=wm.endpoint,
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            dequeue_count=dequeue_count,
        )
        return

    if wm.ano != int(os.getenv("CEAP_API_YEAR", "2026")):
        store.upsert_unit(
            {
                "ano": wm.ano,
                "mes": wm.mes,
                "id_deputado": wm.id_deputado,
                "endpoint": wm.endpoint,
                "source_system": "camara_dados_abertos",
                "execution_id": execution_id,
                "status": "skipped",
                "error_message": "Year not in CEAP API slice for this worker.",
                "finished_at": datetime.now(UTC).isoformat(),
            }
        )
        log_structured(
            logger,
            "info",
            "Skipped non-target API year.",
            execution_id=execution_id,
            ano=wm.ano,
            expected_year=int(os.getenv("CEAP_API_YEAR", "2026")),
        )
        return

    started_at = datetime.now(UTC).isoformat()
    last_ok = int(existing.get("last_successful_page", 0)) if existing else 0
    page = max(1, last_ok + 1)
    queue_retry_count = max(int(existing.get("retry_count", 0)) if existing else 0, dequeue_count - 1)

    store.upsert_unit(
        {
            "ano": wm.ano,
            "mes": wm.mes,
            "id_deputado": wm.id_deputado,
            "endpoint": wm.endpoint,
            "source_system": "camara_dados_abertos",
            "execution_id": execution_id,
            "status": "running",
            "current_page": page,
            "last_successful_page": last_ok,
            "retry_count": queue_retry_count,
            "started_at": existing.get("started_at", started_at) if existing else started_at,
            "http_status_code": 0,
            "error_message": "",
        }
    )

    log_structured(
        logger,
        "info",
        "Worker started.",
        execution_id=execution_id,
        endpoint=wm.endpoint,
        id_deputado=wm.id_deputado,
        ano=wm.ano,
        mes=wm.mes,
        start_page=page,
        dequeue_count=dequeue_count,
    )

    api = CamaraApiClient()
    raw_writer = AdlsRawWriter(account_name=os.environ["RAW_STORAGE_ACCOUNT_NAME"])

    try:
        while True:
            store.upsert_unit(
                {
                    "ano": wm.ano,
                    "mes": wm.mes,
                    "id_deputado": wm.id_deputado,
                    "endpoint": wm.endpoint,
                    "source_system": "camara_dados_abertos",
                    "execution_id": execution_id,
                    "status": "running",
                    "current_page": page,
                }
            )

            try:
                payload, http_status = api.list_expenses_page(
                    deputy_id=wm.id_deputado, ano=wm.ano, mes=wm.mes, page=page
                )
            except requests.exceptions.HTTPError as exc:
                code = exc.response.status_code if exc.response is not None else None
                log_structured(
                    logger,
                    "error",
                    "HTTP error on CEAP despesas page.",
                    execution_id=execution_id,
                    id_deputado=wm.id_deputado,
                    ano=wm.ano,
                    mes=wm.mes,
                    current_page=page,
                    http_status_code=code,
                    error=str(exc)[:500],
                )
                if code in (400, 401, 403, 404):
                    store.upsert_unit(
                        {
                            "ano": wm.ano,
                            "mes": wm.mes,
                            "id_deputado": wm.id_deputado,
                            "endpoint": wm.endpoint,
                            "source_system": "camara_dados_abertos",
                            "execution_id": execution_id,
                            "status": "failed",
                            "http_status_code": int(code or 0),
                            "error_message": str(exc)[:1024],
                            "finished_at": datetime.now(UTC).isoformat(),
                        }
                    )
                    return
                store.upsert_unit(
                    {
                        "ano": wm.ano,
                        "mes": wm.mes,
                        "id_deputado": wm.id_deputado,
                        "endpoint": wm.endpoint,
                        "source_system": "camara_dados_abertos",
                        "execution_id": execution_id,
                        "status": "retrying",
                        "http_status_code": int(code or 0),
                        "error_message": str(exc)[:1024],
                    }
                )
                raise

            raw_path = (
                f"raw/camara/ceap/api/ano={wm.ano}/mes={wm.mes:02d}/deputado_id={wm.id_deputado}/"
                f"page={page:04d}/response.json"
            )
            written = raw_writer.write_json(raw_path, payload)
            record_count = len(payload.get("dados", []) or [])

            store.upsert_unit(
                {
                    "ano": wm.ano,
                    "mes": wm.mes,
                    "id_deputado": wm.id_deputado,
                    "endpoint": wm.endpoint,
                    "source_system": "camara_dados_abertos",
                    "execution_id": execution_id,
                    "status": "running",
                    "last_successful_page": page,
                    "current_page": page,
                    "http_status_code": int(http_status),
                    "raw_path": written,
                }
            )

            log_structured(
                logger,
                "info",
                "Page persisted.",
                execution_id=execution_id,
                endpoint=wm.endpoint,
                id_deputado=wm.id_deputado,
                ano=wm.ano,
                mes=wm.mes,
                current_page=page,
                record_count=record_count,
                http_status_code=http_status,
                raw_path=written,
            )

            if not _has_next_link(payload):
                break
            page += 1

        store.upsert_unit(
            {
                "ano": wm.ano,
                "mes": wm.mes,
                "id_deputado": wm.id_deputado,
                "endpoint": wm.endpoint,
                "source_system": "camara_dados_abertos",
                "execution_id": execution_id,
                "status": "success",
                "finished_at": datetime.now(UTC).isoformat(),
            }
        )
        log_structured(
            logger,
            "info",
            "Worker finished success.",
            execution_id=execution_id,
            endpoint=wm.endpoint,
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            last_successful_page=page,
            dequeue_count=dequeue_count,
        )
    except Exception as exc:
        store.upsert_unit(
            {
                "ano": wm.ano,
                "mes": wm.mes,
                "id_deputado": wm.id_deputado,
                "endpoint": wm.endpoint,
                "source_system": "camara_dados_abertos",
                "execution_id": execution_id,
                "status": "retrying",
                "error_message": str(exc)[:1024],
            }
        )
        log_structured(
            logger,
            "error",
            "Worker failed; will allow queue retry.",
            execution_id=execution_id,
            endpoint=wm.endpoint,
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            error=str(exc)[:500],
            dequeue_count=dequeue_count,
        )
        raise
