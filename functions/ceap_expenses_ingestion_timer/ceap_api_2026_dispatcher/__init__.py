"""Timer: enqueue CEAP API 2026 work units (deputado + mês) with bounded batch size."""

from __future__ import annotations

import os

import azure.functions as func

from shared.control_api_store import IngestionControlApi2026Store
from shared.api_client import CamaraApiClient
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.work_message import CeapApiWorkMessage

logger = get_logger()


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    year = int(os.getenv("CEAP_API_YEAR", "2026"))
    max_msg = int(os.getenv("CEAP_DISPATCH_MAX_MESSAGES", "80"))
    queue_name = os.environ["CEAP_API_QUEUE_NAME"]
    table_name = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    reprocess = os.getenv("CEAP_REPROCESS_DISPATCH", "false").lower() == "true"

    store = IngestionControlApi2026Store.from_connection_string(os.environ["AzureWebJobsStorage"], table_name)
    cursor = store.get_dispatch_cursor()
    pagina = int(cursor.get("next_pagina", 1))
    idx = int(cursor.get("next_idx", 0))
    mes = int(cursor.get("next_mes", 1))

    api = CamaraApiClient()
    remaining = max_msg

    log_structured(
        logger,
        "info",
        "CEAP API 2026 dispatch tick started.",
        ceap_api_year=year,
        max_messages=max_msg,
        next_pagina=pagina,
        next_idx=idx,
        next_mes=mes,
    )

    while remaining > 0:
        payload, http_status = api.list_deputies_page(page=pagina)
        dados = payload.get("dados") or []
        if not dados:
            pagina = 1
            idx = 0
            mes = 1
            store.save_dispatch_cursor(next_pagina=pagina, next_idx=idx, next_mes=mes)
            log_structured(
                logger,
                "info",
                "Dispatcher wrapped deputy list cursor (empty page).",
                http_status=http_status,
            )
            break

        while idx < len(dados) and remaining > 0:
            dep_id = int(dados[idx]["id"])
            while mes <= 12 and remaining > 0:
                row = store.get_unit(year, mes, dep_id)
                st = str(row.get("status", "")).lower() if row else ""
                if st == "success" and not reprocess:
                    mes += 1
                    continue
                # Do not auto-requeue permanent failures; use HTTP replay or clear row.
                if st == "failed" and not reprocess:
                    mes += 1
                    continue
                msg = CeapApiWorkMessage(endpoint="ceap", id_deputado=dep_id, ano=year, mes=mes)
                send_json_message(queue_name, msg.to_json())
                remaining -= 1
                mes += 1
            if mes > 12:
                mes = 1
                idx += 1

        if idx >= len(dados):
            pagina += 1
            idx = 0
            mes = 1

        store.save_dispatch_cursor(next_pagina=pagina, next_idx=idx, next_mes=mes)

        if remaining == 0:
            break

    log_structured(
        logger,
        "info",
        "CEAP API 2026 dispatch tick finished.",
        messages_enqueued=max_msg - remaining,
        next_pagina=pagina,
        next_idx=idx,
        next_mes=mes,
    )
