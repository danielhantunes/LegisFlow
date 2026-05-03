"""
HTTP replay: re-enqueue CEAP API 2026 units from ingestion_control_api_2026.

Query params (GET or POST body JSON optional — MVP uses query only):
- statuses: comma-separated (default: failed,retrying)
- endpoint: filter (default: ceap)
- id_deputado, ano, mes: optional filters
- full: true|false — reset paging checkpoints before enqueue (full re-fetch)
"""

from __future__ import annotations

import json
import os

import azure.functions as func

from shared.control_api_store import IngestionControlApi2026Store
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.work_message import CeapApiWorkMessage

logger = get_logger()


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        table_name = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
        queue_name = os.environ["CEAP_API_QUEUE_NAME"]
        store = IngestionControlApi2026Store.from_connection_string(os.environ["AzureWebJobsStorage"], table_name)

        statuses_raw = req.params.get("statuses") or "failed,retrying"
        statuses = [s.strip().lower() for s in statuses_raw.split(",") if s.strip()]
        endpoint_raw = (req.params.get("endpoint") or "ceap").strip().lower()
        endpoint_filter = None if endpoint_raw == "*" else endpoint_raw
        id_dep = req.params.get("id_deputado")
        ano_p = req.params.get("ano")
        mes_p = req.params.get("mes")
        full = (req.params.get("full") or "false").lower() == "true"

        id_deputado = int(id_dep) if id_dep is not None and id_dep != "" else None
        ano = int(ano_p) if ano_p is not None and ano_p != "" else None
        mes = int(mes_p) if mes_p is not None and mes_p != "" else None

        enqueued = 0
        for ent in store.iter_units_for_replay(
            statuses=statuses,
            endpoint=endpoint_filter,
            id_deputado=id_deputado,
            ano=ano,
            mes=mes,
        ):
            dep = int(ent["id_deputado"])
            y = int(ent["ano"])
            m = int(ent["mes"])
            ep = str(ent.get("endpoint", "ceap"))

            patch: dict = {
                "ano": y,
                "mes": m,
                "id_deputado": dep,
                "endpoint": ep,
                "source_system": "camara_dados_abertos",
                "status": "pending",
                "error_message": "",
                "http_status_code": 0,
            }
            if full:
                patch["last_successful_page"] = 0
                patch["current_page"] = 1
            store.upsert_unit(patch)

            msg = CeapApiWorkMessage(endpoint=ep, id_deputado=dep, ano=y, mes=m)
            send_json_message(queue_name, msg.to_json())
            enqueued += 1

        body = {"enqueued": enqueued, "statuses": statuses, "endpoint": endpoint_raw, "full": full}
        log_structured(logger, "info", "Replay enqueued units.", **{k: body[k] for k in body})
        return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")
    except Exception as exc:
        log_structured(logger, "error", "Replay failed.", error=str(exc)[:1024])
        return func.HttpResponse(json.dumps({"error": str(exc)}), status_code=500, mimetype="application/json")
