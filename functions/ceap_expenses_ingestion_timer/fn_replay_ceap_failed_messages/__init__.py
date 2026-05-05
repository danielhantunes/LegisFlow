"""
HTTP replay: re-enqueue CEAP API partitions from IngestionState (FAILED / POISON / manual).

Query params:
- statuses: comma-separated (default: FAILED,POISON)
- endpoint: filter (default: ceap)
- id_deputado, ano, mes: optional filters
- full: true|false — reset paging checkpoints (last_successful_page) before enqueue
- pipeline_run_id: optional override for replay batch (default: ceap_replay_YYYYMMDD)
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import azure.functions as func

from shared.ceap_partition_state import CeapPartitionStateStore
from shared.dispatch_months import max_dispatch_month
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.work_message import CeapApiWorkMessage

logger = get_logger()


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
        queue_name = os.environ["CEAP_API_QUEUE_NAME"]
        parts = CeapPartitionStateStore.from_connection_string(
            os.environ["AzureWebJobsStorage"], state_table
        )

        statuses_raw = req.params.get("statuses") or "FAILED,POISON"
        statuses = [s.strip() for s in statuses_raw.split(",") if s.strip()]
        endpoint_raw = (req.params.get("endpoint") or "ceap").strip().lower()
        endpoint_filter = None if endpoint_raw == "*" else endpoint_raw
        id_dep = req.params.get("id_deputado")
        ano_p = req.params.get("ano")
        mes_p = req.params.get("mes")
        full = (req.params.get("full") or "false").lower() == "true"
        pr_override = (req.params.get("pipeline_run_id") or "").strip()

        id_deputado = int(id_dep) if id_dep is not None and id_dep != "" else None
        ano = int(ano_p) if ano_p is not None and ano_p != "" else None
        mes = int(mes_p) if mes_p is not None and mes_p != "" else None

        now = datetime.now(UTC)
        enqueued = 0
        skipped_future = 0

        for ent in parts.iter_partitions_for_replay(
            statuses=statuses,
            endpoint=endpoint_filter,
            id_deputado=id_deputado,
            ano=ano,
            mes=mes,
        ):
            dep = int(ent["id_deputado"])
            y = int(ent["ano"])
            m = int(ent["mes"])
            max_m = max_dispatch_month(target_year=y, now=now)
            if m > max_m:
                skipped_future += 1
                continue

            ep = str(ent.get("endpoint", "ceap"))
            mode = str(ent.get("last_mode") or "daily")
            pipeline_run_id = pr_override or f"ceap_replay_{now:%Y%m%d}"
            dispatched_at = now.isoformat()

            patch: dict = {
                "id_deputado": dep,
                "ano": y,
                "mes": m,
                "endpoint": ep,
                "status": "QUEUED",
                "last_mode": mode,
                "current_pipeline_run_id": pipeline_run_id,
                "last_dispatched_at": dispatched_at,
                "last_error": "",
                "updated_at": dispatched_at,
            }
            if full:
                patch["last_successful_page"] = 0

            parts.upsert_partition(patch)

            msg = CeapApiWorkMessage(
                endpoint=ep,
                id_deputado=dep,
                ano=y,
                mes=m,
                mode=mode,
                pipeline_run_id=pipeline_run_id,
                dispatched_at=dispatched_at,
            )
            send_json_message(queue_name, msg.to_json())
            enqueued += 1

        body = {
            "enqueued": enqueued,
            "skipped_future_months": skipped_future,
            "statuses": statuses,
            "endpoint": endpoint_raw,
            "full": full,
            "pipeline_run_id": pr_override or None,
        }
        log_structured(logger, "info", "Replay enqueued partitions from IngestionState.", **body)
        return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")
    except Exception as exc:
        log_structured(logger, "error", "Replay failed.", error=str(exc)[:1024])
        return func.HttpResponse(json.dumps({"error": str(exc)}), status_code=500, mimetype="application/json")
