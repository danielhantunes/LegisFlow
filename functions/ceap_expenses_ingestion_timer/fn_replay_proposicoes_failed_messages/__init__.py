"""HTTP replay: re-enqueue proposicoes partitions from IngestionState.

Query params:

* ``statuses``: comma-separated (default: ``FAILED,POISON``)
* ``endpoint``: optional filter (``proposicao_autores`` or
  ``proposicao_tramitacoes``); when omitted both are replayed
* ``proposicao_id``: optional filter (single proposicao id)
* ``pipeline_run_id``: optional filter (will scope replay to that run)
* ``new_pipeline_run_id``: optional override; defaults to existing run id
  when filter is provided, otherwise ``proposicoes_replay_YYYYMMDD``. When the
  effective target id is a microbatch or reconciliation run, ``run_type`` on
  queued messages matches it; synthetic replay ids use ``manual_replay``.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime

import azure.functions as func

from shared.domain_catalog import PROPOSICOES_DOMAIN
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.queue_messages import DomainWorkMessage
from shared.replay_run_type import infer_run_type_for_requeued_work

logger = get_logger()


def _state_row_key(endpoint_name: str, proposicao_id: str) -> str:
    return f"{endpoint_name}|{proposicao_id}"


def main(req: func.HttpRequest) -> func.HttpResponse:
    domain = PROPOSICOES_DOMAIN
    try:
        state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
        queue_name = os.getenv("PROPOSICOES_QUEUE_NAME", domain.queue_work)
        parts = GenericPartitionStateStore.from_connection_string(
            os.environ["AzureWebJobsStorage"],
            state_table,
            partition_key=domain.state_partition_key,
        )

        statuses_raw = req.params.get("statuses") or "FAILED,POISON"
        statuses = [s.strip() for s in statuses_raw.split(",") if s.strip()]
        endpoint_raw = (req.params.get("endpoint") or "").strip().lower() or None
        if endpoint_raw and endpoint_raw not in (
            "proposicao_autores",
            "proposicao_tramitacoes",
        ):
            return func.HttpResponse(
                json.dumps(
                    {
                        "error": (
                            "endpoint must be proposicao_autores or "
                            "proposicao_tramitacoes."
                        )
                    }
                ),
                status_code=400,
                mimetype="application/json",
            )
        pid_p_filter = (req.params.get("proposicao_id") or "").strip() or None
        pid_filter = (req.params.get("pipeline_run_id") or "").strip() or None
        pid_override = (req.params.get("new_pipeline_run_id") or "").strip()

        now = datetime.now(UTC)
        default_run = pid_override or pid_filter or f"proposicoes_replay_{now:%Y%m%d}"
        enqueued = 0
        for ent in parts.iter_for_replay(
            statuses=statuses,
            endpoint=endpoint_raw,
            pipeline_run_id=pid_filter,
        ):
            ep_name = str(ent.get("endpoint", "")).strip()
            if ep_name not in ("proposicao_autores", "proposicao_tramitacoes"):
                continue
            pid_p = str(ent.get("proposicao_id", "")).strip()
            if not pid_p:
                continue
            if pid_p_filter and pid_p != pid_p_filter:
                continue
            execution_id = str(uuid.uuid4())
            wm = DomainWorkMessage(
                domain=domain.name,
                endpoint=ep_name,
                pipeline_run_id=default_run,
                run_type=infer_run_type_for_requeued_work(default_run),
                payload={"proposicao_id": pid_p},
                execution_id=execution_id,
                dispatched_at=now.isoformat(),
            )
            send_json_message(queue_name, wm.to_json())
            parts.upsert_partition(
                _state_row_key(ep_name, pid_p),
                {
                    "endpoint": ep_name,
                    "proposicao_id": pid_p,
                    "status": "QUEUED",
                    "current_pipeline_run_id": default_run,
                    "last_pipeline_run_id": str(
                        ent.get("current_pipeline_run_id", "") or ""
                    ),
                    "last_dispatched_at": now.isoformat(),
                    "last_error": "",
                    "last_execution_id": execution_id,
                },
            )
            enqueued += 1

        body = {
            "domain": domain.name,
            "enqueued": enqueued,
            "statuses": statuses,
            "endpoint_filter": endpoint_raw,
            "proposicao_id_filter": pid_p_filter,
            "pipeline_run_id_filter": pid_filter,
            "new_pipeline_run_id": default_run,
        }
        log_structured(
            logger,
            "info",
            "Proposicoes replay enqueued partitions.",
            **body,
        )
        return func.HttpResponse(
            json.dumps(body, ensure_ascii=False),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as exc:
        log_structured(
            logger,
            "error",
            "Proposicoes replay failed.",
            error=str(exc)[:1024],
        )
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=500,
            mimetype="application/json",
        )
