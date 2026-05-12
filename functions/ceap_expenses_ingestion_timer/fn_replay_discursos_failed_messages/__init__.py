"""HTTP replay: re-enqueue discursos partitions from IngestionState.

Query params:
* ``statuses``: comma-separated (default: ``FAILED,POISON``)
* ``deputado_id``: optional filter
* ``pipeline_run_id``: optional filter
* ``new_pipeline_run_id``: optional override; defaults to existing run id
  when filter is provided, otherwise ``discursos_replay_YYYYMMDD``.
* ``date_start`` / ``date_end``: optional window override (YYYY-MM-DD); when
  omitted the partition's last-known window is reused if present.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime

import azure.functions as func

from shared.domain_catalog import DISCURSOS_DOMAIN
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.queue_messages import DomainWorkMessage

logger = get_logger()


def _state_row_key(deputado_id: str) -> str:
    return f"deputado_discursos|{deputado_id}"


def main(req: func.HttpRequest) -> func.HttpResponse:
    domain = DISCURSOS_DOMAIN
    try:
        state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
        queue_name = os.getenv("DISCURSOS_QUEUE_NAME", domain.queue_work)
        parts = GenericPartitionStateStore.from_connection_string(
            os.environ["AzureWebJobsStorage"],
            state_table,
            partition_key=domain.state_partition_key,
        )

        statuses_raw = req.params.get("statuses") or "FAILED,POISON"
        statuses = [s.strip() for s in statuses_raw.split(",") if s.strip()]
        did_filter = (req.params.get("deputado_id") or "").strip() or None
        pid_filter = (req.params.get("pipeline_run_id") or "").strip() or None
        pid_override = (req.params.get("new_pipeline_run_id") or "").strip()
        ds_override = (req.params.get("date_start") or "").strip() or None
        de_override = (req.params.get("date_end") or "").strip() or None

        now = datetime.now(UTC)
        default_run = (
            pid_override or pid_filter or f"discursos_replay_{now:%Y%m%d}"
        )
        enqueued = 0
        for ent in parts.iter_for_replay(
            statuses=statuses,
            endpoint="deputado_discursos",
            pipeline_run_id=pid_filter,
        ):
            ep_name = str(ent.get("endpoint", "")).strip()
            if ep_name != "deputado_discursos":
                continue
            deputado_id = str(ent.get("deputado_id", "")).strip()
            if not deputado_id:
                continue
            if did_filter and deputado_id != did_filter:
                continue
            execution_id = str(uuid.uuid4())
            wm_payload: dict[str, object] = {"deputado_id": deputado_id}
            if ds_override:
                wm_payload["date_start"] = ds_override
            elif ent.get("last_window_date_start"):
                wm_payload["date_start"] = str(ent.get("last_window_date_start"))
            if de_override:
                wm_payload["date_end"] = de_override
            elif ent.get("last_window_date_end"):
                wm_payload["date_end"] = str(ent.get("last_window_date_end"))
            wm = DomainWorkMessage(
                domain=domain.name,
                endpoint="deputado_discursos",
                pipeline_run_id=default_run,
                run_type="manual_replay",
                payload=wm_payload,
                execution_id=execution_id,
                dispatched_at=now.isoformat(),
            )
            send_json_message(queue_name, wm.to_json())
            parts.upsert_partition(
                _state_row_key(deputado_id),
                {
                    "endpoint": "deputado_discursos",
                    "deputado_id": deputado_id,
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
            "deputado_id_filter": did_filter,
            "pipeline_run_id_filter": pid_filter,
            "new_pipeline_run_id": default_run,
            "date_start_override": ds_override,
            "date_end_override": de_override,
        }
        log_structured(
            logger,
            "info",
            "Discursos replay enqueued partitions.",
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
            "Discursos replay failed.",
            error=str(exc)[:1024],
        )
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=500,
            mimetype="application/json",
        )
