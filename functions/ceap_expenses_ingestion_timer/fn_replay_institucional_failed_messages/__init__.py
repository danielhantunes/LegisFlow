"""HTTP replay: re-enqueue institucional partitions from IngestionState.

Query params:
* ``statuses``: comma-separated (default: ``FAILED,POISON``)
* ``endpoint``: optional filter (one of the worker sub-endpoints)
* ``parent_id``: optional filter
* ``pipeline_run_id``: optional filter
* ``new_pipeline_run_id``: optional override; defaults to the existing run id
  when filter is provided, otherwise ``institucional_replay_YYYYMMDD``.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime

import azure.functions as func

from shared.domain_catalog import INSTITUCIONAL_DOMAIN
from shared.generic_partition_state import GenericPartitionStateStore
from shared.institucional_raw_manifest import WORKER_ENDPOINTS
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.queue_messages import DomainWorkMessage

logger = get_logger()


def _state_row_key(endpoint_name: str, parent_id: str) -> str:
    return f"{endpoint_name}|{parent_id}"


def main(req: func.HttpRequest) -> func.HttpResponse:
    domain = INSTITUCIONAL_DOMAIN
    try:
        state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
        queue_name = os.getenv("INSTITUCIONAL_QUEUE_NAME", domain.queue_work)
        parts = GenericPartitionStateStore.from_connection_string(
            os.environ["AzureWebJobsStorage"],
            state_table,
            partition_key=domain.state_partition_key,
        )

        statuses_raw = req.params.get("statuses") or "FAILED,POISON"
        statuses = [s.strip() for s in statuses_raw.split(",") if s.strip()]
        endpoint_raw = (req.params.get("endpoint") or "").strip().lower() or None
        if endpoint_raw and endpoint_raw not in WORKER_ENDPOINTS:
            return func.HttpResponse(
                json.dumps(
                    {
                        "error": (
                            f"endpoint must be one of {list(WORKER_ENDPOINTS)}."
                        )
                    }
                ),
                status_code=400,
                mimetype="application/json",
            )
        pid_param = (req.params.get("parent_id") or "").strip() or None
        pid_filter = (req.params.get("pipeline_run_id") or "").strip() or None
        pid_override = (req.params.get("new_pipeline_run_id") or "").strip()

        now = datetime.now(UTC)
        default_run = (
            pid_override
            or pid_filter
            or f"institucional_replay_{now:%Y%m%d}"
        )
        enqueued = 0
        for ent in parts.iter_for_replay(
            statuses=statuses,
            endpoint=endpoint_raw,
            pipeline_run_id=pid_filter,
        ):
            ep_name = str(ent.get("endpoint", "")).strip()
            if ep_name not in WORKER_ENDPOINTS:
                continue
            parent_id = str(ent.get("parent_id", "")).strip()
            if not parent_id:
                continue
            if pid_param and parent_id != pid_param:
                continue
            execution_id = str(uuid.uuid4())
            wm = DomainWorkMessage(
                domain=domain.name,
                endpoint=ep_name,
                pipeline_run_id=default_run,
                run_type="manual_replay",
                payload={"parent_id": parent_id},
                execution_id=execution_id,
                dispatched_at=now.isoformat(),
            )
            send_json_message(queue_name, wm.to_json())
            parts.upsert_partition(
                _state_row_key(ep_name, parent_id),
                {
                    "endpoint": ep_name,
                    "parent_id": parent_id,
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
            "parent_id_filter": pid_param,
            "pipeline_run_id_filter": pid_filter,
            "new_pipeline_run_id": default_run,
        }
        log_structured(
            logger,
            "info",
            "Institucional replay enqueued partitions.",
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
            "Institucional replay failed.",
            error=str(exc)[:1024],
        )
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=500,
            mimetype="application/json",
        )
