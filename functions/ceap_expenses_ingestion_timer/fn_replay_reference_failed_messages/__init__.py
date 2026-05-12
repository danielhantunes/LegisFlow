"""HTTP replay: re-enqueue reference snapshot partitions from IngestionState.

Query params:

* ``statuses``: comma-separated (default: ``FAILED,POISON``)
* ``endpoint``: optional filter (e.g. ``partidos``)
* ``pipeline_run_id``: optional filter (will scope replay to that run)
* ``new_pipeline_run_id``: optional override; defaults to existing run id when
  filter is provided, otherwise ``reference_replay_YYYYMMDD``.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime

import azure.functions as func

from shared.domain_catalog import REFERENCE_DOMAIN
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.queue_helpers import send_json_message
from shared.queue_messages import DomainWorkMessage

logger = get_logger()


def main(req: func.HttpRequest) -> func.HttpResponse:
    domain = REFERENCE_DOMAIN
    try:
        state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
        queue_name = os.getenv("REFERENCE_SNAPSHOT_QUEUE_NAME", domain.queue_work)
        parts = GenericPartitionStateStore.from_connection_string(
            os.environ["AzureWebJobsStorage"],
            state_table,
            partition_key=domain.state_partition_key,
        )

        statuses_raw = req.params.get("statuses") or "FAILED,POISON"
        statuses = [s.strip() for s in statuses_raw.split(",") if s.strip()]
        endpoint_raw = (req.params.get("endpoint") or "").strip().lower() or None
        pid_filter = (req.params.get("pipeline_run_id") or "").strip() or None
        pid_override = (req.params.get("new_pipeline_run_id") or "").strip()

        now = datetime.now(UTC)
        default_run = pid_override or pid_filter or f"reference_replay_{now:%Y%m%d}"
        enqueued = 0
        for ent in parts.iter_for_replay(
            statuses=statuses,
            endpoint=endpoint_raw,
            pipeline_run_id=pid_filter,
        ):
            ep_name = str(ent.get("endpoint", "")).strip()
            if not ep_name:
                continue
            ref_date = str(ent.get("reference_date", "")).strip()
            if not ref_date:
                continue
            try:
                domain.endpoint(ep_name)
            except KeyError:
                continue
            execution_id = str(uuid.uuid4())
            wm = DomainWorkMessage(
                domain=domain.name,
                endpoint=ep_name,
                pipeline_run_id=default_run,
                run_type="snapshot",
                payload={
                    "reference_date": ref_date,
                    "reference_timezone": str(
                        ent.get("reference_timezone", "America/Sao_Paulo")
                    ),
                },
                execution_id=execution_id,
                dispatched_at=now.isoformat(),
            )
            send_json_message(queue_name, wm.to_json())
            parts.upsert_partition(
                f"{ep_name}|{ref_date}",
                {
                    "endpoint": ep_name,
                    "reference_date": ref_date,
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
            "endpoint": endpoint_raw,
            "pipeline_run_id_filter": pid_filter,
            "new_pipeline_run_id": default_run,
        }
        log_structured(
            logger,
            "info",
            "Reference replay enqueued partitions.",
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
            "Reference replay failed.",
            error=str(exc)[:1024],
        )
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=500,
            mimetype="application/json",
        )
