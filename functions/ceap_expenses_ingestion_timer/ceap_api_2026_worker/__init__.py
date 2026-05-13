"""Queue trigger: CEAP despesas (daily or reconciliation) — paging, Raw ADLS, IngestionState + run counters."""

from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime

import azure.functions as func
import requests

from shared.adls_writer import AdlsRawWriter
from shared.api_client import CamaraApiClient
from shared.ceap_partition_state import CeapPartitionStateStore
from shared.ceap_run_registry import CeapRunRegistry, pipeline_run_updates_registry
from shared.dispatch_months import max_dispatch_month
from shared.logger import get_logger, log_structured
from shared.raw_audit import enrich_ceap_page_payload, now_utc_iso
from shared.work_message import CeapApiWorkMessage

logger = get_logger()


def _has_next_link(payload: dict) -> bool:
    links = payload.get("links", [])
    return any(link.get("rel") == "next" for link in links)


def _safe_path_segment(value: str) -> str:
    return re.sub(r"[^\w.\-]+", "_", value)


def main(msg: func.QueueMessage) -> None:
    wm = CeapApiWorkMessage.from_queue_body(msg.get_body())
    dequeue_count = int(getattr(msg, "dequeue_count", None) or 1)
    execution_id = str(uuid.uuid4())

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    target_year = int(os.getenv("CEAP_TARGET_YEAR", os.getenv("CEAP_API_YEAR", "2026")))
    reprocess = os.getenv("CEAP_REPROCESS_QUEUE", "false").lower() == "true"

    registry = CeapRunRegistry.from_connection_string(conn, control_table)
    parts = CeapPartitionStateStore.from_connection_string(conn, state_table)

    now = datetime.now(UTC)
    pipeline_run_id = (wm.pipeline_run_id or "").strip()
    count_run = pipeline_run_updates_registry(pipeline_run_id)

    max_m = max_dispatch_month(target_year=target_year, now=now)
    if wm.ano != target_year:
        log_structured(
            logger,
            "debug",
            "Skipped: ano does not match CEAP_TARGET_YEAR.",
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            mode=wm.mode,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            target_year=target_year,
            final_status="skipped_wrong_year",
        )
        return

    if wm.mes > max_m:
        log_structured(
            logger,
            "debug",
            "Skipped: future month for target year.",
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            mode=wm.mode,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            max_dispatch_month=max_m,
            final_status="skipped_future_month",
        )
        return

    part = parts.get_partition(wm.id_deputado, wm.ano, wm.mes)
    if (
        part
        and str(part.get("current_pipeline_run_id", "")) == pipeline_run_id
        and str(part.get("status", "")).upper() == "SUCCESS"
        and not reprocess
    ):
        log_structured(
            logger,
            "debug",
            "Skipped: partition already SUCCESS for this pipeline_run_id.",
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            mode=wm.mode,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            final_status="skipped_idempotent",
        )
        return

    started_at = now.isoformat()
    page = 1
    if (
        part
        and pipeline_run_id
        and str(part.get("current_pipeline_run_id", "")) == pipeline_run_id
        and str(part.get("status", "")).upper() == "RUNNING"
    ):
        last_ok = int(part.get("last_successful_page", 0) or 0)
        if last_ok > 0:
            page = last_ok + 1

    prev_pid = str(part.get("current_pipeline_run_id", "")) if part else ""
    if part and prev_pid != pipeline_run_id and pipeline_run_id:
        page = 1

    attempt = int(part.get("attempt_count", 0) or 0) + 1 if part else 1
    parts.upsert_partition(
        {
            "id_deputado": wm.id_deputado,
            "ano": wm.ano,
            "mes": wm.mes,
            "endpoint": wm.endpoint,
            "status": "RUNNING",
            "last_mode": wm.mode,
            "current_pipeline_run_id": pipeline_run_id,
            "last_pipeline_run_id": prev_pid,
            "last_started_at": started_at,
            "attempt_count": attempt,
            "last_error": "",
            "updated_at": started_at,
        }
    )

    log_structured(
        logger,
        "debug",
        "Worker started.",
        id_deputado=wm.id_deputado,
        ano=wm.ano,
        mes=wm.mes,
        mode=wm.mode,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        start_page=page,
        dequeue_count=dequeue_count,
    )

    api = CamaraApiClient()
    raw_writer = AdlsRawWriter(account_name=os.environ["RAW_STORAGE_ACCOUNT_NAME"])

    pr_seg = _safe_path_segment(pipeline_run_id or "unknown_run")
    exec_seg = _safe_path_segment(execution_id)

    pages_processed = 0
    total_records = 0
    last_written = ""
    final_status = "running"

    try:
        while True:
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
                    id_deputado=wm.id_deputado,
                    ano=wm.ano,
                    mes=wm.mes,
                    mode=wm.mode,
                    pipeline_run_id=pipeline_run_id,
                    execution_id=execution_id,
                    current_page=page,
                    http_status_code=code,
                    error=str(exc)[:500],
                )
                if code in (400, 401, 403, 404):
                    finished = datetime.now(UTC).isoformat()
                    parts.upsert_partition(
                        {
                            "id_deputado": wm.id_deputado,
                            "ano": wm.ano,
                            "mes": wm.mes,
                            "endpoint": wm.endpoint,
                            "status": "FAILED",
                            "last_mode": wm.mode,
                            "current_pipeline_run_id": pipeline_run_id,
                            "last_finished_at": finished,
                            "last_error": str(exc)[:1024],
                            "http_status_code": int(code or 0),
                            "updated_at": finished,
                        }
                    )
                    if count_run and pipeline_run_id:
                        registry.merge_run_counters(pipeline_run_id, failed_delta=1)
                    final_status = "failed_terminal_http"
                    log_structured(
                        logger,
                        "error",
                        "Worker finished FAILED (terminal HTTP).",
                        id_deputado=wm.id_deputado,
                        ano=wm.ano,
                        mes=wm.mes,
                        mode=wm.mode,
                        pipeline_run_id=pipeline_run_id,
                        execution_id=execution_id,
                        pages_processed=pages_processed,
                        record_count=total_records,
                        raw_path=last_written,
                        final_status=final_status,
                    )
                    return
                parts.upsert_partition(
                    {
                        "id_deputado": wm.id_deputado,
                        "ano": wm.ano,
                        "mes": wm.mes,
                        "endpoint": wm.endpoint,
                        "status": "RUNNING",
                        "last_mode": wm.mode,
                        "current_pipeline_run_id": pipeline_run_id,
                        "last_error": str(exc)[:1024],
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                raise

            raw_path = (
                "raw/camara/ceap/api/despesas/"
                f"reference_year={wm.ano}/reference_month={wm.mes:02d}/"
                f"pipeline_run_id={pr_seg}/execution_id={exec_seg}/"
                f"deputado_id={wm.id_deputado}/page_{page}.json"
            )
            enriched_payload = enrich_ceap_page_payload(
                payload,
                pipeline_run_id=pipeline_run_id,
                execution_id=execution_id,
                id_deputado=wm.id_deputado,
                ano=wm.ano,
                mes=wm.mes,
                page=page,
                raw_path=raw_path,
                ingested_at_utc=now_utc_iso(),
            )
            written = raw_writer.write_json(raw_path, enriched_payload)
            last_written = written
            batch = payload.get("dados", []) or []
            rc = len(batch)
            total_records += rc
            pages_processed += 1

            tick = datetime.now(UTC).isoformat()
            parts.upsert_partition(
                {
                    "id_deputado": wm.id_deputado,
                    "ano": wm.ano,
                    "mes": wm.mes,
                    "endpoint": wm.endpoint,
                    "status": "RUNNING",
                    "last_mode": wm.mode,
                    "current_pipeline_run_id": pipeline_run_id,
                    "last_successful_page": page,
                    "record_count": total_records,
                    "raw_path": written,
                    "http_status_code": int(http_status),
                    "updated_at": tick,
                }
            )

            log_structured(
                logger,
                "debug",
                "Page persisted.",
                id_deputado=wm.id_deputado,
                ano=wm.ano,
                mes=wm.mes,
                mode=wm.mode,
                pipeline_run_id=pipeline_run_id,
                execution_id=execution_id,
                pages_processed=pages_processed,
                page=page,
                record_count=rc,
                http_status_code=http_status,
                raw_path=written,
            )

            if not _has_next_link(payload):
                break
            page += 1

        finished = datetime.now(UTC).isoformat()
        parts.upsert_partition(
            {
                "id_deputado": wm.id_deputado,
                "ano": wm.ano,
                "mes": wm.mes,
                "endpoint": wm.endpoint,
                "status": "SUCCESS",
                "last_mode": wm.mode,
                "current_pipeline_run_id": pipeline_run_id,
                "last_finished_at": finished,
                "last_success_at": finished,
                "record_count": total_records,
                "raw_path": last_written,
                "last_error": "",
                "updated_at": finished,
            }
        )
        if count_run and pipeline_run_id:
            registry.merge_run_counters(pipeline_run_id, success_delta=1)
        final_status = "success"
        log_structured(
            logger,
            "debug",
            "Worker finished SUCCESS.",
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            mode=wm.mode,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            pages_processed=pages_processed,
            record_count=total_records,
            raw_path=last_written,
            final_status=final_status,
        )
    except Exception as exc:
        tick = datetime.now(UTC).isoformat()
        parts.upsert_partition(
            {
                "id_deputado": wm.id_deputado,
                "ano": wm.ano,
                "mes": wm.mes,
                "endpoint": wm.endpoint,
                "status": "RUNNING",
                "last_mode": wm.mode,
                "current_pipeline_run_id": pipeline_run_id,
                "last_error": str(exc)[:1024],
                "updated_at": tick,
            }
        )
        log_structured(
            logger,
            "error",
            "Worker failed; queue may retry.",
            id_deputado=wm.id_deputado,
            ano=wm.ano,
            mes=wm.mes,
            mode=wm.mode,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            pages_processed=pages_processed,
            record_count=total_records,
            raw_path=last_written,
            error=str(exc)[:500],
            dequeue_count=dequeue_count,
            final_status="retrying",
        )
        raise
