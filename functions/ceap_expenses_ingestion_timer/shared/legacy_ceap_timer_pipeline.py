"""
Legacy monolithic CEAP timer pipeline (all deputies / months in one execution).

Disabled by default (set CEAP_LEGACY_MONOLITH_ENABLED=true only for emergency use).
API client contract matches shared.api_client (returns (payload, http_status)).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import azure.functions as func

from shared.adls_writer import AdlsRawWriter
from shared.api_client import CamaraApiClient
from shared.logger import get_logger, log_structured
from shared.state_store import IngestionStateStore

LOCK_NAME = "ceap_daily_ingestion"
SUCCESS = "SUCCESS"
RUNNING = "RUNNING"
FAILED = "FAILED"
STALE = "STALE"

logger = get_logger()


def run_legacy_monolith(timer: func.TimerRequest) -> None:  # noqa: ARG001
    execution_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    pipeline_run_id = f"ceap_daily_{datetime.now(UTC).strftime('%Y%m%d')}"

    state_store = _build_state_store()
    if not state_store.acquire_lock(lock_name=LOCK_NAME, execution_id=execution_id):
        log_structured(logger, "info", "Ingestion lock is active; skipping execution.", execution_id=execution_id)
        return

    log_structured(logger, "info", "Starting legacy CEAP monolith ingestion.", pipeline_run_id=pipeline_run_id, execution_id=execution_id)
    try:
        _run_pipeline(pipeline_run_id=pipeline_run_id, execution_id=execution_id, state_store=state_store)
    finally:
        state_store.release_lock(LOCK_NAME)
        log_structured(logger, "info", "Released ingestion lock.", execution_id=execution_id)


def _run_pipeline(pipeline_run_id: str, execution_id: str, state_store: IngestionStateStore) -> None:
    api = CamaraApiClient()
    raw_writer = AdlsRawWriter(account_name=os.environ["RAW_STORAGE_ACCOUNT_NAME"])

    deputy_ids = _ingest_deputies(api=api, raw_writer=raw_writer, pipeline_run_id=pipeline_run_id, execution_id=execution_id)
    partitions = _build_monthly_partitions(deputy_ids)

    for deputy_id, ano, mes in partitions:
        partition_key = f"despesas|{deputy_id}|{ano}|{mes}"
        current = state_store.get_partition(partition_key)

        if current and current.get("status") == SUCCESS and os.getenv("REPROCESS_MODE", "false").lower() != "true":
            continue

        if current and current.get("status") == RUNNING:
            state_store.upsert_partition({"partition_key": partition_key, "status": STALE})

        if current and current.get("status") == FAILED and int(current.get("attempt_count", 0)) >= int(
            os.getenv("MAX_RETRY_ATTEMPTS", "3")
        ):
            continue

        _process_partition(
            state_store=state_store,
            api=api,
            raw_writer=raw_writer,
            deputy_id=deputy_id,
            ano=ano,
            mes=mes,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            partition_key=partition_key,
        )

    log_structured(
        logger,
        "info",
        "Legacy raw ingestion stage complete.",
        pipeline_run_id=pipeline_run_id,
    )


def _ingest_deputies(api: CamaraApiClient, raw_writer: AdlsRawWriter, pipeline_run_id: str, execution_id: str) -> list[int]:
    page = 1
    deputy_ids: list[int] = []
    ingestion_date = datetime.now(UTC).strftime("%Y-%m-%d")

    while True:
        payload, _http = api.list_deputies_page(page=page)
        path = (
            "raw/camara/deputados/api/"
            f"ingestion_date={ingestion_date}/pipeline_run_id={pipeline_run_id}/execution_id={execution_id}/page_{page:03d}.json"
        )
        raw_writer.write_json(path, payload)

        for item in payload.get("dados", []):
            deputy_id = item.get("id")
            if deputy_id:
                deputy_ids.append(int(deputy_id))

        if not _has_next_link(payload):
            break
        page += 1

    return sorted(set(deputy_ids))


def _process_partition(
    state_store: IngestionStateStore,
    api: CamaraApiClient,
    raw_writer: AdlsRawWriter,
    deputy_id: int,
    ano: int,
    mes: int,
    pipeline_run_id: str,
    execution_id: str,
    partition_key: str,
) -> None:
    now_iso = datetime.now(UTC).isoformat()
    existing = state_store.get_partition(partition_key) or {}
    next_attempt = int(existing.get("attempt_count", 0)) + 1
    last_page = int(existing.get("last_page_processed", 0))

    state_store.upsert_partition(
        {
            "partition_key": partition_key,
            "entity": "despesas",
            "deputado_id": deputy_id,
            "ano": ano,
            "mes": mes,
            "status": RUNNING,
            "attempt_count": next_attempt,
            "last_page_processed": last_page,
            "started_at": existing.get("started_at", now_iso),
            "execution_id": execution_id,
            "pipeline_run_id": pipeline_run_id,
        }
    )

    page = max(1, last_page + 1)
    ingestion_date = datetime.now(UTC).strftime("%Y-%m-%d")
    try:
        while True:
            payload, _http = api.list_expenses_page(deputy_id=deputy_id, ano=ano, mes=mes, page=page)
            path = (
                "raw/camara/ceap/api/despesas/"
                f"reference_year={ano}/reference_month={mes:02d}/"
                f"pipeline_run_id={pipeline_run_id}/execution_id={execution_id}/deputado_id={deputy_id}/page_{page:03d}.json"
            )
            raw_path = raw_writer.write_json(path, payload)

            state_store.upsert_partition(
                {
                    "partition_key": partition_key,
                    "status": RUNNING,
                    "last_page_processed": page,
                    "raw_path": raw_path,
                    "ingestion_date": ingestion_date,
                }
            )

            if not _has_next_link(payload):
                break
            page += 1

        state_store.upsert_partition(
            {"partition_key": partition_key, "status": SUCCESS, "finished_at": datetime.now(UTC).isoformat()}
        )
    except Exception as exc:
        state_store.upsert_partition(
            {
                "partition_key": partition_key,
                "status": FAILED,
                "error_message": str(exc)[:1024],
                "finished_at": datetime.now(UTC).isoformat(),
            }
        )
        log_structured(logger, "error", "Partition failed.", partition_key=partition_key, error=str(exc))


def _build_monthly_partitions(deputy_ids: list[int]) -> list[tuple[int, int, int]]:
    now = datetime.now(UTC)
    previous_month = now.month - 1 or 12
    previous_year = now.year if now.month > 1 else now.year - 1
    return [(deputy_id, now.year, now.month) for deputy_id in deputy_ids] + [
        (deputy_id, previous_year, previous_month) for deputy_id in deputy_ids
    ]


def _has_next_link(payload: dict) -> bool:
    links = payload.get("links", [])
    return any(link.get("rel") == "next" for link in links)


def _build_state_store() -> IngestionStateStore:
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        raise RuntimeError("AzureWebJobsStorage is required to use ingestion state table.")
    table_name = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    return IngestionStateStore.from_connection_string(conn_str=conn_str, table_name=table_name)
