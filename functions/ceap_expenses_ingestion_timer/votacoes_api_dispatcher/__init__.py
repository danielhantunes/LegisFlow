"""Timer: votações API dispatcher — microbatch (10 em 10 min, UTC) + reconciliação mensal.

* **Microbatch**: ``votacoes_microbatch_YYYYMMDDHHmm``, janela curta em dias
  (``VOTACOES_MICROBATCH_DATE_WINDOW_DAYS``, default 2), listagem ``ordenarPor=id``
  ASC e cursor global ``last_processed_votacao_id`` (Table) avançado só pelo worker.
* **Reconciliação** (dia ``VOTACOES_RECONCILIATION_DAY``, UTC): ``votacoes_reconciliation_YYYYMMDD``,
  varre ``TARGET_YEAR``-01-01 até hoje, com retoma paginada e ``discovered_fingerprints.json``.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, time, timedelta

import azure.functions as func

from shared.domain_catalog import VOTACOES_DOMAIN, votacoes_microbatch_run_id, votacoes_reconciliation_run_id
from shared.generic_partition_state import GenericPartitionStateStore
from shared.logger import get_logger, log_structured
from shared.run_registry import GenericRunRegistry
from shared.votacoes_dispatcher_tick import execute_votacoes_ingestion_tick

logger = get_logger()


def _round_minute_down(now: datetime, granularity_min: int) -> datetime:
    minute = (now.minute // granularity_min) * granularity_min
    return now.replace(minute=minute, second=0, microsecond=0)


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    domain = VOTACOES_DOMAIN
    now = datetime.now(UTC)
    recon_day = max(1, min(28, int(os.getenv("VOTACOES_RECONCILIATION_DAY", "25"))))
    granularity = max(1, int(os.getenv("VOTACOES_DISPATCH_GRANULARITY_MIN", "10")))
    target_year = int(os.getenv("TARGET_YEAR", str(now.year)))

    if now.day == recon_day:
        mode = "reconciliation"
        pipeline_run_id = votacoes_reconciliation_run_id(now.strftime("%Y-%m-%d"))
        date_start = f"{target_year}-01-01"
        date_end = now.date().isoformat()
        window_start = datetime(target_year, 1, 1, tzinfo=UTC)
        window_end = now
        run_type_label = "reconciliation"
    else:
        skip_weekend = str(
            os.getenv("VOTACOES_SKIP_WEEKEND_MICROBATCH", "")
        ).lower() in ("1", "true", "yes")
        if skip_weekend and now.weekday() >= 5:
            log_structured(
                logger,
                "info",
                "Votacoes microbatch skipped (weekend).",
                domain=domain.name,
                weekday=now.weekday(),
            )
            return
        mode = "microbatch"
        anchor = _round_minute_down(now, granularity)
        pipeline_run_id = votacoes_microbatch_run_id(anchor.strftime("%Y-%m-%dT%H:%M"))
        window_days = max(
            1, int(os.getenv("VOTACOES_MICROBATCH_DATE_WINDOW_DAYS", "2"))
        )
        window_end = now
        date_end_d = now.date()
        date_start_d = date_end_d - timedelta(days=window_days - 1)
        date_start = date_start_d.isoformat()
        date_end = date_end_d.isoformat()
        window_start = datetime.combine(date_start_d, time.min, tzinfo=UTC)
        run_type_label = "microbatch"

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    state_table = os.getenv("INGESTION_STATE_TABLE", "IngestionState")
    queue_name = os.getenv("VOTACOES_QUEUE_NAME", domain.queue_work)
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    lock_ttl = int(os.getenv("VOTACOES_LOCK_TTL_MINUTES", str(domain.lock_ttl_minutes)))
    max_messages_per_tick = max(1, int(os.getenv("VOTACOES_MAX_MESSAGES_PER_TICK", "500")))
    max_list_pages = int(os.getenv("VOTACOES_MAX_LIST_PAGES", "200"))
    max_pages_tick = max(1, int(os.getenv("VOTACOES_RECON_MAX_PAGES_PER_TICK", "40")))
    stale_raw = os.getenv("VOTACOES_STALE_AFTER_MINUTES")
    stale_after = (
        int(stale_raw)
        if stale_raw not in (None, "")
        else int(domain.stale_after_minutes)
    )

    registry = GenericRunRegistry.from_connection_string(
        conn,
        control_table,
        runs_partition_key=domain.runs_partition_key,
        locks_partition_key=domain.locks_partition_key,
        lock_row_key=domain.lock_row_key,
    )
    parts = GenericPartitionStateStore.from_connection_string(
        conn, state_table, partition_key=domain.state_partition_key
    )

    execute_votacoes_ingestion_tick(
        domain=domain,
        now=now,
        registry=registry,
        parts=parts,
        raw_account=raw_account,
        queue_name=queue_name,
        lock_ttl=lock_ttl,
        max_messages_per_tick=max_messages_per_tick,
        max_list_pages=max_list_pages,
        max_pages_tick=max_pages_tick,
        stale_after=stale_after,
        pipeline_run_id=pipeline_run_id,
        mode=mode,
        run_type_label=run_type_label,
        date_start=date_start,
        date_end=date_end,
        window_start=window_start,
        window_end=window_end,
        target_year=target_year,
        recon_day=recon_day,
    )
