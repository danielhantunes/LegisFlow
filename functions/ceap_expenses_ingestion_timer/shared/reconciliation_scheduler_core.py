"""Cross-domain reconciliation scheduler (picks RUNNING control rows)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .logger import get_logger, log_structured
from .reconciliation_control_store import ReconciliationControlStore
from .reconciliation_proposicoes_controlled import run_proposicoes_controlled_batch

logger = get_logger()

# Extend with eventos / discursos / ceap / votacoes when handlers exist.
SCHEDULER_DOMAINS: tuple[str, ...] = ("proposicoes",)


def execute_reconciliation_scheduler_tick(
    *,
    conn: str,
    control_table: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Process at most one RUNNING control per domain (cost guard)."""
    now = now or datetime.now(timezone.utc)
    store = ReconciliationControlStore.from_connection_string(conn, control_table)
    batches: list[dict[str, Any]] = []
    for dom in SCHEDULER_DOMAINS:
        picked = False
        for ent in store.iter_status(domain=dom, status="RUNNING"):
            if picked:
                break
            if dom == "proposicoes":
                batches.append(run_proposicoes_controlled_batch(control=dict(ent), now=now))
            else:
                log_structured(
                    logger,
                    "warning",
                    "reconciliation scheduler domain not wired",
                    domain=dom,
                )
            picked = True
    if batches:
        log_structured(
            logger,
            "info",
            "reconciliation_scheduler tick",
            domains_checked=len(SCHEDULER_DOMAINS),
            batches_run=len(batches),
        )
    return {"domains_checked": list(SCHEDULER_DOMAINS), "batches": batches}
