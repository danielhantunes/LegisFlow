"""Timer: proposições daily dispatcher (06:15 UTC default).

Lists ``/proposicoes`` once per UTC day, persists a single JSONL snapshot under
``raw/camara/proposicoes/api/list/year=…/month=…/day=…/run_id=…/``, fans out to
autores/tramitacoes with list-hash idempotency (Table: ``last_list_item_hash``).
"""

from __future__ import annotations

from datetime import UTC, datetime

import azure.functions as func

from shared.proposicoes_daily_tick import execute_proposicoes_daily_tick


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    execute_proposicoes_daily_tick(now=datetime.now(UTC))
