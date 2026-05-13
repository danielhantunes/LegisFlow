"""Shared helpers for ``votacoes_api_dispatcher`` (microbatch + reconciliation).

Pure-ish helpers and small orchestration pieces kept out of ``__init__.py``
for readability and reuse from the manual HTTP starter.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from .domain_catalog import DomainSpec, VOTACOES_DOMAIN
from .logger import get_logger
from .queue_helpers import prepare_queue_client_for_dispatch, send_json_message_with_client
from .queue_messages import DomainWorkMessage
from .raw_audit import _resolve_dotted, build_record_uid_from_keys, compute_record_hash
from .generic_partition_state import GenericPartitionStateStore

logger = get_logger()


def list_item_uid_hash(
    domain: DomainSpec,
    *,
    endpoint_name: str,
    business_key_fields: tuple[str, ...],
    item: dict[str, Any],
) -> tuple[str, str]:
    """Stable (uid, hash) fingerprint for one ``/votacoes`` list row."""
    keys: dict[str, Any] = {}
    for field in business_key_fields:
        keys[field] = _resolve_dotted(item, field)
    uid = build_record_uid_from_keys(
        source_system=domain.source_system,
        entity=endpoint_name,
        business_keys=keys,
    )
    clean = {k: v for k, v in item.items() if not str(k).startswith("_")}
    return uid, compute_record_hash(clean)


def reenqueue_stale_votacoes_tasks(
    *,
    parts: GenericPartitionStateStore,
    queue_client: Any,
    pipeline_run_id: str,
    votos_endpoint_name: str,
    stale_after_minutes: int,
    now: datetime,
    logger_: Any,
    window_start_utc: str,
    window_end_utc: str,
    run_type: str,
) -> int:
    """Re-enqueue QUEUED rows older than ``stale_after_minutes`` for this run."""
    if stale_after_minutes <= 0:
        return 0
    safe = pipeline_run_id.replace("'", "''")
    flt = (
        f"PartitionKey eq '{parts.partition_key}' "
        f"and current_pipeline_run_id eq '{safe}' "
        f"and status eq 'QUEUED'"
    )
    cutoff = now - timedelta(minutes=stale_after_minutes)
    requeued = 0
    for ent in parts.table_client.list_entities(filter=flt):
        last_disp = str(ent.get("last_dispatched_at") or "").strip()
        if not last_disp:
            continue
        try:
            ts = datetime.fromisoformat(last_disp.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
        if ts > cutoff:
            continue
        vid = str(ent.get("votacao_id") or "").strip()
        if not vid:
            continue
        row_key = str(ent.get("RowKey") or "")
        if not row_key:
            continue
        execution_id = str(uuid.uuid4())
        dispatched_at = now.isoformat()
        wm = DomainWorkMessage(
            domain=VOTACOES_DOMAIN.name,
            endpoint=votos_endpoint_name,
            pipeline_run_id=pipeline_run_id,
            run_type=run_type,
            payload={
                "votacao_id": vid,
                "window_start_utc": window_start_utc,
                "window_end_utc": window_end_utc,
                "list_record_uid": str(ent.get("last_votacao_list_record_uid") or ""),
                "list_record_hash": str(ent.get("last_votacao_list_record_hash") or ""),
            },
            execution_id=execution_id,
            dispatched_at=dispatched_at,
        )
        send_json_message_with_client(
            queue_client,
            wm.to_json(),
            logger=logger_,
            domain=VOTACOES_DOMAIN.name,
            pipeline_run_id=pipeline_run_id,
            endpoint=votos_endpoint_name,
            votacao_id=vid,
        )
        stale_n = int(ent.get("stale_requeue_count", 0) or 0) + 1
        parts.upsert_partition(
            row_key,
            {
                "status": "QUEUED",
                "last_dispatched_at": dispatched_at,
                "last_execution_id": execution_id,
                "stale_requeue_count": stale_n,
            },
        )
        requeued += 1
    return requeued


def count_votacoes_in_date_range_dry_run(
    *,
    api: Any,
    list_endpoint: Any,
    date_start: str,
    date_end: str,
    max_pages: int,
) -> tuple[int, int, list[str]]:
    """Returns (distinct_ids_count, pages_fetched, warnings)."""
    warnings: list[str] = []
    seen: set[str] = set()
    page = 1
    while page <= max_pages:
        payload, _http = api.list_votacoes_page(
            page=page,
            itens=list_endpoint.items_per_page,
            date_start=date_start,
            date_end=date_end,
        )
        dados = payload.get("dados") or []
        for item in dados:
            if isinstance(item, dict) and item.get("id") is not None:
                seen.add(str(item.get("id")))
        links = payload.get("links") or []
        has_next = any(
            isinstance(li, dict) and li.get("rel") == "next" for li in links
        )
        if not has_next:
            break
        page += 1
    if page >= max_pages:
        warnings.append("max_list_pages_reached_during_count")
    return len(seen), page, warnings


def validate_manual_votacoes_reconciliation_dates(
    *,
    target_year: int,
    date_start: str,
    date_end: str,
    allow_year_mismatch: bool,
) -> list[str]:
    """Return human-readable validation errors (empty list = ok)."""
    errors: list[str] = []
    try:
        ds = datetime.fromisoformat(date_start)
        de = datetime.fromisoformat(date_end)
    except ValueError:
        errors.append("invalid_iso_date")
        return errors
    if ds.date() > de.date():
        errors.append("date_start_after_date_end")
    if not allow_year_mismatch:
        if ds.year != target_year or de.year != target_year:
            errors.append("dates_must_match_target_year_or_set_VOTACOES_MANUAL_ALLOW_YEAR_MISMATCH")
    return errors
