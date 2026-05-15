"""ReconciliationControl rows in Azure Table Storage (same table as run registry).

PartitionKey: ``reco_<domain>`` (e.g. ``reco_proposicoes``).
RowKey: opaque ``control_id`` (UUID string).

Used by the controlled reconciliation scheduler + HTTP API. Legacy per-domain
timers may continue to call monolithic ticks until feature flags are flipped.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timezone
from typing import Any, Iterator

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient

RECONCILIATION_STATUSES = frozenset(
    {
        "PENDING",
        "RUNNING",
        "PAUSED",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "LIMIT_REACHED",
    }
)


def reconciliation_partition_key(domain: str) -> str:
    return f"reco_{(domain or '').strip().lower()}"


class ReconciliationControlStore:
    def __init__(self, table_client: TableClient) -> None:
        self.table_client = table_client

    @classmethod
    def from_connection_string(cls, conn_str: str, table_name: str) -> ReconciliationControlStore:
        tsc = TableServiceClient.from_connection_string(conn_str)
        tsc.create_table_if_not_exists(table_name=table_name)
        return cls(tsc.get_table_client(table_name=table_name))

    def get(self, *, domain: str, control_id: str) -> dict[str, Any] | None:
        pk = reconciliation_partition_key(domain)
        try:
            return dict(
                self.table_client.get_entity(partition_key=pk, row_key=control_id)
            )
        except ResourceNotFoundError:
            return None

    def upsert_merge(self, *, domain: str, control_id: str, fields: dict[str, Any]) -> None:
        pk = reconciliation_partition_key(domain)
        cur = self.get(domain=domain, control_id=control_id) or {}
        merged: dict[str, Any] = {}
        for k, v in cur.items():
            if k in ("PartitionKey", "RowKey", "Timestamp", "etag"):
                continue
            merged[k] = v
        for k, v in fields.items():
            if k in ("PartitionKey", "RowKey", "control_id"):
                continue
            if v is None:
                merged.pop(k, None)
            else:
                merged[k] = v
        merged["PartitionKey"] = pk
        merged["RowKey"] = control_id
        merged["domain"] = domain.strip().lower()
        merged["control_id"] = control_id
        merged["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.table_client.upsert_entity(entity=merged, mode="replace")

    def create_running(
        self,
        *,
        domain: str,
        control_id: str,
        pipeline_run_id: str,
        window_start: str,
        window_end: str,
        target_year: int,
        recon_day: int,
        max_tasks_per_run: int,
        max_runtime_minutes: int,
        dry_run: bool,
        context_json: str | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        entity = {
            "PartitionKey": reconciliation_partition_key(domain),
            "RowKey": control_id,
            "control_id": control_id,
            "domain": domain.strip().lower(),
            "status": "RUNNING",
            "pipeline_run_id": pipeline_run_id,
            "window_start": window_start,
            "window_end": window_end,
            "target_year": int(target_year),
            "recon_day": int(recon_day),
            "max_tasks_per_run": int(max_tasks_per_run),
            "max_runtime_minutes": int(max_runtime_minutes),
            "dry_run": bool(dry_run),
            "checkpoint_json": json.dumps(
                {"phase": "list", "list_next_page": 1, "listing_complete": False}
            ),
            "batches_total": 0,
            "records_seen_total": 0,
            "messages_enqueued_total": 0,
            "skipped_same_hash_total": 0,
            "last_batch_status": "",
            "last_batch_at": "",
            "last_error": "",
            "started_at": now,
            "updated_at": now,
            "context_json": context_json or "",
        }
        self.table_client.create_entity(entity=entity)
        return dict(entity)

    def iter_status(self, *, domain: str, status: str) -> Iterator[dict[str, Any]]:
        pk = reconciliation_partition_key(domain)
        filt = f"PartitionKey eq '{pk}' and status eq '{status}'"
        for ent in self.table_client.query_entities(query_filter=filt):
            yield dict(ent)

    def has_running(self, *, domain: str) -> bool:
        return any(True for _ in self.iter_status(domain=domain, status="RUNNING"))


def new_control_id() -> str:
    return str(uuid.uuid4())
