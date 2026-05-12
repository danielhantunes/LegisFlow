"""Generic IngestionState helper for non-CEAP domains.

The CEAP store (:class:`shared.ceap_partition_state.CeapPartitionStateStore`)
embeds a fixed PartitionKey (``ceap_2026``) and CEAP-specific columns. New
domains share the same physical Table but use their own PartitionKey (declared
in :mod:`shared.domain_catalog`) and a free-form ``RowKey`` chosen by the
caller (e.g. ``partidos|2026-05-11``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient


class GenericPartitionStateStore:
    def __init__(
        self,
        table_client: TableClient,
        *,
        partition_key: str,
    ) -> None:
        self.table_client = table_client
        self.partition_key = partition_key

    @classmethod
    def from_connection_string(
        cls,
        conn_str: str,
        table_name: str,
        *,
        partition_key: str,
    ) -> GenericPartitionStateStore:
        tsc = TableServiceClient.from_connection_string(conn_str)
        tsc.create_table_if_not_exists(table_name=table_name)
        return cls(
            tsc.get_table_client(table_name=table_name),
            partition_key=partition_key,
        )

    def get_partition(self, row_key: str) -> dict[str, Any] | None:
        try:
            return dict(
                self.table_client.get_entity(
                    partition_key=self.partition_key,
                    row_key=row_key,
                )
            )
        except ResourceNotFoundError:
            return None

    def upsert_partition(self, row_key: str, fields: dict[str, Any]) -> None:
        entity: dict[str, Any] = {
            "PartitionKey": self.partition_key,
            "RowKey": row_key,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for k, v in fields.items():
            if k in ("PartitionKey", "RowKey"):
                continue
            if v is not None:
                entity[k] = v
        self.table_client.upsert_entity(entity=entity, mode="merge")

    def count_statuses_by_run(self, pipeline_run_id: str) -> dict[str, int]:
        """Counts state rows whose current OR last pipeline_run_id matches."""
        counts: dict[str, int] = {
            "queued": 0,
            "running": 0,
            "success": 0,
            "failed": 0,
            "poison": 0,
            "pending": 0,
            "stale": 0,
            "other": 0,
        }
        if not pipeline_run_id:
            return counts
        safe_run = pipeline_run_id.replace("'", "''")
        flt = (
            f"PartitionKey eq '{self.partition_key}' "
            f"and (current_pipeline_run_id eq '{safe_run}' "
            f"or last_pipeline_run_id eq '{safe_run}')"
        )
        for ent in self.table_client.list_entities(filter=flt):
            st = str(ent.get("status", "")).upper()
            if st == "QUEUED":
                counts["queued"] += 1
            elif st == "RUNNING":
                counts["running"] += 1
            elif st == "SUCCESS":
                counts["success"] += 1
            elif st == "FAILED":
                counts["failed"] += 1
            elif st == "POISON":
                counts["poison"] += 1
            elif st == "PENDING":
                counts["pending"] += 1
            elif st == "STALE":
                counts["stale"] += 1
            else:
                counts["other"] += 1
        return counts

    def iter_for_replay(
        self,
        *,
        statuses: list[str],
        endpoint: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> Any:
        flt = f"PartitionKey eq '{self.partition_key}'"
        if pipeline_run_id:
            safe_run = pipeline_run_id.replace("'", "''")
            flt += (
                f" and (current_pipeline_run_id eq '{safe_run}' "
                f"or last_pipeline_run_id eq '{safe_run}')"
            )
        want = {str(s).strip().upper() for s in statuses if str(s).strip()}
        for ent in self.table_client.list_entities(filter=flt):
            st = str(ent.get("status", "")).upper()
            if st not in want:
                continue
            if endpoint is not None and str(ent.get("endpoint", "")).lower() != endpoint.lower():
                continue
            yield ent
