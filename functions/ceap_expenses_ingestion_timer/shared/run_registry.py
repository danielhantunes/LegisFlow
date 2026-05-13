"""Generic run registry / dispatcher lock on Table Storage.

Mirrors :class:`shared.ceap_run_registry.CeapRunRegistry` but takes the
PartitionKey + lock RowKey as constructor parameters so each new domain can
share the same physical table (``IngestionControlApi2026``) without colliding
with other domains.

The CEAP code keeps using ``CeapRunRegistry`` unchanged (this module is opt-in
for the new domains).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient


class GenericRunRegistry:
    def __init__(
        self,
        table_client: TableClient,
        *,
        runs_partition_key: str,
        locks_partition_key: str,
        lock_row_key: str,
    ) -> None:
        self.table_client = table_client
        self.runs_partition_key = runs_partition_key
        self.locks_partition_key = locks_partition_key
        self.lock_row_key = lock_row_key

    @classmethod
    def from_connection_string(
        cls,
        conn_str: str,
        table_name: str,
        *,
        runs_partition_key: str,
        locks_partition_key: str,
        lock_row_key: str,
    ) -> GenericRunRegistry:
        tsc = TableServiceClient.from_connection_string(conn_str)
        tsc.create_table_if_not_exists(table_name=table_name)
        return cls(
            tsc.get_table_client(table_name=table_name),
            runs_partition_key=runs_partition_key,
            locks_partition_key=locks_partition_key,
            lock_row_key=lock_row_key,
        )

    def get_run(self, pipeline_run_id: str) -> dict[str, Any] | None:
        try:
            return dict(
                self.table_client.get_entity(
                    partition_key=self.runs_partition_key,
                    row_key=pipeline_run_id,
                )
            )
        except ResourceNotFoundError:
            return None

    def upsert_run(self, fields: dict[str, Any]) -> None:
        """Merge ``fields`` into the existing run row and persist with *replace*.

        Table Storage ``merge`` updates can fail to clear counters when callers
        explicitly set numeric fields to ``0`` / ``False`` (payload quirks in
        some host/SDK combinations). We therefore read the latest row, merge in
        Python, and ``upsert_entity(..., mode="replace")`` so every property we
        keep is written exactly as intended.
        """
        pid = fields["pipeline_run_id"]
        raw = self.get_run(pid)
        merged: dict[str, Any] = {}
        if raw:
            for key, val in raw.items():
                if key in ("PartitionKey", "RowKey", "Timestamp", "etag"):
                    continue
                merged[key] = val
        for key, val in fields.items():
            if key == "pipeline_run_id":
                continue
            if val is None:
                merged.pop(key, None)
            else:
                merged[key] = val
        merged["PartitionKey"] = self.runs_partition_key
        merged["RowKey"] = pid
        merged["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.table_client.upsert_entity(entity=merged, mode="replace")

    def try_acquire_dispatcher_lock(
        self,
        *,
        mode: str,
        pipeline_run_id: str,
        ttl_minutes: int = 15,
    ) -> tuple[bool, str]:
        now = datetime.now(timezone.utc)
        token = str(uuid.uuid4())
        until = now + timedelta(minutes=ttl_minutes)
        try:
            cur = self.table_client.get_entity(
                partition_key=self.locks_partition_key,
                row_key=self.lock_row_key,
            )
            lu = cur.get("locked_until")
            if lu:
                try:
                    exp = datetime.fromisoformat(str(lu).replace("Z", "+00:00"))
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if exp > now and str(cur.get("locked_by", "")):
                        return False, ""
                except (TypeError, ValueError):
                    pass
        except ResourceNotFoundError:
            pass

        self.table_client.upsert_entity(
            entity={
                "PartitionKey": self.locks_partition_key,
                "RowKey": self.lock_row_key,
                "locked_by": token,
                "locked_at": now.isoformat(),
                "locked_until": until.isoformat(),
                "mode": mode,
                "pipeline_run_id": pipeline_run_id,
                "updated_at": now.isoformat(),
            },
            mode="replace",
        )
        return True, token

    def release_dispatcher_lock(self, token: str) -> None:
        try:
            cur = self.table_client.get_entity(
                partition_key=self.locks_partition_key,
                row_key=self.lock_row_key,
            )
            if str(cur.get("locked_by", "")) != token:
                return
        except ResourceNotFoundError:
            return
        now = datetime.now(timezone.utc).isoformat()
        self.table_client.upsert_entity(
            entity={
                "PartitionKey": self.locks_partition_key,
                "RowKey": self.lock_row_key,
                "locked_by": "",
                "locked_until": now,
                "updated_at": now,
            },
            mode="merge",
        )

    def merge_run_counters(
        self,
        pipeline_run_id: str,
        *,
        success_delta: int = 0,
        failed_delta: int = 0,
    ) -> None:
        ent = self.get_run(pipeline_run_id)
        if not ent:
            return
        tq_raw = int(ent.get("total_tasks_queued", 0) or 0)
        te = int(ent.get("total_tasks_expected", 0) or 0)
        tq = te if te > 0 else tq_raw
        ts = int(ent.get("total_tasks_success", 0) or 0) + success_delta
        tf = int(ent.get("total_tasks_failed", 0) or 0) + failed_delta
        patch: dict[str, Any] = {
            "pipeline_run_id": pipeline_run_id,
            "total_tasks_success": ts,
            "total_tasks_failed": tf,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if tq > 0 and ts + tf >= tq:
            patch["status"] = "COMPLETED" if tf == 0 else "PARTIAL"
            patch["completed_at"] = datetime.now(timezone.utc).isoformat()
        elif ts + tf > 0:
            patch["status"] = "RUNNING"
        else:
            patch.setdefault("status", str(ent.get("status", "QUEUED")))

        if str(patch.get("status", "")).upper() == "COMPLETED":
            had_failure_markers = bool(ent.get("failed_at")) or bool(
                str(ent.get("last_error", "")).strip()
            )
            if had_failure_markers or str(ent.get("status", "")).upper() in {
                "FAILED",
                "PARTIAL",
            }:
                patch["last_recovered_at"] = datetime.now(timezone.utc).isoformat()
            patch["failed_at"] = ""
            patch["last_error"] = ""
        self.upsert_run(patch)
