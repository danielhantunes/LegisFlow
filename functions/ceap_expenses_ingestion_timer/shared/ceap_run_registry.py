"""Run registry + dispatcher lock on IngestionControlApi2026 (_runs, _locks)."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient


def pipeline_run_row_key(pipeline_run_id: str) -> str:
    """RowKey equals pipeline_run_id (e.g. ceap_daily_20260504)."""
    return pipeline_run_id


def pipeline_run_updates_registry(pipeline_run_id: str) -> bool:
    """Automated daily/reconciliation runs update IngestionControlApi2026 counters; manual replay does not."""
    p = (pipeline_run_id or "").strip()
    return p.startswith("ceap_daily_") or p.startswith("ceap_reconciliation_")


class CeapRunRegistry:
    PARTITION_RUNS = "_runs"
    PARTITION_LOCKS = "_locks"
    PARTITION_SNAPSHOTS = "_snapshots"
    ROW_LOCK = "ceap_dispatcher_lock"

    def __init__(self, table_client: TableClient) -> None:
        self.table_client = table_client

    @staticmethod
    def snapshot_row_key(reference_date: str) -> str:
        compact = (reference_date or "").replace("-", "")
        return f"deputados_{compact}"

    @classmethod
    def from_connection_string(cls, conn_str: str, table_name: str) -> CeapRunRegistry:
        tsc = TableServiceClient.from_connection_string(conn_str)
        tsc.create_table_if_not_exists(table_name=table_name)
        return cls(tsc.get_table_client(table_name=table_name))

    def get_run(self, pipeline_run_id: str) -> dict[str, Any] | None:
        rk = pipeline_run_row_key(pipeline_run_id)
        try:
            return dict(self.table_client.get_entity(partition_key=self.PARTITION_RUNS, row_key=rk))
        except ResourceNotFoundError:
            return None

    def upsert_run(self, fields: dict[str, Any]) -> None:
        pid = fields["pipeline_run_id"]
        rk = pipeline_run_row_key(pid)
        entity: dict[str, Any] = {
            "PartitionKey": self.PARTITION_RUNS,
            "RowKey": rk,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            **{k: v for k, v in fields.items() if k != "pipeline_run_id" and v is not None},
        }
        self.table_client.upsert_entity(entity=entity, mode="merge")

    def try_acquire_dispatcher_lock(
        self,
        *,
        mode: str,
        pipeline_run_id: str,
        ttl_minutes: int = 15,
    ) -> tuple[bool, str]:
        """Returns (acquired, lock_token)."""
        now = datetime.now(timezone.utc)
        token = str(uuid.uuid4())
        until = now + timedelta(minutes=ttl_minutes)
        try:
            cur = self.table_client.get_entity(partition_key=self.PARTITION_LOCKS, row_key=self.ROW_LOCK)
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
                "PartitionKey": self.PARTITION_LOCKS,
                "RowKey": self.ROW_LOCK,
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
            cur = self.table_client.get_entity(partition_key=self.PARTITION_LOCKS, row_key=self.ROW_LOCK)
            if str(cur.get("locked_by", "")) != token:
                return
        except ResourceNotFoundError:
            return
        now = datetime.now(timezone.utc).isoformat()
        self.table_client.upsert_entity(
            entity={
                "PartitionKey": self.PARTITION_LOCKS,
                "RowKey": self.ROW_LOCK,
                "locked_by": "",
                "locked_until": now,
                "updated_at": now,
            },
            mode="merge",
        )

    def get_snapshot(self, reference_date: str) -> dict[str, Any] | None:
        rk = self.snapshot_row_key(reference_date)
        try:
            return dict(
                self.table_client.get_entity(
                    partition_key=self.PARTITION_SNAPSHOTS, row_key=rk
                )
            )
        except ResourceNotFoundError:
            return None

    def upsert_snapshot(self, reference_date: str, fields: dict[str, Any]) -> None:
        rk = self.snapshot_row_key(reference_date)
        entity: dict[str, Any] = {
            "PartitionKey": self.PARTITION_SNAPSHOTS,
            "RowKey": rk,
            "endpoint": "deputados",
            "reference_date": reference_date,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            **{k: v for k, v in fields.items() if v is not None},
        }
        self.table_client.upsert_entity(entity=entity, mode="merge")

    def find_latest_completed_snapshot_record(
        self, *, before_reference_date: str | None = None
    ) -> dict[str, Any] | None:
        """Returns the most recent ``_snapshots`` record whose ``status == 'COMPLETED'``.

        If ``before_reference_date`` is provided, snapshots whose ``reference_date >=`` that
        value are excluded (used for fallback when today's snapshot is incomplete).
        """
        flt = (
            f"PartitionKey eq '{self.PARTITION_SNAPSHOTS}' and endpoint eq 'deputados'"
        )
        candidates: list[dict[str, Any]] = []
        for ent in self.table_client.list_entities(filter=flt):
            if str(ent.get("status", "")).upper() != "COMPLETED":
                continue
            ref_dt = str(ent.get("reference_date", ""))
            if before_reference_date and ref_dt >= before_reference_date:
                continue
            candidates.append(dict(ent))
        if not candidates:
            return None
        candidates.sort(key=lambda e: str(e.get("reference_date", "")), reverse=True)
        return candidates[0]

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
        self.upsert_run(patch)
