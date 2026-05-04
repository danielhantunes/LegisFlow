from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient


@dataclass
class PartitionState:
    partition_key: str
    row_key: str
    status: str
    attempt_count: int = 0
    last_page_processed: int = 0


class IngestionStateStore:
    def __init__(self, table_client: TableClient) -> None:
        self.table_client = table_client

    @classmethod
    def from_connection_string(cls, conn_str: str, table_name: str) -> IngestionStateStore:
        tsc = TableServiceClient.from_connection_string(conn_str)
        tsc.create_table_if_not_exists(table_name=table_name)
        return cls(tsc.get_table_client(table_name=table_name))

    def acquire_lock(self, lock_name: str, execution_id: str, ttl_minutes: int = 30) -> bool:
        now = datetime.now(timezone.utc)
        entity = {
            "PartitionKey": "LOCK",
            "RowKey": lock_name,
            "status": "RUNNING",
            "execution_id": execution_id,
            "updated_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=ttl_minutes)).isoformat(),
        }
        try:
            current = self.table_client.get_entity(partition_key="LOCK", row_key=lock_name)
            expires_at = datetime.fromisoformat(current["expires_at"])
            if expires_at > now and current.get("status") == "RUNNING":
                return False
        except ResourceNotFoundError:
            pass

        self.table_client.upsert_entity(entity=entity, mode="replace")
        return True

    def release_lock(self, lock_name: str) -> None:
        self.table_client.upsert_entity(
            entity={
                "PartitionKey": "LOCK",
                "RowKey": lock_name,
                "status": "SUCCESS",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            mode="merge",
        )

    def get_partition(self, partition_key: str) -> dict[str, Any] | None:
        try:
            return self.table_client.get_entity(partition_key="CEAP", row_key=partition_key)
        except ResourceNotFoundError:
            return None

    def upsert_partition(self, record: dict[str, Any]) -> None:
        entity = {
            "PartitionKey": "CEAP",
            "RowKey": record["partition_key"],
            **record,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.table_client.upsert_entity(entity=entity, mode="merge")
