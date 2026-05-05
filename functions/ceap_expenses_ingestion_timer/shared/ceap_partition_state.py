"""Partition state for CEAP API ingestion (Table Storage: IngestionState, PK ceap_2026)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient


def partition_row_key(id_deputado: int, ano: int, mes: int) -> str:
    return f"despesas|{id_deputado}|{ano}|{mes}"


class CeapPartitionStateStore:
    PARTITION_KEY = "ceap_2026"

    def __init__(self, table_client: TableClient) -> None:
        self.table_client = table_client

    @classmethod
    def from_connection_string(cls, conn_str: str, table_name: str) -> CeapPartitionStateStore:
        tsc = TableServiceClient.from_connection_string(conn_str)
        tsc.create_table_if_not_exists(table_name=table_name)
        return cls(tsc.get_table_client(table_name=table_name))

    def get_partition(self, id_deputado: int, ano: int, mes: int) -> dict[str, Any] | None:
        rk = partition_row_key(id_deputado, ano, mes)
        try:
            return self.table_client.get_entity(partition_key=self.PARTITION_KEY, row_key=rk)
        except ResourceNotFoundError:
            return None

    def upsert_partition(self, fields: dict[str, Any]) -> None:
        rk = partition_row_key(int(fields["id_deputado"]), int(fields["ano"]), int(fields["mes"]))
        entity: dict[str, Any] = {
            "PartitionKey": self.PARTITION_KEY,
            "RowKey": rk,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for k, v in fields.items():
            if k in ("PartitionKey", "RowKey", "partition_key", "row_key"):
                continue
            if v is not None:
                entity[k] = v
        self.table_client.upsert_entity(entity=entity, mode="merge")

    def iter_partitions_for_replay(
        self,
        *,
        statuses: list[str],
        endpoint: str | None = None,
        id_deputado: int | None = None,
        ano: int | None = None,
        mes: int | None = None,
    ) -> Any:
        entities = self.table_client.list_entities(query_filter=f"PartitionKey eq '{self.PARTITION_KEY}'")
        want = {str(s).strip().upper() for s in statuses if str(s).strip()}
        for ent in entities:
            st = str(ent.get("status", "")).upper()
            if st not in want:
                continue
            if endpoint is not None and str(ent.get("endpoint", "ceap")).lower() != endpoint.lower():
                continue
            if id_deputado is not None and int(ent.get("id_deputado", -1)) != id_deputado:
                continue
            if ano is not None and int(ent.get("ano", -1)) != ano:
                continue
            if mes is not None and int(ent.get("mes", -1)) != mes:
                continue
            yield ent
