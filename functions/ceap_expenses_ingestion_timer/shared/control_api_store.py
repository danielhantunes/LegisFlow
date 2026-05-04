from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def control_row_key(ano: int, mes: int, id_deputado: int) -> str:
    return f"{ano}_{mes:02d}_{id_deputado}"


@dataclass
class ControlUnitKey:
    ano: int
    mes: int
    id_deputado: int

    @property
    def row_key(self) -> str:
        return control_row_key(self.ano, self.mes, self.id_deputado)


class IngestionControlApi2026Store:
    """
    Azure Table Storage control plane for CEAP API 2026 units (deputado + ano + mes).

    Physical table name is alphanumeric (default IngestionControlApi2026); set INGESTION_CONTROL_TABLE to override.
    """

    PARTITION_UNITS = "ceap"
    PARTITION_DISPATCH = "_dispatch"
    ROW_DISPATCH_CURSOR = "ceap_api_2026"

    def __init__(self, table_client: TableClient) -> None:
        self.table_client = table_client

    @classmethod
    def from_connection_string(cls, conn_str: str, table_name: str) -> IngestionControlApi2026Store:
        tsc = TableServiceClient.from_connection_string(conn_str)
        tsc.create_table_if_not_exists(table_name=table_name)
        return cls(tsc.get_table_client(table_name=table_name))

    def get_unit(self, ano: int, mes: int, id_deputado: int) -> dict[str, Any] | None:
        rk = control_row_key(ano, mes, id_deputado)
        try:
            return self.table_client.get_entity(partition_key=self.PARTITION_UNITS, row_key=rk)
        except ResourceNotFoundError:
            return None

    def upsert_unit(self, fields: dict[str, Any]) -> None:
        """Merge fields onto the unit row; expects partition_key/row_key or derivable keys."""
        ano = int(fields["ano"])
        mes = int(fields["mes"])
        id_deputado = int(fields["id_deputado"])
        rk = control_row_key(ano, mes, id_deputado)
        entity: dict[str, Any] = {
            "PartitionKey": self.PARTITION_UNITS,
            "RowKey": rk,
            "updated_at": _utc_now_iso(),
        }
        for k, v in fields.items():
            if k in ("partition_key", "row_key", "PartitionKey", "RowKey"):
                continue
            if v is not None:
                entity[k] = v
        self.table_client.upsert_entity(entity=entity, mode="merge")

    def get_dispatch_cursor(self) -> dict[str, Any]:
        try:
            return self.table_client.get_entity(
                partition_key=self.PARTITION_DISPATCH, row_key=self.ROW_DISPATCH_CURSOR
            )
        except ResourceNotFoundError:
            return {
                "PartitionKey": self.PARTITION_DISPATCH,
                "RowKey": self.ROW_DISPATCH_CURSOR,
                "next_pagina": 1,
                "next_idx": 0,
                "next_mes": 1,
            }

    def save_dispatch_cursor(self, *, next_pagina: int, next_idx: int, next_mes: int) -> None:
        self.table_client.upsert_entity(
            entity={
                "PartitionKey": self.PARTITION_DISPATCH,
                "RowKey": self.ROW_DISPATCH_CURSOR,
                "next_pagina": next_pagina,
                "next_idx": next_idx,
                "next_mes": next_mes,
                "updated_at": _utc_now_iso(),
            },
            mode="merge",
        )

    def iter_units_for_replay(
        self,
        *,
        statuses: Iterable[str],
        endpoint: str | None = None,
        id_deputado: int | None = None,
        ano: int | None = None,
        mes: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Scan units table (prefix filter on PartitionKey). Caller should use sparingly in dev/MVP."""
        status_list = list(statuses)
        if not status_list:
            return

        # Table Storage: query partition ceap and filter in code for compound OR / optional filters.
        entities = self.table_client.list_entities(query_filter=f"PartitionKey eq '{self.PARTITION_UNITS}'")
        for ent in entities:
            if ent.get("RowKey", "").startswith("__"):
                continue
            st = str(ent.get("status", "")).lower()
            if st not in {s.lower() for s in status_list}:
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
