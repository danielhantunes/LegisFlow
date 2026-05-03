from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class CeapApiWorkMessage:
    """One queue work unit: CEAP despesas for one deputy / year / month (API slice)."""

    endpoint: str
    id_deputado: int
    ano: int
    mes: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_queue_body(cls, body: bytes | str) -> CeapApiWorkMessage:
        raw = body.decode("utf-8") if isinstance(body, bytes) else str(body)
        data: dict[str, Any] = json.loads(raw)
        return cls(
            endpoint=str(data.get("endpoint", "ceap")),
            id_deputado=int(data["id_deputado"]),
            ano=int(data["ano"]),
            mes=int(data["mes"]),
        )
