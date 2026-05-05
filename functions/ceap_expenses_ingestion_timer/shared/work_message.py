from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass
class CeapApiWorkMessage:
    """One queue work unit: CEAP despesas for one deputy / year / month (API slice)."""

    endpoint: str
    id_deputado: int
    ano: int
    mes: int
    mode: str = "daily"
    pipeline_run_id: str = ""
    dispatched_at: str = ""

    def to_json(self) -> str:
        d = asdict(self)
        if not d.get("dispatched_at"):
            d["dispatched_at"] = datetime.now(UTC).isoformat()
        return json.dumps(d, ensure_ascii=False)

    @classmethod
    def from_queue_body(cls, body: bytes | str) -> CeapApiWorkMessage:
        raw = body.decode("utf-8") if isinstance(body, bytes) else str(body)
        data: dict[str, Any] = json.loads(raw)
        return cls(
            endpoint=str(data.get("endpoint", "ceap")),
            id_deputado=int(data["id_deputado"]),
            ano=int(data["ano"]),
            mes=int(data["mes"]),
            mode=str(data.get("mode", "daily")),
            pipeline_run_id=str(data.get("pipeline_run_id", "")),
            dispatched_at=str(data.get("dispatched_at", "")),
        )
