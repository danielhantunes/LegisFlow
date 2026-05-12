"""Generic queue messages used by all NEW domains.

CEAP keeps using its own ``shared.work_message.CeapApiWorkMessage`` to avoid
breaking the existing wire format. Every new domain uses
:class:`DomainWorkMessage` which carries a free-form ``payload`` dict for
domain-specific fields (e.g. ``{"reference_date": "2026-05-11"}`` for
reference snapshots, or ``{"votacao_id": 12345}`` for fanout).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class DomainWorkMessage:
    """One queue message routed to a domain worker.

    Required fields (always present in the JSON body):

    * ``domain``    — declared in :mod:`shared.domain_catalog` (e.g. "reference").
    * ``endpoint``  — endpoint name within the domain (e.g. "partidos").
    * ``pipeline_run_id`` — owning run id (matches a row in IngestionControlApi2026).
    * ``run_type`` — "snapshot" / "daily" / "reconciliation" / "microbatch" / etc.
    * ``payload``  — free-form dict for domain-specific args.
    * ``execution_id`` (optional) — set by dispatcher for log correlation.
    * ``dispatched_at`` — ISO-8601 UTC; auto-filled by ``to_json`` if missing.
    """

    domain: str
    endpoint: str
    pipeline_run_id: str
    run_type: str = "snapshot"
    payload: dict[str, Any] = field(default_factory=dict)
    execution_id: str = ""
    dispatched_at: str = ""

    def to_json(self) -> str:
        d = asdict(self)
        if not d.get("dispatched_at"):
            d["dispatched_at"] = datetime.now(UTC).isoformat()
        return json.dumps(d, ensure_ascii=False)

    @classmethod
    def from_queue_body(cls, body: bytes | str) -> DomainWorkMessage:
        raw = body.decode("utf-8") if isinstance(body, bytes) else str(body)
        data: dict[str, Any] = json.loads(raw)
        payload = data.get("payload") or {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        return cls(
            domain=str(data.get("domain", "")).strip(),
            endpoint=str(data.get("endpoint", "")).strip(),
            pipeline_run_id=str(data.get("pipeline_run_id", "")).strip(),
            run_type=str(data.get("run_type", "snapshot")).strip(),
            payload=payload,
            execution_id=str(data.get("execution_id", "")).strip(),
            dispatched_at=str(data.get("dispatched_at", "")).strip(),
        )

    def matches_pipeline_run_id(self, pipeline_run_id: str) -> bool:
        return self.pipeline_run_id == (pipeline_run_id or "").strip()
