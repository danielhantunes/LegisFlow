"""Raw-layer audit/lineage helpers (deterministic, no Bronze/Silver/Gold deps).

This module standardises the **technical audit envelope** written into every
Raw page (``page_*.json``) and provides deterministic functions to compute:

* ``_payload_hash`` for the API page payload (SHA-256 over canonical JSON).
* ``_record_uid`` and ``_record_hash`` for individual records when business
  keys are stable (CEAP despesas, /deputados list).

It is intentionally **independent** from any Spark/Delta concept. The fields
written here are designed to be propagated to Bronze/Silver/Gold *in the
future*, but no current consumer depends on them.

Conventions:

* Canonical JSON: ``json.dumps(obj, sort_keys=True, ensure_ascii=False,
  separators=(",", ":"))`` — stable across Python versions / platforms.
* Hashes: SHA-256, hex digest (``str``).
* Decimals are normalised to ``str(Decimal(...))`` before hashing to avoid
  float drift (``valorDocumento`` etc.).
* No random UUIDs are used to derive ``_record_uid``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

AUDIT_KEY = "_audit"
RECORD_UID_KEY = "_record_uid"
RECORD_HASH_KEY = "_record_hash"

CEAP_SOURCE_SYSTEM = "camara_dadosabertos"
CEAP_API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
CEAP_DESPESAS_API_PATH = "/deputados/{id}/despesas"
DEPUTIES_API_PATH = "/deputados"

CEAP_ENTITY = "deputado_despesas"
CEAP_DOMAIN = "ceap"
DEPUTIES_ENTITY = "deputado"
DEPUTIES_DOMAIN = "deputados"

# Business keys used to derive a stable ``_record_uid`` for CEAP despesas.
# Fields are looked up in the API item; ``None`` is preserved (no fallback)
# to keep the hash deterministic across re-ingestions.
CEAP_RECORD_UID_FIELDS: tuple[str, ...] = (
    "codDocumento",
    "numDocumento",
    "dataDocumento",
    "tipoDespesa",
    "urlDocumento",
    "parcela",
    "numRessarcimento",
    "cnpjCpfFornecedor",
    "valorDocumento",
)


def now_utc_iso() -> str:
    """ISO-8601 UTC timestamp used by every Raw audit envelope."""
    return datetime.now(UTC).isoformat()


def _canonical_json(value: Any) -> str:
    """Stable JSON encoding suitable for hashing across runs/platforms."""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _normalize_decimal_str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Type {type(value).__name__} is not JSON serialisable")


def _normalize_decimal_str(value: Any) -> str | None:
    """Normalises numeric-like inputs to a stable decimal string.

    Returns ``None`` for ``None``/empty values. Strings/ints/floats accepted.
    """
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return format(Decimal(str(value)), "f")
    except (InvalidOperation, ValueError):
        return str(value)


def _normalize_for_hash(value: Any) -> Any:
    """Recursively normalises a payload into JSON-canonical primitives.

    * ``Decimal`` and floats are converted to deterministic strings.
    * ``dict`` and ``list`` are recursed.
    * ``None``/``str``/``int``/``bool`` pass through.
    """
    if isinstance(value, Decimal):
        return _normalize_decimal_str(value)
    if isinstance(value, float):
        return _normalize_decimal_str(value)
    if isinstance(value, Mapping):
        return {k: _normalize_for_hash(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_normalize_for_hash(v) for v in value]
    return value


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compute_payload_hash(payload: Any) -> str:
    """SHA-256 over canonical JSON of ``payload`` (excluding any ``_audit``).

    Audit fields are stripped before hashing so the digest reflects the
    upstream API content, not our own envelope.
    """
    snapshot = _strip_audit(payload)
    canonical = _canonical_json(_normalize_for_hash(snapshot))
    return _sha256_hex(canonical)


def _strip_audit(payload: Any) -> Any:
    """Returns a shallow copy of ``payload`` without ``_audit``/``_record_*``.

    Items inside ``dados`` are also stripped of ``_record_uid``/``_record_hash``
    so payload-level hashes ignore our own enrichment.
    """
    if not isinstance(payload, Mapping):
        return payload
    cleaned = {k: v for k, v in payload.items() if k != AUDIT_KEY}
    if isinstance(cleaned.get("dados"), list):
        cleaned["dados"] = [
            {
                k: v
                for k, v in item.items()
                if k not in (RECORD_UID_KEY, RECORD_HASH_KEY)
            }
            if isinstance(item, Mapping)
            else item
            for item in cleaned["dados"]
        ]
    return cleaned


def compute_record_uid(
    *,
    source: str,
    entity: str,
    business_keys: Mapping[str, Any],
) -> str:
    """Stable ``_record_uid`` for a record using business keys only.

    The digest is ``sha256(source|entity|<canonical-json(business_keys)>)``.
    No random UUID is used. ``None`` values are preserved in the canonical
    representation so the hash distinguishes "key not present" from
    "key present with empty value".
    """
    canonical = _canonical_json(_normalize_for_hash(dict(business_keys)))
    raw = f"{source}|{entity}|{canonical}"
    return _sha256_hex(raw)


def compute_record_hash(record: Mapping[str, Any]) -> str:
    """SHA-256 over canonical JSON of a single record (without audit keys).

    Differs from :func:`compute_record_uid` by including *all* fields present
    in the record (not only business keys). Useful to detect any change in
    upstream content for the same logical record.
    """
    if not isinstance(record, Mapping):
        return _sha256_hex(_canonical_json(record))
    cleaned = {
        k: v
        for k, v in record.items()
        if k not in (RECORD_UID_KEY, RECORD_HASH_KEY)
    }
    canonical = _canonical_json(_normalize_for_hash(cleaned))
    return _sha256_hex(canonical)


def _ceap_business_keys(
    item: Mapping[str, Any], *, id_deputado: int, ano: int, mes: int
) -> dict[str, Any]:
    """Builds the deterministic business-keys dict for a CEAP despesa item."""
    keys: dict[str, Any] = {
        "id_deputado": int(id_deputado),
        "ano": int(ano),
        "mes": int(mes),
    }
    for field in CEAP_RECORD_UID_FIELDS:
        value = item.get(field) if isinstance(item, Mapping) else None
        if field == "valorDocumento":
            keys[field] = _normalize_decimal_str(value)
        else:
            keys[field] = value
    return keys


def build_ceap_record_uid(
    item: Mapping[str, Any], *, id_deputado: int, ano: int, mes: int
) -> str:
    """Returns ``_record_uid`` for a CEAP despesa item (stable across replays)."""
    return compute_record_uid(
        source=CEAP_SOURCE_SYSTEM,
        entity=CEAP_ENTITY,
        business_keys=_ceap_business_keys(
            item, id_deputado=id_deputado, ano=ano, mes=mes
        ),
    )


def build_deputy_record_uid(deputy: Mapping[str, Any]) -> str | None:
    """Returns ``_record_uid`` for a /deputados list item (or ``None`` if no id).

    Uses the API-provided ``id`` (deputado technical id) as the only business
    key. Replay-safe.
    """
    if not isinstance(deputy, Mapping):
        return None
    raw_id = deputy.get("id")
    if raw_id is None:
        return None
    try:
        deputy_id = int(raw_id)
    except (TypeError, ValueError):
        return None
    return compute_record_uid(
        source=CEAP_SOURCE_SYSTEM,
        entity=DEPUTIES_ENTITY,
        business_keys={"id": deputy_id},
    )


def build_ceap_audit_block(
    *,
    pipeline_run_id: str,
    execution_id: str,
    id_deputado: int,
    ano: int,
    mes: int,
    page: int,
    raw_path: str,
    payload_hash: str,
    ingested_at_utc: str | None = None,
    api_base_url: str = CEAP_API_BASE_URL,
    api_path: str = CEAP_DESPESAS_API_PATH,
) -> dict[str, Any]:
    """Standard audit envelope embedded at top-level of CEAP page JSON files."""
    ingested = ingested_at_utc or now_utc_iso()
    return {
        "_metadata_version": "1.0",
        "_pipeline_run_id": pipeline_run_id,
        "_execution_id": execution_id,
        "_source_system": CEAP_SOURCE_SYSTEM,
        "_source_endpoint": CEAP_ENTITY,
        "_api_base_url": api_base_url,
        "_api_path": api_path,
        "_entity": CEAP_ENTITY,
        "_domain": CEAP_DOMAIN,
        "_reference_year": int(ano),
        "_reference_month": int(mes),
        "_deputado_id": int(id_deputado),
        "_page": int(page),
        "_raw_path": raw_path,
        "_ingested_at_utc": ingested,
        "_loaded_at": ingested,
        "_payload_hash": payload_hash,
    }


def build_deputies_audit_block(
    *,
    pipeline_run_id: str,
    execution_id: str,
    reference_date: str,
    page: int,
    raw_path: str,
    payload_hash: str,
    ingested_at_utc: str | None = None,
    api_base_url: str = CEAP_API_BASE_URL,
    api_path: str = DEPUTIES_API_PATH,
) -> dict[str, Any]:
    """Audit envelope embedded at top-level of /deputados list page JSON files."""
    ingested = ingested_at_utc or now_utc_iso()
    return {
        "_metadata_version": "1.0",
        "_pipeline_run_id": pipeline_run_id,
        "_execution_id": execution_id,
        "_source_system": CEAP_SOURCE_SYSTEM,
        "_source_endpoint": DEPUTIES_ENTITY,
        "_api_base_url": api_base_url,
        "_api_path": api_path,
        "_entity": DEPUTIES_ENTITY,
        "_domain": DEPUTIES_DOMAIN,
        "_reference_date": reference_date,
        "_page": int(page),
        "_raw_path": raw_path,
        "_ingested_at_utc": ingested,
        "_loaded_at": ingested,
        "_payload_hash": payload_hash,
    }


def enrich_ceap_page_payload(
    payload: Mapping[str, Any],
    *,
    pipeline_run_id: str,
    execution_id: str,
    id_deputado: int,
    ano: int,
    mes: int,
    page: int,
    raw_path: str,
    ingested_at_utc: str | None = None,
) -> dict[str, Any]:
    """Returns a new payload with ``_audit`` and per-item ``_record_uid``/``_record_hash``.

    Side-effect free: the input mapping is *not* mutated. The function is
    deterministic — calling it twice with the same inputs yields identical
    output (timestamps come from ``ingested_at_utc`` when provided).
    """
    base_payload = _strip_audit(payload)
    payload_hash = compute_payload_hash(base_payload)
    enriched: dict[str, Any] = dict(base_payload) if isinstance(base_payload, Mapping) else {}
    audit = build_ceap_audit_block(
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        id_deputado=id_deputado,
        ano=ano,
        mes=mes,
        page=page,
        raw_path=raw_path,
        payload_hash=payload_hash,
        ingested_at_utc=ingested_at_utc,
    )
    new_payload: dict[str, Any] = {AUDIT_KEY: audit}
    new_payload.update(enriched)

    items = new_payload.get("dados")
    if isinstance(items, list):
        new_items: list[Any] = []
        for item in items:
            if isinstance(item, Mapping):
                cleaned = {
                    k: v
                    for k, v in item.items()
                    if k not in (RECORD_UID_KEY, RECORD_HASH_KEY)
                }
                cleaned[RECORD_UID_KEY] = build_ceap_record_uid(
                    cleaned, id_deputado=id_deputado, ano=ano, mes=mes
                )
                cleaned[RECORD_HASH_KEY] = compute_record_hash(cleaned)
                new_items.append(cleaned)
            else:
                new_items.append(item)
        new_payload["dados"] = new_items
    return new_payload


def enrich_deputies_page_payload(
    payload: Mapping[str, Any],
    *,
    pipeline_run_id: str,
    execution_id: str,
    reference_date: str,
    page: int,
    raw_path: str,
    ingested_at_utc: str | None = None,
) -> dict[str, Any]:
    """Returns a new /deputados page payload enriched with audit metadata.

    Each item under ``dados`` (when it has an ``id``) receives a stable
    ``_record_uid`` derived from the technical deputy id. ``_record_hash`` is
    computed over the full item.
    """
    base_payload = _strip_audit(payload)
    payload_hash = compute_payload_hash(base_payload)
    enriched: dict[str, Any] = dict(base_payload) if isinstance(base_payload, Mapping) else {}
    audit = build_deputies_audit_block(
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        reference_date=reference_date,
        page=page,
        raw_path=raw_path,
        payload_hash=payload_hash,
        ingested_at_utc=ingested_at_utc,
    )
    new_payload: dict[str, Any] = {AUDIT_KEY: audit}
    new_payload.update(enriched)

    items = new_payload.get("dados")
    if isinstance(items, list):
        new_items: list[Any] = []
        for item in items:
            if isinstance(item, Mapping):
                cleaned = {
                    k: v
                    for k, v in item.items()
                    if k not in (RECORD_UID_KEY, RECORD_HASH_KEY)
                }
                uid = build_deputy_record_uid(cleaned)
                if uid is not None:
                    cleaned[RECORD_UID_KEY] = uid
                cleaned[RECORD_HASH_KEY] = compute_record_hash(cleaned)
                new_items.append(cleaned)
            else:
                new_items.append(item)
        new_payload["dados"] = new_items
    return new_payload
