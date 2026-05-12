"""Tests for the generic Raw audit envelope (``enrich_generic_page_payload``)."""

from __future__ import annotations

from shared.raw_audit import (
    AUDIT_KEY,
    RECORD_HASH_KEY,
    RECORD_UID_KEY,
    build_record_uid_from_keys,
    enrich_generic_page_payload,
)


def _sample_payload() -> dict:
    return {
        "dados": [
            {"id": 1, "nome": "Frente A"},
            {"id": 2, "nome": "Frente B"},
        ],
        "links": [{"rel": "self", "href": "https://example/frentes?pagina=1"}],
    }


def test_enrich_generic_page_payload_adds_audit_block() -> None:
    enriched = enrich_generic_page_payload(
        _sample_payload(),
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-123",
        domain="reference",
        entity="frentes",
        endpoint="frentes",
        api_path="/frentes",
        raw_path="raw/camara/frentes/api/list/reference_date=2026-05-11/page_1.json",
        page=1,
        business_key_fields=("id",),
        reference_date="2026-05-11",
    )
    audit = enriched[AUDIT_KEY]
    assert audit["_pipeline_run_id"] == "reference_snapshot_20260511"
    assert audit["_execution_id"] == "exec-123"
    assert audit["_domain"] == "reference"
    assert audit["_entity"] == "frentes"
    assert audit["_endpoint" if "_endpoint" in audit else "_source_endpoint"] == "frentes"
    assert audit["_reference_date"] == "2026-05-11"
    assert isinstance(audit["_payload_hash"], str)
    assert len(audit["_payload_hash"]) == 64


def test_each_record_gets_uid_and_hash() -> None:
    enriched = enrich_generic_page_payload(
        _sample_payload(),
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-123",
        domain="reference",
        entity="frentes",
        endpoint="frentes",
        api_path="/frentes",
        raw_path="raw/camara/frentes/api/list/.../page_1.json",
        page=1,
        business_key_fields=("id",),
        reference_date="2026-05-11",
    )
    items = enriched["dados"]
    assert len(items) == 2
    for item in items:
        assert RECORD_UID_KEY in item
        assert RECORD_HASH_KEY in item
        assert len(item[RECORD_UID_KEY]) == 64
        assert len(item[RECORD_HASH_KEY]) == 64


def test_record_uid_is_deterministic_for_same_business_key() -> None:
    expected = build_record_uid_from_keys(
        source_system="camara_dadosabertos",
        entity="frentes",
        business_keys={"id": 1},
    )
    enriched = enrich_generic_page_payload(
        _sample_payload(),
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-1",
        domain="reference",
        entity="frentes",
        endpoint="frentes",
        api_path="/frentes",
        raw_path="raw/.../page_1.json",
        page=1,
    )
    assert enriched["dados"][0][RECORD_UID_KEY] == expected


def test_payload_hash_is_independent_of_audit_envelope() -> None:
    enriched_a = enrich_generic_page_payload(
        _sample_payload(),
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-A",
        domain="reference",
        entity="frentes",
        endpoint="frentes",
        api_path="/frentes",
        raw_path="raw/.../page_1.json",
        page=1,
    )
    enriched_b = enrich_generic_page_payload(
        _sample_payload(),
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-B",
        domain="reference",
        entity="frentes",
        endpoint="frentes",
        api_path="/frentes",
        raw_path="raw/.../page_1.json",
        page=1,
    )
    assert (
        enriched_a[AUDIT_KEY]["_payload_hash"]
        == enriched_b[AUDIT_KEY]["_payload_hash"]
    )
    assert (
        enriched_a["dados"][0][RECORD_HASH_KEY]
        == enriched_b["dados"][0][RECORD_HASH_KEY]
    )


def test_record_uid_omitted_when_all_business_keys_null() -> None:
    enriched = enrich_generic_page_payload(
        {"dados": [{"nome": "no id here"}]},
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-1",
        domain="reference",
        entity="frentes",
        endpoint="frentes",
        api_path="/frentes",
        raw_path="raw/.../page_1.json",
        page=1,
        business_key_fields=("id",),
    )
    assert RECORD_UID_KEY not in enriched["dados"][0]
    # Hash always present, even without UID.
    assert RECORD_HASH_KEY in enriched["dados"][0]
