"""Tests for ``shared.raw_audit`` — deterministic hashes + Raw enrichment."""

from __future__ import annotations

from shared.raw_audit import (
    AUDIT_KEY,
    CEAP_API_BASE_URL,
    CEAP_DOMAIN,
    CEAP_ENTITY,
    CEAP_SOURCE_SYSTEM,
    DEPUTIES_API_PATH,
    DEPUTIES_DOMAIN,
    DEPUTIES_ENTITY,
    RECORD_HASH_KEY,
    RECORD_UID_KEY,
    build_ceap_record_uid,
    build_deputy_record_uid,
    compute_payload_hash,
    compute_record_hash,
    compute_record_uid,
    enrich_ceap_page_payload,
    enrich_deputies_page_payload,
)


def test_compute_payload_hash_is_deterministic_and_key_order_invariant() -> None:
    a = {"foo": 1, "bar": [1, 2, 3], "dados": [{"x": "y"}]}
    b = {"dados": [{"x": "y"}], "bar": [1, 2, 3], "foo": 1}
    assert compute_payload_hash(a) == compute_payload_hash(b)


def test_compute_payload_hash_changes_on_content_change() -> None:
    base = {"dados": [{"x": "y"}]}
    other = {"dados": [{"x": "z"}]}
    assert compute_payload_hash(base) != compute_payload_hash(other)


def test_compute_payload_hash_ignores_audit_envelope() -> None:
    raw = {"dados": [{"id": 1}], "links": []}
    enriched = {**raw, AUDIT_KEY: {"_pipeline_run_id": "x"}}
    assert compute_payload_hash(raw) == compute_payload_hash(enriched)


def test_compute_payload_hash_ignores_per_item_audit_keys() -> None:
    raw = {"dados": [{"id": 1, "name": "A"}]}
    enriched_item = {
        "dados": [
            {"id": 1, "name": "A", RECORD_UID_KEY: "uid", RECORD_HASH_KEY: "rh"}
        ]
    }
    assert compute_payload_hash(raw) == compute_payload_hash(enriched_item)


def test_compute_record_uid_deterministic_for_same_inputs() -> None:
    keys = {"a": 1, "b": "x", "c": None}
    uid_a = compute_record_uid(source="s", entity="e", business_keys=keys)
    uid_b = compute_record_uid(source="s", entity="e", business_keys=keys)
    assert uid_a == uid_b


def test_compute_record_uid_changes_with_source_or_entity() -> None:
    keys = {"a": 1}
    uid_default = compute_record_uid(source="s", entity="e", business_keys=keys)
    uid_other_source = compute_record_uid(
        source="s2", entity="e", business_keys=keys
    )
    uid_other_entity = compute_record_uid(
        source="s", entity="e2", business_keys=keys
    )
    assert uid_default != uid_other_source
    assert uid_default != uid_other_entity


def test_compute_record_uid_distinguishes_none_from_missing_via_canonical_form() -> None:
    uid_none = compute_record_uid(
        source="s", entity="e", business_keys={"a": 1, "b": None}
    )
    uid_missing = compute_record_uid(
        source="s", entity="e", business_keys={"a": 1}
    )
    assert uid_none != uid_missing


def test_build_ceap_record_uid_is_stable_and_uses_business_keys() -> None:
    item = {
        "codDocumento": 7654321,
        "numDocumento": "NF-001",
        "dataDocumento": "2026-04-30",
        "tipoDespesa": "DIVULGAÇÃO",
        "urlDocumento": "https://example/doc.pdf",
        "parcela": 0,
        "numRessarcimento": "",
        "cnpjCpfFornecedor": "12345678000199",
        "valorDocumento": "1234.56",
    }
    uid = build_ceap_record_uid(item, id_deputado=204554, ano=2026, mes=4)
    uid_again = build_ceap_record_uid(item, id_deputado=204554, ano=2026, mes=4)
    assert uid == uid_again

    item_other = dict(item, codDocumento=8888888)
    uid_other = build_ceap_record_uid(
        item_other, id_deputado=204554, ano=2026, mes=4
    )
    assert uid != uid_other


def test_build_ceap_record_uid_normalises_value_decimal() -> None:
    """Same decimal value must produce the same UID regardless of input type.

    Float ``1234.5`` and string ``"1234.5"`` represent the same Decimal and
    therefore must hash identically. Distinct representations (``"1234.50"``
    with trailing zero) intentionally hash differently because the upstream
    explicitly chose that representation — preserving it is part of audit.
    """
    item_float = {"codDocumento": 7654321, "valorDocumento": 1234.5}
    item_str = {"codDocumento": 7654321, "valorDocumento": "1234.5"}
    uid_float = build_ceap_record_uid(
        item_float, id_deputado=1, ano=2026, mes=4
    )
    uid_str = build_ceap_record_uid(
        item_str, id_deputado=1, ano=2026, mes=4
    )
    assert uid_float == uid_str

    item_with_zero = {"codDocumento": 7654321, "valorDocumento": "1234.50"}
    uid_with_zero = build_ceap_record_uid(
        item_with_zero, id_deputado=1, ano=2026, mes=4
    )
    assert uid_with_zero != uid_float


def test_build_deputy_record_uid_uses_id_only() -> None:
    uid = build_deputy_record_uid({"id": 12345, "nome": "FULANO"})
    uid_again = build_deputy_record_uid({"id": 12345, "nome": "OUTRO NOME"})
    assert uid == uid_again


def test_build_deputy_record_uid_returns_none_when_id_missing() -> None:
    assert build_deputy_record_uid({"nome": "X"}) is None
    assert build_deputy_record_uid({"id": "not-int"}) is None


def test_compute_record_hash_changes_when_any_field_changes() -> None:
    base = {"a": 1, "b": "x"}
    h1 = compute_record_hash(base)
    h2 = compute_record_hash({**base, "b": "y"})
    assert h1 != h2


def test_compute_record_hash_ignores_audit_keys() -> None:
    base = {"a": 1, "b": "x"}
    enriched = {**base, RECORD_UID_KEY: "uid", RECORD_HASH_KEY: "rh"}
    assert compute_record_hash(base) == compute_record_hash(enriched)


def test_enrich_ceap_page_payload_adds_audit_and_per_item_uids() -> None:
    raw_payload = {
        "dados": [
            {"codDocumento": 1, "valorDocumento": "10.50"},
            {"codDocumento": 2, "valorDocumento": "20.00"},
        ],
        "links": [],
    }
    fixed_ts = "2026-05-10T12:00:00+00:00"

    enriched = enrich_ceap_page_payload(
        raw_payload,
        pipeline_run_id="ceap_daily_20260510",
        execution_id="exec-1",
        id_deputado=204554,
        ano=2026,
        mes=4,
        page=2,
        raw_path="raw/camara/ceap/api/despesas/.../page_2.json",
        ingested_at_utc=fixed_ts,
    )

    audit = enriched[AUDIT_KEY]
    assert audit["_metadata_version"] == "1.0"
    assert audit["_pipeline_run_id"] == "ceap_daily_20260510"
    assert audit["_execution_id"] == "exec-1"
    assert audit["_source_system"] == CEAP_SOURCE_SYSTEM
    assert audit["_source_endpoint"] == CEAP_ENTITY
    assert audit["_api_base_url"] == CEAP_API_BASE_URL
    assert audit["_entity"] == CEAP_ENTITY
    assert audit["_domain"] == CEAP_DOMAIN
    assert audit["_reference_year"] == 2026
    assert audit["_reference_month"] == 4
    assert audit["_deputado_id"] == 204554
    assert audit["_page"] == 2
    assert audit["_ingested_at_utc"] == fixed_ts
    assert audit["_loaded_at"] == fixed_ts
    assert audit["_payload_hash"] == compute_payload_hash(raw_payload)

    items = enriched["dados"]
    assert all(RECORD_UID_KEY in i and RECORD_HASH_KEY in i for i in items)
    expected_uid_first = build_ceap_record_uid(
        {"codDocumento": 1, "valorDocumento": "10.50"},
        id_deputado=204554,
        ano=2026,
        mes=4,
    )
    assert items[0][RECORD_UID_KEY] == expected_uid_first


def test_enrich_ceap_page_payload_is_deterministic_with_fixed_timestamp() -> None:
    raw_payload = {"dados": [{"codDocumento": 1}]}
    args = dict(
        pipeline_run_id="r",
        execution_id="e",
        id_deputado=1,
        ano=2026,
        mes=4,
        page=1,
        raw_path="raw/x.json",
        ingested_at_utc="2026-05-10T00:00:00+00:00",
    )
    a = enrich_ceap_page_payload(raw_payload, **args)
    b = enrich_ceap_page_payload(raw_payload, **args)
    assert a == b


def test_enrich_ceap_page_payload_does_not_mutate_input() -> None:
    raw_payload = {"dados": [{"codDocumento": 1}]}
    snapshot = {"dados": [{"codDocumento": 1}]}
    enrich_ceap_page_payload(
        raw_payload,
        pipeline_run_id="r",
        execution_id="e",
        id_deputado=1,
        ano=2026,
        mes=4,
        page=1,
        raw_path="raw/x.json",
        ingested_at_utc="2026-05-10T00:00:00+00:00",
    )
    assert raw_payload == snapshot


def test_enrich_deputies_page_payload_adds_audit_and_record_uid_per_item() -> None:
    raw_payload = {
        "dados": [
            {"id": 1, "nome": "A"},
            {"id": 2, "nome": "B"},
        ]
    }
    fixed_ts = "2026-05-10T12:00:00+00:00"
    enriched = enrich_deputies_page_payload(
        raw_payload,
        pipeline_run_id="ceap_daily_20260510",
        execution_id="exec-1",
        reference_date="2026-05-10",
        page=1,
        raw_path="raw/camara/deputados/api/list/reference_date=2026-05-10/.../page_1.json",
        ingested_at_utc=fixed_ts,
    )
    audit = enriched[AUDIT_KEY]
    assert audit["_source_system"] == CEAP_SOURCE_SYSTEM
    assert audit["_source_endpoint"] == DEPUTIES_ENTITY
    assert audit["_entity"] == DEPUTIES_ENTITY
    assert audit["_domain"] == DEPUTIES_DOMAIN
    assert audit["_reference_date"] == "2026-05-10"
    assert audit["_api_path"] == DEPUTIES_API_PATH

    items = enriched["dados"]
    assert items[0][RECORD_UID_KEY] == build_deputy_record_uid({"id": 1})
    assert items[1][RECORD_UID_KEY] == build_deputy_record_uid({"id": 2})
    assert all(RECORD_HASH_KEY in i for i in items)
