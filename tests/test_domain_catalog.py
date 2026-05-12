"""Tests for ``shared.domain_catalog``."""

from __future__ import annotations

import pytest

from shared.domain_catalog import (
    CEAP_DOMAIN,
    REFERENCE_DOMAIN,
    get_domain,
    is_well_formed_pipeline_run_id,
    list_domains,
    reference_run_id_for_date,
)


def test_known_domains_registered() -> None:
    names = {d.name for d in list_domains()}
    assert {"ceap", "reference"}.issubset(names)
    assert get_domain("ceap") is CEAP_DOMAIN
    assert get_domain("reference") is REFERENCE_DOMAIN


def test_get_domain_raises_for_unknown() -> None:
    with pytest.raises(KeyError):
        get_domain("does_not_exist")


def test_reference_endpoints_cover_all_required_apis() -> None:
    expected = {"partidos", "legislaturas", "deputados", "frentes", "orgaos"}
    declared = {ep.name for ep in REFERENCE_DOMAIN.endpoints}
    assert declared == expected


def test_reference_run_id_format() -> None:
    pid = reference_run_id_for_date("2026-05-11")
    assert pid == "reference_snapshot_20260511"
    assert REFERENCE_DOMAIN.is_pipeline_run_id_owned_here(pid)
    assert not CEAP_DOMAIN.is_pipeline_run_id_owned_here(pid)


def test_pipeline_run_id_well_formed_check() -> None:
    assert is_well_formed_pipeline_run_id("reference_snapshot_20260511")
    assert is_well_formed_pipeline_run_id("ceap_daily_20260510")
    assert not is_well_formed_pipeline_run_id("")
    assert not is_well_formed_pipeline_run_id("invalid id with spaces")


def test_each_domain_has_unique_queue_and_partition_keys() -> None:
    queues = [(d.queue_work, d.queue_poison) for d in list_domains()]
    assert len(queues) == len({tuple(q) for q in queues})
    state_keys = [d.state_partition_key for d in list_domains()]
    assert len(state_keys) == len(set(state_keys))
    runs_keys = [d.runs_partition_key for d in list_domains()]
    assert len(runs_keys) == len(set(runs_keys))


def test_reference_domain_advertises_audit_strategy() -> None:
    assert REFERENCE_DOMAIN.hash_strategy == "payload_and_record_hash_v1"
    assert "_audit" in REFERENCE_DOMAIN.audit_fields
    assert "_payload_hash" in REFERENCE_DOMAIN.audit_fields
    assert "_record_uid" in REFERENCE_DOMAIN.audit_fields
    assert "_record_hash" in REFERENCE_DOMAIN.audit_fields
