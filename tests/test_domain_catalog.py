"""Tests for ``shared.domain_catalog``."""

from __future__ import annotations

import pytest

from shared.domain_catalog import (
    CEAP_DOMAIN,
    PROPOSICOES_DOMAIN,
    REFERENCE_DOMAIN,
    VOTACOES_DOMAIN,
    get_domain,
    is_well_formed_pipeline_run_id,
    list_domains,
    proposicoes_microbatch_run_id,
    proposicoes_reconciliation_run_id,
    reference_run_id_for_date,
    votacoes_microbatch_run_id,
    votacoes_reconciliation_run_id,
)


def test_known_domains_registered() -> None:
    names = {d.name for d in list_domains()}
    assert {"ceap", "reference", "votacoes", "proposicoes"}.issubset(names)
    assert get_domain("ceap") is CEAP_DOMAIN
    assert get_domain("reference") is REFERENCE_DOMAIN
    assert get_domain("votacoes") is VOTACOES_DOMAIN
    assert get_domain("proposicoes") is PROPOSICOES_DOMAIN


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


def test_votacoes_endpoints_cover_list_and_votos() -> None:
    declared = {ep.name for ep in VOTACOES_DOMAIN.endpoints}
    assert {"votacoes", "votacao_votos"} == declared
    votos = VOTACOES_DOMAIN.endpoint("votacao_votos")
    assert votos.path_template == "/votacoes/{id}/votos"
    assert votos.parent_field == "id"
    # Avoid claiming `idVotacao` is in the per-vote payload (it isn't).
    assert "idVotacao" not in votos.business_key_fields
    assert "deputado_.id" in votos.business_key_fields


def test_votacoes_microbatch_run_id_is_idempotent_per_minute() -> None:
    pid_a = votacoes_microbatch_run_id("2026-05-11T22:30")
    pid_b = votacoes_microbatch_run_id("2026-05-11T22:30")
    assert pid_a == pid_b == "votacoes_microbatch_202605112230"
    assert VOTACOES_DOMAIN.is_pipeline_run_id_owned_here(pid_a)


def test_votacoes_reconciliation_run_id_format() -> None:
    pid = votacoes_reconciliation_run_id("2026-05-11")
    assert pid == "votacoes_reconciliation_20260511"
    assert VOTACOES_DOMAIN.is_pipeline_run_id_owned_here(pid)


def test_votacoes_domain_owns_only_its_prefixes() -> None:
    assert VOTACOES_DOMAIN.is_pipeline_run_id_owned_here(
        "votacoes_microbatch_202605112230"
    )
    assert not VOTACOES_DOMAIN.is_pipeline_run_id_owned_here(
        "reference_snapshot_20260511"
    )
    assert not REFERENCE_DOMAIN.is_pipeline_run_id_owned_here(
        "votacoes_microbatch_202605112230"
    )


def test_proposicoes_endpoints_cover_list_autores_tramitacoes() -> None:
    declared = {ep.name for ep in PROPOSICOES_DOMAIN.endpoints}
    assert {"proposicoes", "proposicao_autores", "proposicao_tramitacoes"} == declared
    autores = PROPOSICOES_DOMAIN.endpoint("proposicao_autores")
    assert autores.path_template == "/proposicoes/{id}/autores"
    assert autores.parent_field == "id"
    tramitacoes = PROPOSICOES_DOMAIN.endpoint("proposicao_tramitacoes")
    assert tramitacoes.path_template == "/proposicoes/{id}/tramitacoes"
    assert tramitacoes.business_key_fields == ("sequencia", "dataHora")


def test_proposicoes_microbatch_run_id_is_idempotent_per_minute() -> None:
    pid_a = proposicoes_microbatch_run_id("2026-05-11T22:30")
    pid_b = proposicoes_microbatch_run_id("2026-05-11T22:30")
    assert pid_a == pid_b == "proposicoes_microbatch_202605112230"
    assert PROPOSICOES_DOMAIN.is_pipeline_run_id_owned_here(pid_a)


def test_proposicoes_reconciliation_run_id_format() -> None:
    pid = proposicoes_reconciliation_run_id("2026-05-11")
    assert pid == "proposicoes_reconciliation_20260511"
    assert PROPOSICOES_DOMAIN.is_pipeline_run_id_owned_here(pid)


def test_proposicoes_domain_has_disjoint_prefixes() -> None:
    assert not PROPOSICOES_DOMAIN.is_pipeline_run_id_owned_here(
        "votacoes_microbatch_202605112230"
    )
    assert not VOTACOES_DOMAIN.is_pipeline_run_id_owned_here(
        "proposicoes_microbatch_202605112230"
    )
    assert not CEAP_DOMAIN.is_pipeline_run_id_owned_here(
        "proposicoes_reconciliation_20260511"
    )
