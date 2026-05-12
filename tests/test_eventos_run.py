"""Tests for eventos worker logic (paging + manifest + audit)."""

from __future__ import annotations

from typing import Any

import pytest

from shared.domain_catalog import EVENTOS_DOMAIN
from shared.eventos_raw_manifest import (
    EVENTO_SUB_ENDPOINTS,
    evento_sub_metadata_path,
    evento_sub_success_path,
)
from shared.eventos_run import run_evento_sub_snapshot
from shared.raw_audit import AUDIT_KEY, RECORD_UID_KEY


def _make_fetcher(pages: list[dict[str, Any]]):
    enriched_pages: list[dict[str, Any]] = []
    for idx, page in enumerate(pages):
        copy = dict(page)
        if idx < len(pages) - 1:
            copy.setdefault("links", []).append(
                {"rel": "next", "href": "https://example/page?pagina=N"}
            )
        enriched_pages.append(copy)

    def fetcher(page_number: int) -> tuple[dict[str, Any], int]:
        return enriched_pages[page_number - 1], 200

    return fetcher


@pytest.mark.parametrize("endpoint_name", list(EVENTO_SUB_ENDPOINTS))
def test_completed_sub_snapshot_writes_pages_and_success(
    raw_writer, endpoint_name
) -> None:
    domain = EVENTOS_DOMAIN
    endpoint = domain.endpoint(endpoint_name)

    if endpoint_name == "evento_deputados":
        items = [
            {"id": 100, "nome": "Dep A"},
            {"id": 200, "nome": "Dep B"},
        ]
    elif endpoint_name == "evento_orgaos":
        items = [
            {"id": 1, "sigla": "PLEN"},
            {"id": 2, "sigla": "CCJC"},
        ]
    elif endpoint_name == "evento_pauta":
        items = [
            {
                "ordem": 1,
                "proposicao_": {"id": 999, "siglaTipo": "PL"},
                "regime": "Urgência",
            },
            {
                "ordem": 2,
                "proposicao_": {"id": 1000, "siglaTipo": "PL"},
                "regime": "Ordinário",
            },
        ]
    else:  # evento_votacoes
        items = [
            {"id": "55-1", "descricao": "Aprovar"},
            {"id": "55-2", "descricao": "Rejeitar"},
        ]
    fetcher = _make_fetcher(
        [
            {"dados": items[:1]},
            {"dados": items[1:]},
        ]
    )

    pid = "eventos_microbatch_202605112230"
    result = run_evento_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        evento_id="55",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "COMPLETED"
    assert result.pages_written == 2
    assert result.record_count == 2

    meta_path = evento_sub_metadata_path(endpoint_name, "55", pid)
    success_path = evento_sub_success_path(endpoint_name, "55", pid)
    assert raw_writer.path_exists(meta_path)
    assert raw_writer.path_exists(success_path)

    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "COMPLETED"
    assert meta["evento_id"] == "55"
    assert meta["sub_endpoint"] == endpoint_name
    assert meta["record_count"] == 2
    assert meta["total_pages"] == 2
    assert meta["files_written"] == 2
    assert meta["hash_strategy"] == "payload_and_record_hash_v1"


def test_each_page_carries_audit_envelope_with_parent_id(raw_writer) -> None:
    domain = EVENTOS_DOMAIN
    endpoint = domain.endpoint("evento_pauta")
    fetcher = _make_fetcher(
        [
            {
                "dados": [
                    {
                        "ordem": 1,
                        "proposicao_": {"id": 999, "siglaTipo": "PL"},
                        "regime": "Urgência",
                    }
                ]
            }
        ]
    )

    pid = "eventos_microbatch_202605112230"
    run_evento_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        evento_id="55",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    page_paths = [p for p in raw_writer.json_files if p.endswith("/page_1.json")]
    assert len(page_paths) == 1
    page = raw_writer.read_json(page_paths[0]) or {}
    assert AUDIT_KEY in page
    assert page[AUDIT_KEY]["_pipeline_run_id"] == pid
    assert page[AUDIT_KEY]["_parent_id"] == "55"
    assert page[AUDIT_KEY]["_parent_entity"] == "evento"
    # Nested business key (`proposicao_.id`) must produce a UID.
    assert page["dados"][0][RECORD_UID_KEY]


def test_failed_sub_snapshot_writes_failed_metadata_and_no_success(
    raw_writer,
) -> None:
    domain = EVENTOS_DOMAIN
    endpoint = domain.endpoint("evento_orgaos")

    def fetcher(page: int) -> tuple[dict[str, Any], int]:
        if page == 1:
            return (
                {
                    "dados": [{"id": 1, "sigla": "PLEN"}],
                    "links": [{"rel": "next", "href": "x"}],
                },
                200,
            )
        raise RuntimeError("simulated upstream failure")

    pid = "eventos_microbatch_202605112230"
    result = run_evento_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        evento_id="55",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "FAILED"
    assert result.error_type == "RuntimeError"
    assert result.pages_written == 1
    assert result.record_count == 1

    meta_path = evento_sub_metadata_path("evento_orgaos", "55", pid)
    success_path = evento_sub_success_path("evento_orgaos", "55", pid)
    assert raw_writer.path_exists(meta_path)
    assert not raw_writer.path_exists(success_path)
    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "FAILED"
    assert meta["error_type"] == "RuntimeError"
