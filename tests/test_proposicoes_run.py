"""Tests for proposicoes worker logic (paging + manifest + audit)."""

from __future__ import annotations

from typing import Any

import pytest

from shared.domain_catalog import PROPOSICOES_DOMAIN
from shared.proposicoes_raw_manifest import (
    proposicao_sub_metadata_path,
    proposicao_sub_success_path,
)
from shared.proposicoes_run import run_proposicao_sub_snapshot
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


@pytest.mark.parametrize(
    "endpoint_name",
    ["proposicao_autores", "proposicao_tramitacoes"],
)
def test_completed_sub_snapshot_writes_pages_and_success(
    raw_writer, endpoint_name
) -> None:
    domain = PROPOSICOES_DOMAIN
    endpoint = domain.endpoint(endpoint_name)
    if endpoint_name == "proposicao_autores":
        items = [
            {"uri": "https://camara/api/deputados/1", "nome": "Dep A", "proponente": "S"},
            {"uri": "https://camara/api/deputados/2", "nome": "Dep B", "proponente": "S"},
        ]
    else:
        items = [
            {"sequencia": 1, "dataHora": "2026-05-10T09:00", "descricaoSituacao": "Inicial"},
            {"sequencia": 2, "dataHora": "2026-05-10T10:00", "descricaoSituacao": "Pareceres"},
        ]
    fetcher = _make_fetcher(
        [
            {"dados": items[:1]},
            {"dados": items[1:]},
        ]
    )

    pid = "proposicoes_microbatch_202605112230"
    result = run_proposicao_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        proposicao_id="42",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "COMPLETED"
    assert result.pages_written == 2
    assert result.record_count == 2

    meta_path = proposicao_sub_metadata_path(endpoint_name, "42", pid)
    success_path = proposicao_sub_success_path(endpoint_name, "42", pid)
    assert raw_writer.path_exists(meta_path)
    assert raw_writer.path_exists(success_path)

    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "COMPLETED"
    assert meta["proposicao_id"] == "42"
    assert meta["sub_endpoint"] == endpoint_name
    assert meta["record_count"] == 2
    assert meta["total_pages"] == 2
    assert meta["files_written"] == 2
    assert meta["hash_strategy"] == "payload_and_record_hash_v1"


def test_each_page_carries_audit_envelope_with_parent_id(raw_writer) -> None:
    domain = PROPOSICOES_DOMAIN
    endpoint = domain.endpoint("proposicao_tramitacoes")
    fetcher = _make_fetcher(
        [
            {
                "dados": [
                    {
                        "sequencia": 1,
                        "dataHora": "2026-05-10T09:00",
                        "descricaoSituacao": "X",
                    }
                ]
            }
        ]
    )

    pid = "proposicoes_microbatch_202605112230"
    run_proposicao_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        proposicao_id="42",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    page_paths = [p for p in raw_writer.json_files if p.endswith("/page_1.json")]
    assert len(page_paths) == 1
    page = raw_writer.read_json(page_paths[0]) or {}
    assert AUDIT_KEY in page
    assert page[AUDIT_KEY]["_pipeline_run_id"] == pid
    assert page[AUDIT_KEY]["_parent_id"] == "42"
    assert page[AUDIT_KEY]["_parent_entity"] == "proposicao"
    assert page["dados"][0][RECORD_UID_KEY]


def test_failed_sub_snapshot_writes_failed_metadata_and_no_success(
    raw_writer,
) -> None:
    domain = PROPOSICOES_DOMAIN
    endpoint = domain.endpoint("proposicao_autores")

    def fetcher(page: int) -> tuple[dict[str, Any], int]:
        if page == 1:
            return (
                {
                    "dados": [
                        {
                            "uri": "https://camara/api/deputados/1",
                            "nome": "Dep A",
                            "proponente": "S",
                        }
                    ],
                    "links": [{"rel": "next", "href": "x"}],
                },
                200,
            )
        raise RuntimeError("simulated upstream failure")

    pid = "proposicoes_microbatch_202605112230"
    result = run_proposicao_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        proposicao_id="42",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "FAILED"
    assert result.error_type == "RuntimeError"
    assert result.pages_written == 1
    assert result.record_count == 1

    meta_path = proposicao_sub_metadata_path("proposicao_autores", "42", pid)
    success_path = proposicao_sub_success_path("proposicao_autores", "42", pid)
    assert raw_writer.path_exists(meta_path)
    assert not raw_writer.path_exists(success_path)
    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "FAILED"
    assert meta["error_type"] == "RuntimeError"
