"""Tests for votacoes worker logic (paging + manifest + audit)."""

from __future__ import annotations

from typing import Any

from shared.domain_catalog import VOTACOES_DOMAIN
from shared.raw_audit import AUDIT_KEY, RECORD_UID_KEY
from shared.votacoes_raw_manifest import (
    votacao_votos_metadata_path,
    votacao_votos_success_path,
)
from shared.votacoes_run import run_votacao_votos_snapshot


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


def test_completed_votos_writes_pages_metadata_and_success(raw_writer) -> None:
    domain = VOTACOES_DOMAIN
    endpoint = domain.endpoint("votacao_votos")
    fetcher = _make_fetcher(
        [
            {
                "dados": [
                    {
                        "tipoVoto": "Sim",
                        "deputado_": {"id": 100, "nome": "A"},
                    },
                    {
                        "tipoVoto": "Não",
                        "deputado_": {"id": 200, "nome": "B"},
                    },
                ]
            },
            {
                "dados": [
                    {
                        "tipoVoto": "Abstenção",
                        "deputado_": {"id": 300, "nome": "C"},
                    },
                ]
            },
        ]
    )

    pid = "votacoes_microbatch_202605112230"
    result = run_votacao_votos_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        votacao_id="9999",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "COMPLETED"
    assert result.pages_written == 2
    assert result.record_count == 3

    meta_path = votacao_votos_metadata_path("9999", pid)
    success_path = votacao_votos_success_path("9999", pid)
    assert raw_writer.path_exists(meta_path)
    assert raw_writer.path_exists(success_path)

    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "COMPLETED"
    assert meta["votacao_id"] == "9999"
    assert meta["record_count"] == 3
    assert meta["total_pages"] == 2
    assert meta["files_written"] == 2
    assert meta["hash_strategy"] == "payload_and_record_hash_v1"
    assert "_audit" in meta["audit_fields_applied"]


def test_each_persisted_page_carries_audit_envelope_and_parent_id(raw_writer) -> None:
    domain = VOTACOES_DOMAIN
    endpoint = domain.endpoint("votacao_votos")
    fetcher = _make_fetcher(
        [
            {
                "dados": [
                    {
                        "tipoVoto": "Sim",
                        "deputado_": {"id": 100, "nome": "A"},
                    }
                ]
            }
        ]
    )

    pid = "votacoes_microbatch_202605112230"
    run_votacao_votos_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        votacao_id="9999",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    page_paths = [p for p in raw_writer.json_files if p.endswith("/page_1.json")]
    assert len(page_paths) == 1
    page = raw_writer.read_json(page_paths[0]) or {}
    assert AUDIT_KEY in page
    assert page[AUDIT_KEY]["_pipeline_run_id"] == pid
    assert page[AUDIT_KEY]["_parent_id"] == "9999"
    # Nested business key (`deputado_.id`) must be resolved into a UID.
    assert page["dados"][0][RECORD_UID_KEY]


def test_failed_votos_writes_failed_metadata_and_no_success(raw_writer) -> None:
    domain = VOTACOES_DOMAIN
    endpoint = domain.endpoint("votacao_votos")

    def fetcher(page: int) -> tuple[dict[str, Any], int]:
        if page == 1:
            return (
                {
                    "dados": [
                        {
                            "tipoVoto": "Sim",
                            "deputado_": {"id": 100, "nome": "A"},
                        }
                    ],
                    "links": [{"rel": "next", "href": "x"}],
                },
                200,
            )
        raise RuntimeError("simulated upstream failure")

    pid = "votacoes_microbatch_202605112230"
    result = run_votacao_votos_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        votacao_id="9999",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "FAILED"
    assert result.error_type == "RuntimeError"
    assert result.pages_written == 1
    assert result.record_count == 1

    meta_path = votacao_votos_metadata_path("9999", pid)
    success_path = votacao_votos_success_path("9999", pid)
    assert raw_writer.path_exists(meta_path)
    assert not raw_writer.path_exists(success_path)
    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "FAILED"
    assert meta["error_type"] == "RuntimeError"
