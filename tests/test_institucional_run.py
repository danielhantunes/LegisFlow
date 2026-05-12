"""Tests for institucional worker logic."""

from __future__ import annotations

from typing import Any

import pytest

from shared.domain_catalog import INSTITUCIONAL_DOMAIN
from shared.institucional_raw_manifest import (
    WORKER_ENDPOINTS,
    institucional_sub_metadata_path,
    institucional_sub_success_path,
    parent_label_for_worker,
)
from shared.institucional_run import run_institucional_sub_snapshot
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


@pytest.mark.parametrize("endpoint_name", list(WORKER_ENDPOINTS))
def test_completed_sub_snapshot_writes_pages_and_success(
    raw_writer, endpoint_name
) -> None:
    domain = INSTITUCIONAL_DOMAIN
    endpoint = domain.endpoint(endpoint_name)

    if endpoint_name in ("orgao_membros",):
        items = [
            {"id": 100, "nome": "Dep A", "dataInicio": "2026-01-01"},
            {"id": 200, "nome": "Dep B", "dataInicio": "2026-01-01"},
        ]
    elif endpoint_name in ("partido_membros", "frente_membros"):
        items = [
            {"id": 100, "nome": "Dep A"},
            {"id": 200, "nome": "Dep B"},
        ]
    elif endpoint_name == "legislatura_lideres":
        items = [
            {
                "parlamentar": {"id": 100, "uri": "https://x"},
                "titulo": "Líder Maioria",
                "dataInicio": "2026-01-01",
            },
            {
                "parlamentar": {"id": 200, "uri": "https://y"},
                "titulo": "Líder Minoria",
                "dataInicio": "2026-01-01",
            },
        ]
    else:  # legislatura_mesa
        items = [
            {"id": 1, "titulo": "Presidente", "dataInicio": "2026-01-01"},
            {"id": 2, "titulo": "1º Vice", "dataInicio": "2026-01-01"},
        ]

    fetcher = _make_fetcher(
        [
            {"dados": items[:1]},
            {"dados": items[1:]},
        ]
    )

    pid = "institucional_daily_20260511"
    result = run_institucional_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        parent_id="42",
        started_at_utc="2026-05-11T06:00:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "COMPLETED"
    assert result.pages_written == 2
    assert result.record_count == 2

    meta_path = institucional_sub_metadata_path(endpoint_name, "42", pid)
    success_path = institucional_sub_success_path(endpoint_name, "42", pid)
    assert raw_writer.path_exists(meta_path)
    assert raw_writer.path_exists(success_path)

    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "COMPLETED"
    assert meta["parent_id"] == "42"
    assert meta["sub_endpoint"] == endpoint_name
    assert meta["parent_label"] == parent_label_for_worker(endpoint_name)
    assert meta["record_count"] == 2
    assert meta["total_pages"] == 2
    assert meta["files_written"] == 2
    assert meta["hash_strategy"] == "payload_and_record_hash_v1"


def test_each_page_carries_audit_envelope_with_parent_label(raw_writer) -> None:
    domain = INSTITUCIONAL_DOMAIN
    endpoint = domain.endpoint("legislatura_lideres")
    fetcher = _make_fetcher(
        [
            {
                "dados": [
                    {
                        "parlamentar": {"id": 100, "uri": "https://x"},
                        "titulo": "Líder Maioria",
                        "dataInicio": "2026-01-01",
                    }
                ]
            }
        ]
    )

    pid = "institucional_daily_20260511"
    run_institucional_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        parent_id="57",
        started_at_utc="2026-05-11T06:00:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    page_paths = [p for p in raw_writer.json_files if p.endswith("/page_1.json")]
    assert len(page_paths) == 1
    page = raw_writer.read_json(page_paths[0]) or {}
    assert AUDIT_KEY in page
    assert page[AUDIT_KEY]["_pipeline_run_id"] == pid
    assert page[AUDIT_KEY]["_parent_id"] == "57"
    assert page[AUDIT_KEY]["_parent_entity"] == "legislatura"
    # Compound nested business key (parlamentar.id, titulo, dataInicio) ⇒ UID present.
    assert page["dados"][0][RECORD_UID_KEY]


def test_failed_sub_snapshot_writes_failed_metadata_and_no_success(
    raw_writer,
) -> None:
    domain = INSTITUCIONAL_DOMAIN
    endpoint = domain.endpoint("partido_membros")

    def fetcher(page: int) -> tuple[dict[str, Any], int]:
        if page == 1:
            return (
                {
                    "dados": [{"id": 1, "nome": "X"}],
                    "links": [{"rel": "next", "href": "x"}],
                },
                200,
            )
        raise RuntimeError("simulated upstream failure")

    pid = "institucional_daily_20260511"
    result = run_institucional_sub_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        parent_id="42",
        started_at_utc="2026-05-11T06:00:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "FAILED"
    assert result.error_type == "RuntimeError"
    assert result.pages_written == 1
    assert result.record_count == 1

    meta_path = institucional_sub_metadata_path("partido_membros", "42", pid)
    success_path = institucional_sub_success_path("partido_membros", "42", pid)
    assert raw_writer.path_exists(meta_path)
    assert not raw_writer.path_exists(success_path)
    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "FAILED"
    assert meta["error_type"] == "RuntimeError"
