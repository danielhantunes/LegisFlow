"""Tests for discursos worker logic."""

from __future__ import annotations

from typing import Any

from shared.discursos_raw_manifest import (
    discursos_detail_metadata_path,
    discursos_detail_success_path,
)
from shared.discursos_run import run_deputado_discursos_snapshot
from shared.domain_catalog import DISCURSOS_DOMAIN
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


def test_completed_discursos_writes_pages_metadata_and_success(
    raw_writer,
) -> None:
    domain = DISCURSOS_DOMAIN
    endpoint = domain.endpoint("deputado_discursos")

    items = [
        {
            "dataHoraInicio": "2026-05-11T22:00:00",
            "faseEvento": {"titulo": "Pequeno Expediente"},
            "tipoDiscurso": "DISCURSO",
            "transcricao": "Texto 1",
        },
        {
            "dataHoraInicio": "2026-05-11T22:15:00",
            "faseEvento": {"titulo": "Pequeno Expediente"},
            "tipoDiscurso": "APARTE",
            "transcricao": "Texto 2",
        },
    ]
    fetcher = _make_fetcher(
        [
            {"dados": items[:1]},
            {"dados": items[1:]},
        ]
    )

    pid = "discursos_microbatch_202605112230"
    result = run_deputado_discursos_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        deputado_id="161550",
        window_start_utc="2026-05-11T21:30:00+00:00",
        window_end_utc="2026-05-11T22:30:00+00:00",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "COMPLETED"
    assert result.pages_written == 2
    assert result.record_count == 2

    meta_path = discursos_detail_metadata_path("161550", pid)
    success_path = discursos_detail_success_path("161550", pid)
    assert raw_writer.path_exists(meta_path)
    assert raw_writer.path_exists(success_path)

    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "COMPLETED"
    assert meta["deputado_id"] == "161550"
    assert meta.get("entity") == "deputado_discursos"
    assert meta.get("endpoint") == "deputado_discursos"
    assert meta["record_count"] == 2
    assert meta["total_pages"] == 2
    assert meta["files_written"] == 2
    assert meta["window_start_utc"] == "2026-05-11T21:30:00+00:00"
    assert meta["window_end_utc"] == "2026-05-11T22:30:00+00:00"
    assert meta["hash_strategy"] == "payload_and_record_hash_v1"


def test_each_page_carries_audit_envelope_with_parent_and_window(
    raw_writer,
) -> None:
    domain = DISCURSOS_DOMAIN
    endpoint = domain.endpoint("deputado_discursos")
    fetcher = _make_fetcher(
        [
            {
                "dados": [
                    {
                        "dataHoraInicio": "2026-05-11T22:00:00",
                        "faseEvento": {"titulo": "Pequeno Expediente"},
                        "tipoDiscurso": "DISCURSO",
                    }
                ]
            }
        ]
    )

    pid = "discursos_microbatch_202605112230"
    run_deputado_discursos_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        deputado_id="161550",
        window_start_utc="2026-05-11T21:30:00+00:00",
        window_end_utc="2026-05-11T22:30:00+00:00",
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    page_paths = [p for p in raw_writer.json_files if p.endswith("/page_1.json")]
    assert len(page_paths) == 1
    page = raw_writer.read_json(page_paths[0]) or {}
    assert AUDIT_KEY in page
    audit = page[AUDIT_KEY]
    assert audit["_pipeline_run_id"] == pid
    assert audit["_parent_id"] == "161550"
    assert audit["_parent_entity"] == "deputado"
    assert audit["_window_start_utc"] == "2026-05-11T21:30:00+00:00"
    assert audit["_window_end_utc"] == "2026-05-11T22:30:00+00:00"
    # Compound business key (dataHoraInicio + faseEvento.titulo + tipoDiscurso)
    # must produce a UID despite the nested faseEvento.
    assert page["dados"][0][RECORD_UID_KEY]


def test_failed_discursos_writes_failed_metadata_and_no_success(
    raw_writer,
) -> None:
    domain = DISCURSOS_DOMAIN
    endpoint = domain.endpoint("deputado_discursos")

    def fetcher(page: int) -> tuple[dict[str, Any], int]:
        if page == 1:
            return (
                {
                    "dados": [
                        {
                            "dataHoraInicio": "2026-05-11T22:00:00",
                            "tipoDiscurso": "DISCURSO",
                        }
                    ],
                    "links": [{"rel": "next", "href": "x"}],
                },
                200,
            )
        raise RuntimeError("simulated upstream failure")

    pid = "discursos_microbatch_202605112230"
    result = run_deputado_discursos_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id=pid,
        execution_id="exec-1",
        deputado_id="161550",
        window_start_utc=None,
        window_end_utc=None,
        started_at_utc="2026-05-11T22:30:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "FAILED"
    assert result.error_type == "RuntimeError"
    assert result.pages_written == 1
    assert result.record_count == 1

    meta_path = discursos_detail_metadata_path("161550", pid)
    success_path = discursos_detail_success_path("161550", pid)
    assert raw_writer.path_exists(meta_path)
    assert not raw_writer.path_exists(success_path)
    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "FAILED"
    assert meta["error_type"] == "RuntimeError"
