"""Tests for the reference snapshot worker logic (paging + manifest + audit)."""

from __future__ import annotations

from typing import Any

from shared.domain_catalog import REFERENCE_DOMAIN
from shared.raw_audit import AUDIT_KEY, RECORD_UID_KEY
from shared.reference_raw_manifest import (
    reference_endpoint_metadata_path,
    reference_endpoint_success_path,
)
from shared.reference_run import run_reference_endpoint_snapshot


def _make_fetcher(pages: list[dict[str, Any]]):
    """Return a fetcher that yields each page in order; appends a 'next' link except on the last."""
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


def test_completed_snapshot_writes_pages_metadata_and_success(raw_writer) -> None:
    domain = REFERENCE_DOMAIN
    endpoint = domain.endpoint("partidos")
    fetcher = _make_fetcher(
        [
            {"dados": [{"id": 1, "nome": "Partido A"}, {"id": 2, "nome": "Partido B"}]},
            {"dados": [{"id": 3, "nome": "Partido C"}]},
        ]
    )

    result = run_reference_endpoint_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-1",
        reference_date="2026-05-11",
        reference_timezone="America/Sao_Paulo",
        started_at_utc="2026-05-11T00:00:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "COMPLETED"
    assert result.pages_written == 2
    assert result.record_count == 3

    # metadata.json + _SUCCESS were written under the manifest folder.
    meta_path = reference_endpoint_metadata_path(
        endpoint, "2026-05-11", "reference_snapshot_20260511"
    )
    success_path = reference_endpoint_success_path(
        endpoint, "2026-05-11", "reference_snapshot_20260511"
    )
    assert raw_writer.path_exists(meta_path)
    assert raw_writer.path_exists(success_path)

    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "COMPLETED"
    assert meta["record_count"] == 3
    assert meta["total_pages"] == 2
    assert meta["files_written"] == 2
    assert meta["hash_strategy"] == "payload_and_record_hash_v1"
    assert "_audit" in meta["audit_fields_applied"]


def test_each_persisted_page_carries_audit_envelope(raw_writer) -> None:
    domain = REFERENCE_DOMAIN
    endpoint = domain.endpoint("legislaturas")
    fetcher = _make_fetcher(
        [{"dados": [{"id": 56, "nomeLegislatura": "56ª"}]}]
    )

    run_reference_endpoint_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-1",
        reference_date="2026-05-11",
        reference_timezone="America/Sao_Paulo",
        started_at_utc="2026-05-11T00:00:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    page_paths = [p for p in raw_writer.json_files if p.endswith("/page_1.json")]
    assert len(page_paths) == 1
    page = raw_writer.read_json(page_paths[0]) or {}
    assert AUDIT_KEY in page
    assert page[AUDIT_KEY]["_pipeline_run_id"] == "reference_snapshot_20260511"
    assert page[AUDIT_KEY]["_reference_date"] == "2026-05-11"
    assert page["dados"][0][RECORD_UID_KEY]
    assert page["dados"][0]["id"] == 56


def test_failed_snapshot_writes_failed_metadata_and_no_success(raw_writer) -> None:
    domain = REFERENCE_DOMAIN
    endpoint = domain.endpoint("orgaos")
    pages_seen: list[int] = []

    def fetcher(page: int) -> tuple[dict[str, Any], int]:
        pages_seen.append(page)
        if page == 1:
            return (
                {
                    "dados": [{"id": 10}],
                    "links": [{"rel": "next", "href": "x"}],
                },
                200,
            )
        raise RuntimeError("simulated upstream failure")

    result = run_reference_endpoint_snapshot(
        domain=domain,
        endpoint=endpoint,
        pipeline_run_id="reference_snapshot_20260511",
        execution_id="exec-1",
        reference_date="2026-05-11",
        reference_timezone="America/Sao_Paulo",
        started_at_utc="2026-05-11T00:00:00+00:00",
        raw_writer=raw_writer,
        page_fetcher=fetcher,
    )

    assert result.final_status == "FAILED"
    assert result.error_type == "RuntimeError"
    assert result.pages_written == 1
    assert result.record_count == 1

    meta_path = reference_endpoint_metadata_path(
        endpoint, "2026-05-11", "reference_snapshot_20260511"
    )
    success_path = reference_endpoint_success_path(
        endpoint, "2026-05-11", "reference_snapshot_20260511"
    )
    assert raw_writer.path_exists(meta_path)
    assert not raw_writer.path_exists(success_path)
    meta = raw_writer.read_json(meta_path) or {}
    assert meta["status"] == "FAILED"
    assert meta["error_type"] == "RuntimeError"
