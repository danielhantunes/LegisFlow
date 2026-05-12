"""Pure logic for reference-domain snapshot work units.

The Azure Function entry points (dispatcher / worker / poison handler) just
wire IO + Table Storage; the business logic — paginate, enrich and persist
each endpoint snapshot — lives here so it is easy to unit test.

A single work message snapshots ONE endpoint for ONE ``reference_date`` under
ONE ``pipeline_run_id``. The dispatcher enqueues N messages (N == number of
endpoints declared for the domain).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable, Mapping

from .domain_catalog import DomainSpec, EndpointSpec
from .raw_audit import enrich_generic_page_payload, now_utc_iso
from .reference_raw_manifest import (
    build_reference_endpoint_run_metadata,
    persist_reference_endpoint_metadata,
    reference_endpoint_run_dir,
)

if TYPE_CHECKING:  # pragma: no cover - only for type checkers
    from .adls_writer import AdlsRawWriter


@dataclass
class ReferenceWorkResult:
    endpoint: str
    pipeline_run_id: str
    reference_date: str
    pages_written: int
    record_count: int
    raw_dir: str
    last_raw_path: str
    final_status: str  # COMPLETED / FAILED
    error_type: str | None = None
    error_message: str | None = None


def _has_next_link(payload: Mapping[str, Any]) -> bool:
    links = payload.get("links") or []
    return any(
        isinstance(link, Mapping) and link.get("rel") == "next" for link in links
    )


PageFetcher = Callable[[int], tuple[dict[str, Any], int]]


def run_reference_endpoint_snapshot(
    *,
    domain: DomainSpec,
    endpoint: EndpointSpec,
    pipeline_run_id: str,
    execution_id: str,
    reference_date: str,
    reference_timezone: str,
    started_at_utc: str,
    raw_writer: AdlsRawWriter,
    page_fetcher: PageFetcher,
    max_pages: int = 10_000,
) -> ReferenceWorkResult:
    """Paginates ``page_fetcher`` until exhaustion, enriches and persists pages.

    * Always overwrites ``metadata.json`` (RUNNING during paging).
    * Writes ``_SUCCESS`` only when the snapshot completes without errors.
    * On failure, ``metadata.json`` is rewritten with status ``FAILED`` and
      no ``_SUCCESS`` is written.
    """
    raw_dir = reference_endpoint_run_dir(endpoint, reference_date, pipeline_run_id)
    page = 1
    pages_written = 0
    record_count = 0
    last_path = ""

    # Persist a RUNNING manifest before any page write so observers can see it.
    running_meta = build_reference_endpoint_run_metadata(
        endpoint=endpoint,
        domain_name=domain.name,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        reference_date=reference_date,
        reference_timezone=reference_timezone,
        status="RUNNING",
        started_at_utc=started_at_utc,
        completed_at_utc=None,
        failed_at_utc=None,
        total_pages=0,
        record_count=0,
        api_base_url=domain.api_base_url,
        source_system=domain.source_system,
        hash_strategy=domain.hash_strategy,
        audit_fields_applied=domain.audit_fields,
    )
    persist_reference_endpoint_metadata(
        raw_writer,
        endpoint,
        reference_date,
        pipeline_run_id,
        running_meta,
        write_success_marker_now=False,
    )

    try:
        while page <= max_pages:
            payload, _http_status = page_fetcher(page)
            dados = payload.get("dados") or []
            raw_path = (
                f"{raw_dir}/execution_id={execution_id}/page_{page}.json"
            )
            enriched = enrich_generic_page_payload(
                payload,
                pipeline_run_id=pipeline_run_id,
                execution_id=execution_id,
                domain=domain.name,
                entity=endpoint.name,
                endpoint=endpoint.name,
                api_path=endpoint.path_template,
                raw_path=raw_path,
                page=page,
                business_key_fields=endpoint.business_key_fields or ("id",),
                source_system=domain.source_system,
                api_base_url=domain.api_base_url,
                reference_date=reference_date,
            )
            raw_writer.write_json(raw_path, enriched)
            last_path = raw_path
            pages_written += 1
            record_count += len(dados)
            if not _has_next_link(payload):
                break
            page += 1
    except Exception as exc:
        failed_at = datetime.now(UTC).isoformat()
        failed_meta = build_reference_endpoint_run_metadata(
            endpoint=endpoint,
            domain_name=domain.name,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            reference_date=reference_date,
            reference_timezone=reference_timezone,
            status="FAILED",
            started_at_utc=started_at_utc,
            completed_at_utc=None,
            failed_at_utc=failed_at,
            total_pages=pages_written,
            record_count=record_count,
            error_type=type(exc).__name__,
            error_message=str(exc)[:1024],
            api_base_url=domain.api_base_url,
            source_system=domain.source_system,
            hash_strategy=domain.hash_strategy,
            audit_fields_applied=domain.audit_fields,
        )
        persist_reference_endpoint_metadata(
            raw_writer,
            endpoint,
            reference_date,
            pipeline_run_id,
            failed_meta,
            write_success_marker_now=False,
        )
        return ReferenceWorkResult(
            endpoint=endpoint.name,
            pipeline_run_id=pipeline_run_id,
            reference_date=reference_date,
            pages_written=pages_written,
            record_count=record_count,
            raw_dir=raw_dir,
            last_raw_path=last_path,
            final_status="FAILED",
            error_type=type(exc).__name__,
            error_message=str(exc)[:1024],
        )

    completed_at = datetime.now(UTC).isoformat()
    completed_meta = build_reference_endpoint_run_metadata(
        endpoint=endpoint,
        domain_name=domain.name,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        reference_date=reference_date,
        reference_timezone=reference_timezone,
        status="COMPLETED",
        started_at_utc=started_at_utc,
        completed_at_utc=completed_at,
        failed_at_utc=None,
        total_pages=pages_written,
        record_count=record_count,
        api_base_url=domain.api_base_url,
        source_system=domain.source_system,
        hash_strategy=domain.hash_strategy,
        audit_fields_applied=domain.audit_fields,
    )
    persist_reference_endpoint_metadata(
        raw_writer,
        endpoint,
        reference_date,
        pipeline_run_id,
        completed_meta,
        write_success_marker_now=True,
    )

    return ReferenceWorkResult(
        endpoint=endpoint.name,
        pipeline_run_id=pipeline_run_id,
        reference_date=reference_date,
        pages_written=pages_written,
        record_count=record_count,
        raw_dir=raw_dir,
        last_raw_path=last_path,
        final_status="COMPLETED",
    )


def now_utc_iso_str() -> str:
    """Re-exported for callers that don't need raw_audit."""
    return now_utc_iso()
