"""Pure logic for eventos work units (one sub-endpoint per call).

The dispatcher lists ``/eventos`` and persists the listing pages itself. For
each evento_id detected, it enqueues 4 work messages (one per sub-endpoint:
deputados, orgaos, pauta, votacoes). The worker delegates to
:func:`run_evento_sub_snapshot` here.

Layout per (endpoint_name, evento_id, pipeline_run_id):

* Pages: ``raw/camara/eventos/api/{deputados|orgaos|pauta|votacoes}/``
  ``evento_id={eid}/pipeline_run_id={pid}/execution_id={exec}/page_*.json``
* Manifest: ``…/_metadata/runs/pipeline_run_id={pid}/metadata.json``
* Marker:   ``…/_metadata/runs/pipeline_run_id={pid}/_SUCCESS``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable, Mapping

from .domain_catalog import DomainSpec, EndpointSpec
from .eventos_raw_manifest import (
    build_evento_sub_metadata,
    evento_sub_data_dir,
    persist_evento_sub_metadata,
)
from .raw_audit import enrich_generic_page_payload

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter


@dataclass
class EventoSubWorkResult:
    endpoint: str
    evento_id: str
    pipeline_run_id: str
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


def run_evento_sub_snapshot(
    *,
    domain: DomainSpec,
    endpoint: EndpointSpec,
    pipeline_run_id: str,
    execution_id: str,
    evento_id: str,
    started_at_utc: str,
    raw_writer: AdlsRawWriter,
    page_fetcher: PageFetcher,
    max_pages: int = 10_000,
) -> EventoSubWorkResult:
    """Paginate one sub-endpoint of one evento and persist Raw pages."""
    raw_dir = evento_sub_data_dir(endpoint.name, evento_id, pipeline_run_id)
    page = 1
    pages_written = 0
    record_count = 0
    last_path = ""

    running_meta = build_evento_sub_metadata(
        endpoint=endpoint,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        evento_id=evento_id,
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
    persist_evento_sub_metadata(
        raw_writer,
        endpoint.name,
        evento_id,
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
                api_path=endpoint.path_template.format(id=evento_id),
                raw_path=raw_path,
                page=page,
                business_key_fields=endpoint.business_key_fields or ("id",),
                source_system=domain.source_system,
                api_base_url=domain.api_base_url,
                parent_id=str(evento_id),
                parent_entity="evento",
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
        failed_meta = build_evento_sub_metadata(
            endpoint=endpoint,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            evento_id=evento_id,
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
        persist_evento_sub_metadata(
            raw_writer,
            endpoint.name,
            evento_id,
            pipeline_run_id,
            failed_meta,
            write_success_marker_now=False,
        )
        return EventoSubWorkResult(
            endpoint=endpoint.name,
            evento_id=evento_id,
            pipeline_run_id=pipeline_run_id,
            pages_written=pages_written,
            record_count=record_count,
            raw_dir=raw_dir,
            last_raw_path=last_path,
            final_status="FAILED",
            error_type=type(exc).__name__,
            error_message=str(exc)[:1024],
        )

    completed_at = datetime.now(UTC).isoformat()
    completed_meta = build_evento_sub_metadata(
        endpoint=endpoint,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        evento_id=evento_id,
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
    persist_evento_sub_metadata(
        raw_writer,
        endpoint.name,
        evento_id,
        pipeline_run_id,
        completed_meta,
        write_success_marker_now=True,
    )

    return EventoSubWorkResult(
        endpoint=endpoint.name,
        evento_id=evento_id,
        pipeline_run_id=pipeline_run_id,
        pages_written=pages_written,
        record_count=record_count,
        raw_dir=raw_dir,
        last_raw_path=last_path,
        final_status="COMPLETED",
    )
