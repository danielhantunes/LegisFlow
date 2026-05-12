"""Pure logic for institucional work units.

Handles one (sub_endpoint, parent_id) pair per call. The dispatcher does the
parent listings (orgaos/partidos/frentes/legislaturas) directly so it can fan
out work messages; this module is invoked by the worker only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable, Mapping

from .domain_catalog import DomainSpec, EndpointSpec
from .institucional_raw_manifest import (
    build_institucional_sub_metadata,
    institucional_sub_data_dir,
    parent_label_for_worker,
    persist_institucional_sub_metadata,
)
from .raw_audit import enrich_generic_page_payload

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter


@dataclass
class InstitucionalSubWorkResult:
    endpoint: str
    parent_id: str
    pipeline_run_id: str
    pages_written: int
    record_count: int
    raw_dir: str
    last_raw_path: str
    final_status: str
    error_type: str | None = None
    error_message: str | None = None


def _has_next_link(payload: Mapping[str, Any]) -> bool:
    links = payload.get("links") or []
    return any(
        isinstance(link, Mapping) and link.get("rel") == "next" for link in links
    )


PageFetcher = Callable[[int], tuple[dict[str, Any], int]]


def run_institucional_sub_snapshot(
    *,
    domain: DomainSpec,
    endpoint: EndpointSpec,
    pipeline_run_id: str,
    execution_id: str,
    parent_id: str,
    started_at_utc: str,
    raw_writer: AdlsRawWriter,
    page_fetcher: PageFetcher,
    max_pages: int = 10_000,
) -> InstitucionalSubWorkResult:
    raw_dir = institucional_sub_data_dir(endpoint.name, parent_id, pipeline_run_id)
    parent_label = parent_label_for_worker(endpoint.name)
    page = 1
    pages_written = 0
    record_count = 0
    last_path = ""

    running_meta = build_institucional_sub_metadata(
        endpoint=endpoint,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        parent_id=parent_id,
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
    persist_institucional_sub_metadata(
        raw_writer,
        endpoint.name,
        parent_id,
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
                api_path=endpoint.path_template.format(id=parent_id),
                raw_path=raw_path,
                page=page,
                business_key_fields=endpoint.business_key_fields or ("id",),
                source_system=domain.source_system,
                api_base_url=domain.api_base_url,
                parent_id=str(parent_id),
                parent_entity=parent_label,
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
        failed_meta = build_institucional_sub_metadata(
            endpoint=endpoint,
            pipeline_run_id=pipeline_run_id,
            execution_id=execution_id,
            parent_id=parent_id,
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
        persist_institucional_sub_metadata(
            raw_writer,
            endpoint.name,
            parent_id,
            pipeline_run_id,
            failed_meta,
            write_success_marker_now=False,
        )
        return InstitucionalSubWorkResult(
            endpoint=endpoint.name,
            parent_id=parent_id,
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
    completed_meta = build_institucional_sub_metadata(
        endpoint=endpoint,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        parent_id=parent_id,
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
    persist_institucional_sub_metadata(
        raw_writer,
        endpoint.name,
        parent_id,
        pipeline_run_id,
        completed_meta,
        write_success_marker_now=True,
    )

    return InstitucionalSubWorkResult(
        endpoint=endpoint.name,
        parent_id=parent_id,
        pipeline_run_id=pipeline_run_id,
        pages_written=pages_written,
        record_count=record_count,
        raw_dir=raw_dir,
        last_raw_path=last_path,
        final_status="COMPLETED",
    )
