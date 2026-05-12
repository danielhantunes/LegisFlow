"""metadata.json + _SUCCESS for reference-domain snapshot endpoints.

Layout per (endpoint, reference_date, pipeline_run_id):

* ``raw/camara/<raw_prefix>/reference_date={ref}/pipeline_run_id={pid}/``
  ``execution_id={eid}/page_*.json`` — actual API pages (enriched).
* ``raw/camara/<raw_prefix>/reference_date={ref}/_metadata/runs/``
  ``pipeline_run_id={pid}/metadata.json`` — manifest (this module).
* Same folder as above + ``_SUCCESS`` — completion marker (only when
  ``status == 'COMPLETED'``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from .domain_catalog import (
    DEFAULT_API_BASE_URL,
    DEFAULT_AUDIT_FIELDS,
    DEFAULT_HASH_STRATEGY,
    DEFAULT_SOURCE_SYSTEM,
    EndpointSpec,
)
from .metadata import (
    PROFILE_DIMENSION_SNAPSHOT,
    RunStatus,
    build_run_metadata,
    validate_completed_metadata,
    write_run_metadata,
    write_success_marker,
)

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter


def reference_endpoint_data_dir(endpoint: EndpointSpec, reference_date: str) -> str:
    return f"raw/camara/{endpoint.raw_prefix}/reference_date={reference_date}"


def reference_endpoint_run_dir(
    endpoint: EndpointSpec, reference_date: str, pipeline_run_id: str
) -> str:
    return (
        f"{reference_endpoint_data_dir(endpoint, reference_date)}/"
        f"pipeline_run_id={pipeline_run_id}"
    )


def reference_endpoint_manifest_prefix(
    endpoint: EndpointSpec, reference_date: str, pipeline_run_id: str
) -> str:
    return (
        f"{reference_endpoint_data_dir(endpoint, reference_date)}/"
        f"_metadata/runs/pipeline_run_id={pipeline_run_id}"
    )


def reference_endpoint_metadata_path(
    endpoint: EndpointSpec, reference_date: str, pipeline_run_id: str
) -> str:
    return f"{reference_endpoint_manifest_prefix(endpoint, reference_date, pipeline_run_id)}/metadata.json"


def reference_endpoint_success_path(
    endpoint: EndpointSpec, reference_date: str, pipeline_run_id: str
) -> str:
    return f"{reference_endpoint_manifest_prefix(endpoint, reference_date, pipeline_run_id)}/_SUCCESS"


def build_reference_endpoint_run_metadata(
    *,
    endpoint: EndpointSpec,
    domain_name: str,
    pipeline_run_id: str,
    execution_id: str,
    reference_date: str,
    reference_timezone: str,
    status: str,
    started_at_utc: str,
    completed_at_utc: str | None,
    failed_at_utc: str | None,
    total_pages: int,
    record_count: int,
    error_type: str | None = None,
    error_message: str | None = None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    source_system: str = DEFAULT_SOURCE_SYSTEM,
    hash_strategy: str = DEFAULT_HASH_STRATEGY,
    audit_fields_applied: tuple[str, ...] = DEFAULT_AUDIT_FIELDS,
) -> dict[str, Any]:
    st_upper = str(status).upper()
    normalized = cast(
        RunStatus,
        st_upper
        if st_upper
        in {
            "STARTED",
            "RUNNING",
            "COMPLETED",
            "PARTIAL",
            "FAILED",
            "PARTIALLY_COMPLETED",
        }
        else "RUNNING",
    )
    raw_dir = reference_endpoint_run_dir(endpoint, reference_date, pipeline_run_id)
    success_path = reference_endpoint_success_path(
        endpoint, reference_date, pipeline_run_id
    )
    meta = build_run_metadata(
        source=source_system,
        domain=domain_name,
        entity=endpoint.name,
        endpoint=endpoint.name,
        api_base_url=api_base_url,
        api_path=endpoint.path_template,
        pipeline_run_id=pipeline_run_id,
        execution_id=execution_id,
        run_type="snapshot",
        status=normalized,
        started_at=started_at_utc,
        completed_at=completed_at_utc,
        raw_path=raw_dir,
        success_marker_path=success_path,
        total_pages=int(total_pages or 0),
        items_per_page=int(endpoint.items_per_page or 0),
        files_written=int(total_pages or 0),
        record_count=int(record_count or 0),
        error_message=error_message,
        partitioning={
            "reference_date": reference_date,
            "reference_timezone": reference_timezone,
        },
        snapshot={
            "snapshot_type": "dimension",
            "snapshot_date": reference_date,
            "snapshot_status": normalized,
            "snapshot_record_count": int(record_count or 0),
        },
        hash_strategy=hash_strategy,
        audit_fields_applied=audit_fields_applied,
    )
    meta["started_at_utc"] = started_at_utc
    meta["finished_at_utc"] = completed_at_utc
    meta["failed_at_utc"] = failed_at_utc
    meta["pipeline_run_id"] = pipeline_run_id
    meta["status"] = st_upper
    if error_type:
        meta["error_type"] = error_type
    elif "error_type" in meta:
        del meta["error_type"]
    return meta


def persist_reference_endpoint_metadata(
    adls: AdlsRawWriter,
    endpoint: EndpointSpec,
    reference_date: str,
    pipeline_run_id: str,
    metadata: dict[str, Any],
    *,
    write_success_marker_now: bool,
) -> tuple[str, bool]:
    metadata_path = reference_endpoint_metadata_path(
        endpoint, reference_date, pipeline_run_id
    )
    success_path = reference_endpoint_success_path(
        endpoint, reference_date, pipeline_run_id
    )
    write_run_metadata(adls, metadata_path, metadata)
    success_written = False
    if write_success_marker_now:
        success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


def is_reference_endpoint_manifest_valid(
    adls: AdlsRawWriter,
    endpoint: EndpointSpec,
    reference_date: str,
    pipeline_run_id: str,
) -> tuple[bool, dict[str, Any]]:
    metadata_path = reference_endpoint_metadata_path(
        endpoint, reference_date, pipeline_run_id
    )
    meta = adls.read_json(metadata_path) or {}
    success_path = reference_endpoint_success_path(
        endpoint, reference_date, pipeline_run_id
    )
    ok, _reason = validate_completed_metadata(
        adls,
        meta,
        success_path,
        profile=PROFILE_DIMENSION_SNAPSHOT,
    )
    return ok, meta


def now_iso_utc() -> str:
    return datetime.now(UTC).isoformat()
