"""Reusable run-manifest contract for Raw datasets in LegisFlow.

This module centralizes the ``metadata.json`` and ``_SUCCESS`` markers that
every Raw pipeline_run_id folder must produce, regardless of domain
(deputados, ceap, votacoes, eventos, etc.). The Bronze layer is expected to
validate Raw completeness using these files alone, without consulting Azure
Table Storage.

Schema overview (version "1.0"):

* Mandatory base fields are always written (``status`` etc.).
* Optional capability blocks (``partitioning``, ``tasks``, ``fanout``,
  ``snapshot``) are merged in only when the domain provides them.
* ``dependencies`` records other Raw runs/snapshots this run requires.
* ``extras`` is a last-resort escape hatch for domain-specific keys.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, TypedDict

if TYPE_CHECKING:
    from .adls_writer import AdlsRawWriter

METADATA_VERSION = "1.0"

RunStatus = Literal[
    "STARTED",
    "RUNNING",
    "COMPLETED",
    "PARTIAL",
    "FAILED",
    "PARTIALLY_COMPLETED",
]
RunType = Literal[
    "snapshot", "daily", "reconciliation", "backfill", "manual_replay"
]
SnapshotType = Literal["dimension", "fact_window"]


PARTITION_FIELDS: tuple[str, ...] = (
    "reference_date",
    "reference_timezone",
    "reference_year",
    "reference_month",
    "reference_months",
    "target_year",
    "date_start",
    "date_end",
    "watermark_field",
    "watermark_start",
    "watermark_end",
)
TASK_FIELDS: tuple[str, ...] = (
    "total_tasks_expected",
    "total_tasks_queued",
    "total_tasks_success",
    "total_tasks_failed",
    "total_tasks_pending",
    "total_tasks_poison",
    "total_tasks_running",
    "enqueue_phase_complete",
)
FANOUT_FIELDS: tuple[str, ...] = (
    "fanout_from",
    "parent_entity",
    "parent_id_field",
    "parent_record_count",
    "parent_snapshot_path",
    "parent_pipeline_run_id",
)
SNAPSHOT_FIELDS: tuple[str, ...] = (
    "snapshot_type",
    "snapshot_date",
    "snapshot_status",
    "snapshot_record_count",
)


class RunDependency(TypedDict, total=False):
    """One Raw dataset/run this manifest depends on (e.g. a parent snapshot)."""

    entity: str
    pipeline_run_id: str
    path: str
    status: str
    record_count: int
    reference_date: str


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _filter_block(
    block: dict[str, Any] | None, allowed: tuple[str, ...]
) -> dict[str, Any]:
    """Keeps only known keys from a capability block (drops ``None``)."""
    if not block:
        return {}
    return {k: v for k, v in block.items() if k in allowed and v is not None}


def build_run_metadata(
    *,
    source: str,
    domain: str,
    entity: str,
    endpoint: str,
    api_base_url: str,
    api_path: str,
    pipeline_run_id: str,
    run_type: RunType,
    status: RunStatus,
    started_at: str,
    raw_path: str,
    success_marker_path: str,
    total_pages: int = 0,
    items_per_page: int = 0,
    files_written: int = 0,
    record_count: int = 0,
    completed_at: str | None = None,
    created_at: str | None = None,
    error_message: str | None = None,
    execution_id: str | None = None,
    partitioning: dict[str, Any] | None = None,
    tasks: dict[str, Any] | None = None,
    fanout: dict[str, Any] | None = None,
    snapshot: dict[str, Any] | None = None,
    dependencies: list[RunDependency] | None = None,
    hash_strategy: str | None = None,
    audit_fields_applied: tuple[str, ...] | list[str] | None = None,
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Builds a Raw ``metadata.json`` payload following schema v1.0.

    Mandatory keys are always present (use ``0``/``None`` when not applicable).
    Capability blocks (``partitioning``, ``tasks``, ``fanout``, ``snapshot``)
    are filtered to known fields and only merged if non-empty.
    """
    base: dict[str, Any] = {
        "metadata_version": METADATA_VERSION,
        "source": source,
        "domain": domain,
        "entity": entity,
        "endpoint": endpoint,
        "api_base_url": api_base_url,
        "api_path": api_path,
        "pipeline_run_id": pipeline_run_id,
        "execution_id": execution_id,
        "run_type": run_type,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "created_at": created_at or _now_utc_iso(),
        "raw_path": raw_path,
        "success_marker_path": success_marker_path,
        "total_pages": int(total_pages or 0),
        "items_per_page": int(items_per_page or 0),
        "files_written": int(files_written or 0),
        "record_count": int(record_count or 0),
        "error_message": error_message if error_message else None,
    }

    base.update(_filter_block(partitioning, PARTITION_FIELDS))
    base.update(_filter_block(tasks, TASK_FIELDS))
    base.update(_filter_block(fanout, FANOUT_FIELDS))
    base.update(_filter_block(snapshot, SNAPSHOT_FIELDS))

    if dependencies:
        base["dependencies"] = [dict(dep) for dep in dependencies]

    if hash_strategy:
        base["hash_strategy"] = hash_strategy
    if audit_fields_applied:
        base["audit_fields_applied"] = list(audit_fields_applied)

    if extras:
        # Extras are merged last but never override mandatory keys.
        for k, v in extras.items():
            if k in base:
                continue
            base[k] = v

    return base


def write_run_metadata(
    adls: AdlsRawWriter, metadata_path: str, metadata: dict[str, Any]
) -> str:
    """Persists ``metadata.json`` regardless of status (idempotent overwrite)."""
    return adls.write_json(metadata_path, metadata)


def write_success_marker(
    adls: AdlsRawWriter, success_path: str, metadata: dict[str, Any]
) -> bool:
    """Writes ``_SUCCESS`` only when ``metadata.status == 'COMPLETED'``.

    Returns ``True`` when the marker was written.
    """
    if str(metadata.get("status", "")).upper() != "COMPLETED":
        return False
    adls.write_text(success_path, "")
    return True


def persist_run_manifest(
    adls: AdlsRawWriter,
    *,
    metadata_path: str,
    success_path: str,
    metadata: dict[str, Any],
) -> tuple[str, bool]:
    """Convenience: writes ``metadata.json`` and (when COMPLETED) ``_SUCCESS``.

    Returns ``(metadata_path, success_written)``.
    """
    write_run_metadata(adls, metadata_path, metadata)
    success_written = write_success_marker(adls, success_path, metadata)
    return metadata_path, success_written


@dataclass(frozen=True)
class ValidationProfile:
    """Profile-driven completion checks for the Bronze contract.

    All flags are optional add-ons over the base requirement
    ``status == 'COMPLETED'`` and ``_SUCCESS`` present.
    """

    require_record_count_positive: bool = False
    require_total_pages_positive: bool = False
    require_files_written_match_pages: bool = False
    require_tasks_balanced: bool = False


PROFILE_DIMENSION_SNAPSHOT = ValidationProfile(
    require_record_count_positive=True,
    require_total_pages_positive=True,
    require_files_written_match_pages=True,
)
"""For dimension-style snapshots (e.g. /deputados list).

Requires ``record_count > 0``, ``total_pages > 0`` and (when present)
``files_written == total_pages``.
"""

PROFILE_FANOUT_RUN = ValidationProfile(
    require_tasks_balanced=True,
)
"""For fanout/task-driven runs (e.g. CEAP /despesas).

Requires task counters to balance: ``success == expected`` and
``failed == pending == poison == running == 0``.
"""


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def validate_metadata_content(
    metadata: dict[str, Any] | None, *, profile: ValidationProfile
) -> tuple[bool, str]:
    """Content-only validation (does not check ``_SUCCESS``).

    Use when ``_SUCCESS`` presence is verified separately by the caller.
    Returns ``(ok, reason)``; ``reason`` is empty when ok.
    """
    if not metadata:
        return False, "metadata_missing"
    if str(metadata.get("status", "")).upper() != "COMPLETED":
        return False, f"status={metadata.get('status')}"

    if profile.require_record_count_positive and _to_int(metadata.get("record_count")) <= 0:
        return False, "record_count<=0"
    if profile.require_total_pages_positive and _to_int(metadata.get("total_pages")) <= 0:
        return False, "total_pages<=0"
    if profile.require_files_written_match_pages:
        fw = metadata.get("files_written")
        if fw is not None and _to_int(fw) != _to_int(metadata.get("total_pages")):
            return False, "files_written!=total_pages"
    if profile.require_tasks_balanced:
        exp = _to_int(metadata.get("total_tasks_expected"))
        if exp <= 0:
            return False, "total_tasks_expected<=0"
        if _to_int(metadata.get("total_tasks_success")) != exp:
            return False, "success!=expected"
        for k in (
            "total_tasks_failed",
            "total_tasks_pending",
            "total_tasks_poison",
            "total_tasks_running",
        ):
            if _to_int(metadata.get(k)) != 0:
                return False, f"{k}!=0"
    return True, ""


def validate_completed_metadata(
    adls: AdlsRawWriter,
    metadata: dict[str, Any] | None,
    success_path: str,
    *,
    profile: ValidationProfile,
) -> tuple[bool, str]:
    """Full Bronze-contract validation: content + ``_SUCCESS`` presence.

    Returns ``(ok, reason)``; ``reason`` is empty when ok.
    """
    ok, reason = validate_metadata_content(metadata, profile=profile)
    if not ok:
        return False, reason
    if not adls.path_exists(success_path):
        return False, "success_marker_missing"
    return True, ""
