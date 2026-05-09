"""Helpers for /deputados snapshots in ADLS Raw with completeness markers."""

from __future__ import annotations

import re
from typing import Any

from .adls_writer import AdlsRawWriter

DEPUTIES_LIST_PREFIX = "raw/camara/deputados/api/list"

_PAGE_FILE_RE = re.compile(r"page_(\d+)\.json$", re.IGNORECASE)


def deputies_date_dir(reference_date: str) -> str:
    return f"{DEPUTIES_LIST_PREFIX}/reference_date={reference_date}"


def deputies_metadata_path(reference_date: str) -> str:
    return f"{deputies_date_dir(reference_date)}/metadata.json"


def deputies_manifest_path(reference_date: str) -> str:
    return f"{deputies_date_dir(reference_date)}/_metadata/run_summary.json"


def deputies_success_path(reference_date: str) -> str:
    return f"{deputies_date_dir(reference_date)}/_SUCCESS"


def write_deputies_metadata(
    adls: AdlsRawWriter, reference_date: str, summary: dict[str, Any]
) -> str:
    return adls.write_json(deputies_metadata_path(reference_date), summary)


def write_deputies_manifest(
    adls: AdlsRawWriter, reference_date: str, summary: dict[str, Any]
) -> str:
    return adls.write_json(deputies_manifest_path(reference_date), summary)


def write_deputies_success_marker(adls: AdlsRawWriter, reference_date: str) -> str:
    return adls.write_text(deputies_success_path(reference_date), "")


def read_deputies_manifest(adls: AdlsRawWriter, reference_date: str) -> dict[str, Any] | None:
    return adls.read_json(deputies_manifest_path(reference_date))


def is_snapshot_manifest_valid(
    manifest: dict[str, Any] | None, *, require_success_marker: bool = True
) -> bool:
    if not manifest:
        return False
    status = str(manifest.get("status", "")).upper()
    if status != "COMPLETED":
        return False
    try:
        record_count = int(manifest.get("record_count", 0) or 0)
        total_pages = int(manifest.get("total_pages", 0) or 0)
        files_written = int(manifest.get("files_written", 0) or 0)
    except (TypeError, ValueError):
        return False
    if record_count <= 0 or total_pages <= 0:
        return False
    if files_written != total_pages:
        return False
    if require_success_marker and not str(manifest.get("_success_path", "")).strip():
        # Optional field for diagnostics; actual marker check happens at call sites.
        pass
    return True


def list_snapshot_page_paths(adls: AdlsRawWriter, snapshot_path: str) -> list[str]:
    """Returns full paths to all ``page_*.json`` files under a snapshot date folder.

    Looks one level deep (per ``pipeline_run_id``/``execution_id`` subfolder).
    """
    pages: list[tuple[int, str]] = []
    try:
        runs = adls.list_subdirectories(snapshot_path)
    except Exception:
        runs = []
    for run_dir in runs:
        try:
            executions = adls.list_subdirectories(run_dir)
        except Exception:
            executions = []
        for exec_dir in executions:
            try:
                for entry in adls.fs_client.get_paths(path=exec_dir, recursive=False):
                    if getattr(entry, "is_directory", False):
                        continue
                    name = str(entry.name).rstrip("/").split("/")[-1]
                    m = _PAGE_FILE_RE.match(name)
                    if not m:
                        continue
                    pages.append((int(m.group(1)), str(entry.name)))
            except Exception:
                continue
    pages.sort(key=lambda p: p[0])
    seen: set[int] = set()
    deduped: list[str] = []
    for page_num, path in pages:
        if page_num in seen:
            continue
        seen.add(page_num)
        deduped.append(path)
    return deduped


def load_deputies_from_snapshot(
    adls: AdlsRawWriter, snapshot_path: str
) -> tuple[list[dict[str, Any]], int]:
    """Loads ``dados`` records from every ``page_*.json`` under ``snapshot_path``.

    Returns ``(deputies, pages_read)``.
    """
    deputies: list[dict[str, Any]] = []
    pages_read = 0
    for page_path in list_snapshot_page_paths(adls, snapshot_path):
        payload = adls.read_json(page_path)
        if not payload:
            continue
        pages_read += 1
        for item in payload.get("dados") or []:
            if isinstance(item, dict) and "id" in item:
                deputies.append(item)
    return deputies, pages_read


def find_latest_valid_snapshot(
    adls: AdlsRawWriter, *, before_reference_date: str | None = None
) -> dict[str, Any] | None:
    """Returns latest valid deputies snapshot by Raw manifest + _SUCCESS marker.

    Returns ``{"reference_date", "path", "manifest"}``.
    """
    subdirs = adls.list_subdirectories(DEPUTIES_LIST_PREFIX)
    candidates: list[tuple[str, str]] = []
    for raw in subdirs:
        name = raw.rstrip("/").split("/")[-1]
        if not name.startswith("reference_date="):
            continue
        dt = name.split("=", 1)[1]
        candidates.append((dt, raw))
    candidates.sort(reverse=True)
    for dt, path in candidates:
        if before_reference_date and dt >= before_reference_date:
            continue
        if not adls.path_exists(f"{path}/_SUCCESS"):
            continue
        manifest = adls.read_json(f"{path}/_metadata/run_summary.json") or {}
        if not is_snapshot_manifest_valid(manifest):
            continue
        return {"reference_date": dt, "path": path, "manifest": manifest}
    return None
