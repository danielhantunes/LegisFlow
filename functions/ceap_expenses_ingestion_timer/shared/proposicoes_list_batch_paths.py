"""Partitioned list paths + per-run operation manifest for proposições RAW."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .proposicoes_raw_manifest import PROPOSICOES_LIST_PREFIX


def proposicoes_list_partition_run_dir(
    *,
    run_anchor: datetime,
    pipeline_run_id: str,
) -> str:
    """Hive-style folder for one dispatcher run (list snapshot + operation manifest)."""
    y = run_anchor.year
    m = f"{run_anchor.month:02d}"
    d = f"{run_anchor.day:02d}"
    return f"{PROPOSICOES_LIST_PREFIX}/year={y}/month={m}/day={d}/run_id={pipeline_run_id}"


def list_snapshot_jsonl_path(run_dir: str) -> str:
    return f"{run_dir.rstrip('/')}/list_pages.jsonl"


def list_operation_manifest_path(run_dir: str) -> str:
    return f"{run_dir.rstrip('/')}/manifest.json"


def changed_records_jsonl_path(run_dir: str) -> str:
    return f"{run_dir.rstrip('/')}/changed_records.jsonl"


def build_list_operation_manifest(
    *,
    source: str,
    endpoint: str,
    pipeline_name: str,
    run_id: str,
    window_start: str,
    window_end: str,
    records_seen: int,
    records_written: int,
    records_skipped_same_hash: int,
    records_failed: int,
    status: str,
    raw_path: str,
    started_at: str,
    finished_at: str | None,
    error_summary: str | None = None,
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "source": source,
        "endpoint": endpoint,
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "window_start": window_start,
        "window_end": window_end,
        "records_seen": int(records_seen),
        "records_written": int(records_written),
        "records_skipped_same_hash": int(records_skipped_same_hash),
        "records_failed": int(records_failed),
        "status": status,
        "raw_path": raw_path,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    if error_summary:
        body["error_summary"] = error_summary
    if extras:
        body.update(extras)
    return body


def persist_operation_manifest_json(
    adls: Any,
    path: str,
    manifest: dict[str, Any],
) -> str:
    content = json.dumps(manifest, ensure_ascii=False, indent=2)
    adls.write_text(path, content)
    return path
