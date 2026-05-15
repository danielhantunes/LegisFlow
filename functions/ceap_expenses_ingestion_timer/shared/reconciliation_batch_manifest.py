"""Per-batch manifest for controlled reconciliation (written to ADLS RAW)."""

from __future__ import annotations

from typing import Any

from .adls_writer import AdlsRawWriter


def reconciliation_batch_manifest_path(
    *,
    domain: str,
    control_id: str,
    batch_index: int,
) -> str:
    d = (domain or "").strip().lower()
    cid = (control_id or "").strip()
    return (
        "raw/camara/_reconciliation_batches/"
        f"domain={d}/control_id={cid}/batch_{batch_index:05d}.json"
    )


def build_reconciliation_batch_manifest(
    *,
    control_id: str,
    pipeline_run_id: str,
    domain: str,
    window_start: str,
    window_end: str,
    checkpoint_before: dict[str, Any],
    checkpoint_after: dict[str, Any],
    records_seen: int,
    messages_enqueued: int,
    records_skipped_same_hash: int,
    records_failed: int,
    status: str,
    started_at: str,
    finished_at: str,
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "kind": "reconciliation_batch",
        "control_id": control_id,
        "pipeline_run_id": pipeline_run_id,
        "domain": domain,
        "window_start": window_start,
        "window_end": window_end,
        "checkpoint_before": checkpoint_before,
        "checkpoint_after": checkpoint_after,
        "records_seen": int(records_seen),
        "messages_enqueued": int(messages_enqueued),
        "records_skipped_same_hash": int(records_skipped_same_hash),
        "records_failed": int(records_failed),
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    if extras:
        body["extras"] = extras
    return body


def persist_reconciliation_batch_manifest(
    raw_writer: AdlsRawWriter,
    path: str,
    manifest: dict[str, Any],
) -> str:
    raw_writer.write_json(path, manifest)
    return path
