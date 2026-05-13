"""Infer ``DomainWorkMessage.run_type`` when re-enqueueing from replay HTTP endpoints.

When ``new_pipeline_run_id`` is omitted and the target id is the same microbatch
or reconciliation run the dispatcher used, keep that run_type so Raw paths and
manifests stay consistent with the original run.
"""

from __future__ import annotations


def infer_run_type_for_requeued_work(pipeline_run_id: str) -> str:
    p = (pipeline_run_id or "").strip()
    if "_reconciliation_" in p:
        return "reconciliation"
    if "_daily_" in p:
        return "daily"
    if "_microbatch_" in p:
        return "microbatch"
    return "manual_replay"
