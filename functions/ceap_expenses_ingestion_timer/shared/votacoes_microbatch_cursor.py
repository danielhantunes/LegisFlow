"""Global microbatch cursor for ``/votacoes`` incremental listing (by numeric id).

``last_processed_votacao_id`` is advanced **only** by :mod:`votacoes_api_worker`
after a **microbatch** run completes with ``SUCCESS``. The dispatcher uses it to
skip API rows already ingested, avoiding a full safety-window scan every tick.

Uses duck-typed ``parts`` (:class:`shared.generic_partition_state.GenericPartitionStateStore`)
without importing that module at load time so unit tests can import sort helpers
without installing ``azure-data-tables``.
"""

from __future__ import annotations

VOTACOES_MICROBATCH_CURSOR_ROW_KEY = "_cursor_microbatch_v1"


def votacao_id_sort_key(vid: str) -> tuple[int, str]:
    """Sort key: numeric ids first (ascending), then lexicographic fallback."""
    s = str(vid).strip()
    if s.isdigit():
        return (int(s), "")
    return (10**18, s)


def last_processed_votacao_id_int(parts: object) -> int:
    ent = parts.get_partition(VOTACOES_MICROBATCH_CURSOR_ROW_KEY) or {}
    raw = str(ent.get("last_processed_votacao_id") or "").strip()
    if raw.isdigit():
        return int(raw)
    return 0


def advance_last_processed_votacao_cursor(parts: object, *, votacao_id: str) -> None:
    """Monotonic max cursor in Table Storage (best-effort under concurrency)."""
    s = str(votacao_id).strip()
    if not s.isdigit():
        return
    new_i = int(s)
    ent = parts.get_partition(VOTACOES_MICROBATCH_CURSOR_ROW_KEY) or {}
    cur = str(ent.get("last_processed_votacao_id") or "").strip()
    cur_i = int(cur) if cur.isdigit() else 0
    if new_i <= cur_i:
        return
    parts.upsert_partition(
        VOTACOES_MICROBATCH_CURSOR_ROW_KEY,
        {
            "endpoint": "_cursor",
            "votacao_id": "",
            "last_processed_votacao_id": str(new_i),
            "status": "CURSOR",
        },
    )
