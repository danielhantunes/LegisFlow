"""Dev/test helper: reset proposicoes pipeline_run_id artifacts.

Mirrors :mod:`shared.votacoes_pipeline_reset` but scoped to the proposicoes
domain. Guarded by either ``ENABLE_RESET_FUNCTIONS=true`` or
``ENABLE_PROPOSICOES_RESET_FUNCTION=true``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.data.tables import TableServiceClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.queue import QueueServiceClient

from .domain_catalog import PROPOSICOES_DOMAIN
from .proposicoes_pipeline_reset_helpers import (
    message_matches_pipeline_run,
    safe_path_segment,
)
from .queue_helpers import _queue_storage_connection_string


def _escape_odata(s: str) -> str:
    return (s or "").replace("'", "''")


@dataclass
class ResetSummary:
    pipeline_run_id: str
    dry_run: bool
    deleted: dict[str, int] = field(
        default_factory=lambda: {
            "control_run_records": 0,
            "state_records": 0,
            "queue_messages": 0,
            "poison_messages": 0,
            "raw_files": 0,
            "metadata_files": 0,
            "locks": 0,
        }
    )
    warnings: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "dry_run": self.dry_run,
            "deleted": dict(self.deleted),
            "warnings": list(self.warnings),
        }


def _table_client(conn: str, table_name: str) -> Any:
    tsc = TableServiceClient.from_connection_string(conn)
    tsc.create_table_if_not_exists(table_name=table_name)
    return tsc.get_table_client(table_name=table_name)


def _count_or_delete_state_for_run(
    *, state_tc: Any, pipeline_run_id: str, dry_run: bool
) -> int:
    safe = _escape_odata(pipeline_run_id)
    flt = (
        f"PartitionKey eq '{PROPOSICOES_DOMAIN.state_partition_key}' "
        f"and (current_pipeline_run_id eq '{safe}' "
        f"or last_pipeline_run_id eq '{safe}')"
    )
    keys: list[tuple[str, str]] = []
    for ent in state_tc.list_entities(filter=flt):
        pk = str(ent.get("PartitionKey", ""))
        rk = str(ent.get("RowKey", ""))
        if pk and rk:
            keys.append((pk, rk))
    if dry_run:
        return len(keys)
    for pk, rk in keys:
        try:
            state_tc.delete_entity(partition_key=pk, row_key=rk)
        except ResourceNotFoundError:
            pass
    return len(keys)


def _count_or_delete_control_run(
    *, control_tc: Any, pipeline_run_id: str, dry_run: bool
) -> int:
    try:
        control_tc.get_entity(
            partition_key=PROPOSICOES_DOMAIN.runs_partition_key,
            row_key=pipeline_run_id,
        )
    except ResourceNotFoundError:
        return 0
    if dry_run:
        return 1
    try:
        control_tc.delete_entity(
            partition_key=PROPOSICOES_DOMAIN.runs_partition_key,
            row_key=pipeline_run_id,
        )
    except ResourceNotFoundError:
        pass
    return 1


def _clear_dispatcher_lock_if_same_run(
    *, control_tc: Any, pipeline_run_id: str, dry_run: bool
) -> tuple[int, list[str]]:
    warnings: list[str] = []
    try:
        lock = control_tc.get_entity(
            partition_key=PROPOSICOES_DOMAIN.locks_partition_key,
            row_key=PROPOSICOES_DOMAIN.lock_row_key,
        )
    except ResourceNotFoundError:
        return 0, warnings
    lock_pid = str(lock.get("pipeline_run_id", "") or "").strip()
    if lock_pid != pipeline_run_id:
        warnings.append(
            f"Proposicoes dispatcher lock is for pipeline_run_id={lock_pid!r}; not cleared."
        )
        return 0, warnings
    if dry_run:
        return 1, warnings
    now = datetime.now(timezone.utc).isoformat()
    control_tc.upsert_entity(
        entity={
            "PartitionKey": PROPOSICOES_DOMAIN.locks_partition_key,
            "RowKey": PROPOSICOES_DOMAIN.lock_row_key,
            "locked_by": "",
            "locked_until": now,
            "pipeline_run_id": "",
            "updated_at": now,
        },
        mode="merge",
    )
    return 1, warnings


def _purge_queue_by_pipeline_run_id(
    queue_client: Any,
    pipeline_run_id: str,
    *,
    dry_run: bool,
    max_rounds: int = 500,
) -> tuple[int, list[str]]:
    warnings: list[str] = []
    deleted = 0
    empty_streak = 0
    for _round_idx in range(max_rounds):
        try:
            page = queue_client.receive_messages(
                max_messages=32, visibility_timeout=60
            )
        except HttpResponseError as ex:
            warnings.append(f"Queue receive error: {ex}")
            break
        batch = list(page)
        if not batch:
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue
        empty_streak = 0
        for msg in batch:
            content = getattr(msg, "content", None)
            if content is None:
                continue
            raw = content if isinstance(content, bytes) else str(content).encode("utf-8")
            if message_matches_pipeline_run(raw, pipeline_run_id):
                deleted += 1
                if not dry_run:
                    queue_client.delete_message(msg)
    return deleted, warnings


def _filesystem_client(account_name: str, filesystem: str) -> Any:
    url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(
        account_url=url, credential=DefaultAzureCredential()
    ).get_file_system_client(filesystem)


def _list_file_paths(fs: Any, path_prefix: str) -> list[str]:
    out: list[str] = []
    try:
        for p in fs.get_paths(path=path_prefix, recursive=True):
            if getattr(p, "is_directory", False):
                continue
            out.append(str(p.name))
    except (ResourceNotFoundError, HttpResponseError):
        return out
    return out


def _delete_file_path(fs: Any, path: str) -> None:
    fc = fs.get_file_client(path)
    try:
        fc.delete_file()
    except ResourceNotFoundError:
        pass


def run_proposicoes_pipeline_reset(
    *,
    pipeline_run_id: str,
    dry_run: bool,
    delete_raw: bool,
    delete_queues: bool,
    delete_tables: bool,
    conn_str: str,
    control_table: str,
    state_table: str,
    queue_work_name: str,
    queue_poison_name: str,
    raw_account: str,
    filesystem: str,
) -> ResetSummary:
    summary = ResetSummary(pipeline_run_id=pipeline_run_id, dry_run=dry_run)
    control_tc = _table_client(conn_str, control_table)
    state_tc = _table_client(conn_str, state_table)

    if delete_tables:
        summary.deleted["control_run_records"] += _count_or_delete_control_run(
            control_tc=control_tc,
            pipeline_run_id=pipeline_run_id,
            dry_run=dry_run,
        )
        nl, wl = _clear_dispatcher_lock_if_same_run(
            control_tc=control_tc,
            pipeline_run_id=pipeline_run_id,
            dry_run=dry_run,
        )
        summary.deleted["locks"] += nl
        summary.warnings.extend(wl)
        ns = _count_or_delete_state_for_run(
            state_tc=state_tc,
            pipeline_run_id=pipeline_run_id,
            dry_run=dry_run,
        )
        summary.deleted["state_records"] += ns

    if delete_queues:
        qconn = _queue_storage_connection_string()
        qsvc = QueueServiceClient.from_connection_string(qconn)
        dq, wq = _purge_queue_by_pipeline_run_id(
            qsvc.get_queue_client(queue_work_name),
            pipeline_run_id,
            dry_run=dry_run,
        )
        summary.deleted["queue_messages"] += dq
        summary.warnings.extend(wq)
        dp, wp = _purge_queue_by_pipeline_run_id(
            qsvc.get_queue_client(queue_poison_name),
            pipeline_run_id,
            dry_run=dry_run,
        )
        summary.deleted["poison_messages"] += dp
        summary.warnings.extend(wp)

    if delete_raw:
        fs = _filesystem_client(raw_account, filesystem)
        pr_safe = safe_path_segment(pipeline_run_id)
        needle_run = f"pipeline_run_id={pipeline_run_id}"
        needle_safe = f"pipeline_run_id={pr_safe}"
        prefixes = [
            "raw/camara/proposicoes/api/list/",
            "raw/camara/proposicoes/api/autores/",
            "raw/camara/proposicoes/api/tramitacoes/",
            "raw/camara/proposicoes/api/_metadata/",
        ]
        for base_prefix in prefixes:
            for name in _list_file_paths(fs, base_prefix):
                if needle_run not in name and needle_safe not in name:
                    continue
                if "_metadata/" in name.replace("\\", "/"):
                    summary.deleted["metadata_files"] += 1
                else:
                    summary.deleted["raw_files"] += 1
                if not dry_run:
                    _delete_file_path(fs, name)

    return summary
