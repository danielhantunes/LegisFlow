"""Consolidated daily ingestion summary (``daily_summary.json``) across domains.

Writes under:

``raw/camara/_metadata/daily_summary/reference_date=YYYY-MM-DD/daily_summary.json``

Optionally ``_SUCCESS`` when the day is fully green (see module docstring in builder).
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from typing import Any, Callable

from .daily_summary_types import (
    CRITICAL_WARNING_TYPES,
    METADATA_VERSION,
    RunInspection,
    append_run_warning,
    canonical_domain_name,
    compact_yyyymmdd,
    daily_summary_json_path,
    daily_summary_success_path,
    int_field,
    normalize_run_status,
    parse_expected_domains,
    resolve_reference_date_string,
    rollup_daily_status,
    rollup_domain_status,
)
from .adls_writer import AdlsRawWriter
from .ceap_raw_manifest import ceap_run_metadata_path, ceap_run_success_path
from .ceap_run_registry import CeapRunRegistry
from .discursos_raw_manifest import discursos_run_metadata_path, discursos_run_success_path
from .domain_catalog import (
    REFERENCE_DOMAIN,
    VOTACOES_DOMAIN,
    get_domain,
    institucional_daily_run_id,
    reference_run_id_for_date,
    votacoes_reconciliation_run_id,
)
from .eventos_raw_manifest import eventos_run_metadata_path, eventos_run_success_path
from .institucional_raw_manifest import institucional_run_metadata_path, institucional_run_success_path
from .logger import get_logger, log_structured
from .proposicoes_raw_manifest import proposicoes_run_metadata_path, proposicoes_run_success_path
from .reference_raw_manifest import reference_endpoint_metadata_path, reference_endpoint_success_path
from .run_registry import GenericRunRegistry
from .votacoes_raw_manifest import votacoes_run_metadata_path, votacoes_run_success_path

logger = get_logger()

_RUNS_ROOT_BY_DOMAIN: dict[str, str] = {
    "ceap": "raw/camara/ceap/api/despesas/_metadata/runs",
    "votacoes": "raw/camara/votacoes/api/_metadata/runs",
    "proposicoes": "raw/camara/proposicoes/api/_metadata/runs",
    "eventos": "raw/camara/eventos/api/_metadata/runs",
    "institucional": "raw/camara/institucional/api/_metadata/runs",
    "discursos": "raw/camara/discursos/api/_metadata/runs",
}


def _list_pipeline_run_ids_for_compact(adls: AdlsRawWriter, runs_root: str, ref_compact: str) -> list[str]:
    """Immediate children ``pipeline_run_id=*`` whose id contains ``ref_compact``."""
    found: set[str] = set()
    root = runs_root.rstrip("/")
    try:
        for p in adls.fs_client.get_paths(path=root, recursive=False):
            if not getattr(p, "is_directory", False):
                continue
            name = (p.name or "").replace("\\", "/").rstrip("/")
            if "pipeline_run_id=" not in name:
                continue
            seg = name.split("pipeline_run_id=", 1)[-1].split("/", 1)[0]
            if ref_compact and ref_compact in seg:
                found.add(seg)
    except Exception:
        return []
    return sorted(found)


def _inspect_generic_aggregate(
    *,
    domain_name: str,
    entity: str,
    pipeline_run_id: str,
    metadata_path: str,
    success_path: str,
    adls: AdlsRawWriter,
    control_row: dict[str, Any] | None,
) -> RunInspection:
    insp = RunInspection(
        domain=domain_name,
        entity=entity,
        pipeline_run_id=pipeline_run_id,
        metadata_path=metadata_path,
        success_marker_path=success_path,
        control_row=control_row,
    )
    meta = adls.read_json(metadata_path)
    if meta is None:
        insp.status = "NOT_FOUND"
        append_run_warning(insp, warning_type="metadata_not_found", message="metadata.json missing")
        return insp
    insp.metadata = meta
    st = normalize_run_status(str(meta.get("status", "")))
    insp.run_type = str(meta.get("run_type", "") or "")
    insp.started_at = str(meta.get("started_at") or meta.get("started_at_utc") or "")
    insp.completed_at = str(meta.get("completed_at") or meta.get("finished_at_utc") or "")
    insp.total_tasks_expected = int_field(meta, "total_tasks_expected")
    insp.total_tasks_success = int_field(meta, "total_tasks_success")
    insp.total_tasks_failed = int_field(meta, "total_tasks_failed")
    insp.total_tasks_pending = int_field(meta, "total_tasks_pending")
    insp.total_tasks_poison = int_field(meta, "total_tasks_poison")
    insp.total_tasks_running = int_field(meta, "total_tasks_running")
    insp.total_tasks_queued = int_field(meta, "total_tasks_queued")
    trw = meta.get("total_raw_files_written")
    trc = meta.get("total_records_collected")
    if trw is not None:
        insp.total_raw_files_written = int_field(meta, "total_raw_files_written")
    if trc is not None:
        insp.total_records_collected = int_field(meta, "total_records_collected")

    if st == "COMPLETED" and not adls.path_exists(success_path):
        append_run_warning(
            insp,
            warning_type="success_marker_missing",
            message="metadata.status is COMPLETED but _SUCCESS was not found",
        )
    if adls.path_exists(success_path) and st != "COMPLETED" and st != "NO_DATA":
        append_run_warning(
            insp,
            warning_type="success_marker_exists_but_status_not_completed",
            message=f"_SUCCESS exists but metadata.status is {st}",
        )
    exp = insp.total_tasks_expected
    if exp > 0 and insp.total_tasks_success != exp and st == "COMPLETED":
        append_run_warning(
            insp,
            warning_type="task_counts_inconsistent",
            message="status COMPLETED but total_tasks_success != total_tasks_expected",
        )
    if insp.total_tasks_failed > 0 or insp.total_tasks_poison > 0:
        if st in {"COMPLETED", "NO_DATA"}:
            append_run_warning(
                insp,
                warning_type="task_counts_inconsistent",
                message="failed/poison counters > 0 but status is terminal-success-like",
            )
            st = "FAILED"

    if st in {"FAILED"} or insp.total_tasks_failed > 0 or insp.total_tasks_poison > 0:
        insp.status = "FAILED"
    elif st in {"RUNNING", "QUEUED", "QUEUING"}:
        insp.status = st
    elif st in {"PARTIAL", "PARTIALLY_COMPLETED"}:
        insp.status = "PARTIAL"
    elif st == "NO_DATA":
        insp.status = "NO_DATA"
    elif st == "COMPLETED":
        insp.status = "COMPLETED"
    else:
        insp.status = st or "INVALID_METADATA"

    if control_row:
        cst = normalize_run_status(str(control_row.get("status", "")))
        mst = insp.status
        if cst and mst and cst != mst and mst not in {"NOT_FOUND", "INVALID_METADATA"}:
            append_run_warning(
                insp,
                warning_type="ingestion_control_differs_from_raw_metadata",
                message=f"control status {cst} vs metadata-derived {mst}",
            )
    return insp


def _ceap_candidates(reference_date: str, now_utc: datetime) -> list[str]:
    """CEAP dispatcher uses UTC calendar day for pipeline_run_id (see ceap_api_2026_dispatcher)."""
    compact_sp = compact_yyyymmdd(reference_date)
    compact_utc = now_utc.strftime("%Y%m%d")
    cands = [f"ceap_daily_{compact_sp}", f"ceap_reconciliation_{compact_sp}"]
    if compact_utc != compact_sp:
        cands.extend([f"ceap_daily_{compact_utc}", f"ceap_reconciliation_{compact_utc}"])
    # de-dupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for p in cands:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _recon_day_for(iso_date: str, recon_day: int) -> bool:
    try:
        d = date.fromisoformat(iso_date)
    except ValueError:
        return False
    return d.day == recon_day


def _collect_ceap_runs(
    *,
    adls: AdlsRawWriter,
    registry: CeapRunRegistry,
    reference_date: str,
    now_utc: datetime,
) -> list[RunInspection]:
    out: list[RunInspection] = []
    recon_day = int(os.getenv("CEAP_RECONCILIATION_DAY", "25"))
    for pid in _ceap_candidates(reference_date, now_utc):
        if pid.startswith("ceap_reconciliation_") and not _recon_day_for(reference_date, recon_day):
            continue
        meta_path = ceap_run_metadata_path(pid)
        if not adls.path_exists(meta_path) and registry.get_run(pid) is None:
            continue
        ctrl = registry.get_run(pid)
        insp = _inspect_generic_aggregate(
            domain_name="ceap",
            entity="deputado_despesas",
            pipeline_run_id=pid,
            metadata_path=meta_path,
            success_path=ceap_run_success_path(pid),
            adls=adls,
            control_row=ctrl,
        )
        out.append(insp)
    return out


def _control_for_generic(
    conn: str,
    table: str,
    runs_partition_key: str,
    locks_partition_key: str,
    lock_row_key: str,
    pipeline_run_id: str,
) -> dict[str, Any] | None:
    reg = GenericRunRegistry.from_connection_string(
        conn,
        table,
        runs_partition_key=runs_partition_key,
        locks_partition_key=locks_partition_key,
        lock_row_key=lock_row_key,
    )
    return reg.get_run(pipeline_run_id)


def _collect_microbatch_family(
    *,
    domain_name: str,
    entity: str,
    adls: AdlsRawWriter,
    conn: str,
    control_table: str,
    domain_spec,
    ref_compact: str,
    metadata_path_fn: Callable[[str], str],
    success_path_fn: Callable[[str], str],
) -> list[RunInspection]:
    runs_root = _RUNS_ROOT_BY_DOMAIN.get(domain_name, "")
    if not runs_root:
        return []
    pids = _list_pipeline_run_ids_for_compact(adls, runs_root, ref_compact)
    out: list[RunInspection] = []
    for pid in pids:
        ctrl = _control_for_generic(
            conn,
            control_table,
            domain_spec.runs_partition_key,
            domain_spec.locks_partition_key,
            domain_spec.lock_row_key,
            pid,
        )
        out.append(
            _inspect_generic_aggregate(
                domain_name=domain_name,
                entity=entity,
                pipeline_run_id=pid,
                metadata_path=metadata_path_fn(pid),
                success_path=success_path_fn(pid),
                adls=adls,
                control_row=ctrl,
            )
        )
    return out


def _collect_votacoes(
    *,
    adls: AdlsRawWriter,
    conn: str,
    control_table: str,
    reference_date: str,
    ref_compact: str,
    now_utc: datetime,
) -> list[RunInspection]:
    spec = VOTACOES_DOMAIN
    out: list[RunInspection] = []
    recon_day = int(os.getenv("VOTACOES_RECONCILIATION_DAY", "25"))
    if _recon_day_for(reference_date, recon_day):
        rid = votacoes_reconciliation_run_id(reference_date)
        ctrl = _control_for_generic(
            conn,
            control_table,
            spec.runs_partition_key,
            spec.locks_partition_key,
            spec.lock_row_key,
            rid,
        )
        mp = votacoes_run_metadata_path(rid)
        if adls.path_exists(mp) or ctrl:
            out.append(
                _inspect_generic_aggregate(
                    domain_name="votacoes",
                    entity="votacoes",
                    pipeline_run_id=rid,
                    metadata_path=mp,
                    success_path=votacoes_run_success_path(rid),
                    adls=adls,
                    control_row=ctrl,
                )
            )
    out.extend(
        _collect_microbatch_family(
            domain_name="votacoes",
            entity="votacoes",
            adls=adls,
            conn=conn,
            control_table=control_table,
            domain_spec=spec,
            ref_compact=ref_compact,
            metadata_path_fn=votacoes_run_metadata_path,
            success_path_fn=votacoes_run_success_path,
        )
    )
    # de-dupe by pipeline_run_id
    seen: set[str] = set()
    deduped: list[RunInspection] = []
    for r in out:
        if r.pipeline_run_id in seen:
            continue
        seen.add(r.pipeline_run_id)
        deduped.append(r)
    return deduped


def _collect_reference_runs(
    *,
    adls: AdlsRawWriter,
    conn: str,
    control_table: str,
    reference_date: str,
) -> list[RunInspection]:
    pid = reference_run_id_for_date(reference_date)
    spec = REFERENCE_DOMAIN
    ctrl = _control_for_generic(
        conn,
        control_table,
        spec.runs_partition_key,
        spec.locks_partition_key,
        spec.lock_row_key,
        pid,
    )
    out: list[RunInspection] = []
    for ep in spec.endpoints:
        mp = reference_endpoint_metadata_path(ep, reference_date, pid)
        sp = reference_endpoint_success_path(ep, reference_date, pid)
        out.append(
            _inspect_generic_aggregate(
                domain_name="reference",
                entity=ep.name,
                pipeline_run_id=pid,
                metadata_path=mp,
                success_path=sp,
                adls=adls,
                control_row=ctrl,
            )
        )
    return out


def _collect_institucional(
    *,
    adls: AdlsRawWriter,
    conn: str,
    control_table: str,
    reference_date: str,
    now_utc: datetime,
) -> list[RunInspection]:
    spec = get_domain("institucional")
    # Dispatcher uses UTC date for pipeline_run_id; include SP date too.
    pid_sp = institucional_daily_run_id(reference_date)
    pid_utc = institucional_daily_run_id(now_utc.date().isoformat())
    pids = [pid_sp]
    if pid_utc != pid_sp:
        pids.append(pid_utc)
    out: list[RunInspection] = []
    for pid in pids:
        if any(r.pipeline_run_id == pid for r in out):
            continue
        ctrl = _control_for_generic(
            conn,
            control_table,
            spec.runs_partition_key,
            spec.locks_partition_key,
            spec.lock_row_key,
            pid,
        )
        mp = institucional_run_metadata_path(pid)
        if not adls.path_exists(mp) and ctrl is None:
            continue
        out.append(
            _inspect_generic_aggregate(
                domain_name="institucional",
                entity="institucional",
                pipeline_run_id=pid,
                metadata_path=mp,
                success_path=institucional_run_success_path(pid),
                adls=adls,
                control_row=ctrl,
            )
        )
    return out


def build_daily_summary_document(
    *,
    reference_date: str,
    reference_timezone: str,
    expected_domains: list[str],
    now_utc: datetime,
    adls: AdlsRawWriter,
    conn: str,
    control_table: str,
) -> tuple[dict[str, Any], list[RunInspection]]:
    ref_compact = compact_yyyymmdd(reference_date)
    runs_out: list[RunInspection] = []
    domain_groups: dict[str, list[RunInspection]] = {}

    for raw_name in expected_domains:
        canon = canonical_domain_name(raw_name)
        try:
            spec = get_domain(canon)
        except KeyError:
            placeholder = RunInspection(
                domain=raw_name,
                entity="",
                pipeline_run_id="",
                metadata_path="",
                success_marker_path="",
                status="NOT_IMPLEMENTED",
            )
            append_run_warning(
                placeholder,
                warning_type="domain_not_implemented",
                message=f"Domain {raw_name!r} is not registered in domain_catalog.",
            )
            runs_out.append(placeholder)
            domain_groups.setdefault(raw_name, []).append(placeholder)
            continue

        collected: list[RunInspection] = []
        if canon == "ceap":
            collected = _collect_ceap_runs(
                adls=adls, registry=CeapRunRegistry.from_connection_string(conn, control_table), reference_date=reference_date, now_utc=now_utc
            )
        elif canon == "reference":
            collected = _collect_reference_runs(
                adls=adls, conn=conn, control_table=control_table, reference_date=reference_date
            )
        elif canon == "votacoes":
            collected = _collect_votacoes(
                adls=adls,
                conn=conn,
                control_table=control_table,
                reference_date=reference_date,
                ref_compact=ref_compact,
                now_utc=now_utc,
            )
        elif canon == "proposicoes":
            collected = _collect_microbatch_family(
                domain_name="proposicoes",
                entity="proposicoes",
                adls=adls,
                conn=conn,
                control_table=control_table,
                domain_spec=spec,
                ref_compact=ref_compact,
                metadata_path_fn=proposicoes_run_metadata_path,
                success_path_fn=proposicoes_run_success_path,
            )
        elif canon == "eventos":
            collected = _collect_microbatch_family(
                domain_name="eventos",
                entity="eventos",
                adls=adls,
                conn=conn,
                control_table=control_table,
                domain_spec=spec,
                ref_compact=ref_compact,
                metadata_path_fn=eventos_run_metadata_path,
                success_path_fn=eventos_run_success_path,
            )
        elif canon == "institucional":
            collected = _collect_institucional(
                adls=adls,
                conn=conn,
                control_table=control_table,
                reference_date=reference_date,
                now_utc=now_utc,
            )
        elif canon == "discursos":
            collected = _collect_microbatch_family(
                domain_name="discursos",
                entity="discursos",
                adls=adls,
                conn=conn,
                control_table=control_table,
                domain_spec=spec,
                ref_compact=ref_compact,
                metadata_path_fn=discursos_run_metadata_path,
                success_path_fn=discursos_run_success_path,
            )
        else:
            ph = RunInspection(
                domain=raw_name,
                entity=canon,
                pipeline_run_id="",
                metadata_path="",
                success_marker_path="",
                status="NOT_IMPLEMENTED",
            )
            append_run_warning(
                ph,
                warning_type="domain_not_implemented",
                message=f"No collector wired for domain {canon!r}.",
            )
            collected = [ph]

        if not collected and canon not in {"ceap", "reference", "votacoes", "institucional"}:
            nf = RunInspection(
                domain=raw_name,
                entity=canon,
                pipeline_run_id="",
                metadata_path="",
                success_marker_path="",
                status="NOT_FOUND",
            )
            append_run_warning(
                nf,
                warning_type="metadata_not_found",
                message="No pipeline_run_id directories matched this reference_date.",
            )
            collected = [nf]

        runs_out.extend(collected)
        domain_groups[raw_name] = collected

    # Domain-level rollups keyed by *expected* list name for stable reporting
    domain_statuses: dict[str, str] = {}
    for raw_name, rows in domain_groups.items():
        domain_statuses[raw_name] = rollup_domain_status(rows)

    expected_keys = list(expected_domains)
    daily_status = rollup_daily_status(domain_statuses, expected=expected_keys)

    flat_warnings: list[dict[str, Any]] = []
    for r in runs_out:
        flat_warnings.extend(r.warnings)

    summary_block = {
        "total_domains_expected": len(expected_keys),
        "total_domains_completed": sum(
            1 for d in expected_keys if domain_statuses.get(d) == "COMPLETED"
        ),
        "total_domains_running": sum(
            1
            for d in expected_keys
            if domain_statuses.get(d) in {"RUNNING", "QUEUED", "QUEUING"}
        ),
        "total_domains_failed": sum(1 for d in expected_keys if domain_statuses.get(d) == "FAILED"),
        "total_domains_no_data": sum(
            1 for d in expected_keys if domain_statuses.get(d) == "NO_DATA"
        ),
        "total_domains_not_found": sum(
            1 for d in expected_keys if domain_statuses.get(d) == "NOT_FOUND"
        ),
        "total_domains_partial": sum(
            1 for d in expected_keys if domain_statuses.get(d) == "PARTIAL"
        ),
        "total_domains_not_implemented": sum(
            1 for d in expected_keys if domain_statuses.get(d) == "NOT_IMPLEMENTED"
        ),
    }

    runs_json: list[dict[str, Any]] = []
    for r in runs_out:
        runs_json.append(
            {
                "domain": r.domain,
                "entity": r.entity,
                "pipeline_run_id": r.pipeline_run_id,
                "run_type": r.run_type,
                "status": r.status,
                "metadata_path": r.metadata_path,
                "success_marker_path": r.success_marker_path,
                "started_at": r.started_at,
                "completed_at": r.completed_at,
                "total_tasks_expected": r.total_tasks_expected,
                "total_tasks_success": r.total_tasks_success,
                "total_tasks_failed": r.total_tasks_failed,
                "total_tasks_pending": r.total_tasks_pending,
                "total_tasks_poison": r.total_tasks_poison,
                "total_tasks_running": r.total_tasks_running,
                "total_tasks_queued": r.total_tasks_queued,
                "total_raw_files_written": r.total_raw_files_written,
                "total_records_collected": r.total_records_collected,
                "warnings": r.warnings,
            }
        )

    doc: dict[str, Any] = {
        "metadata_version": METADATA_VERSION,
        "source": "camara",
        "summary_type": "daily_runs",
        "reference_date": reference_date,
        "reference_timezone": reference_timezone,
        "status": daily_status,
        "created_at": now_utc.isoformat(),
        "updated_at": now_utc.isoformat(),
        "expected_domains": expected_keys,
        "summary": {k: v for k, v in summary_block.items() if not str(k).startswith("_")},
        "domain_status": domain_statuses,
        "runs": runs_json,
        "warnings": flat_warnings,
    }
    return doc, runs_out


def persist_daily_summary(
    *,
    adls: AdlsRawWriter,
    reference_date: str,
    document: dict[str, Any],
    create_success_marker: bool,
) -> tuple[str, bool]:
    path = daily_summary_json_path(reference_date)
    adls.write_json(path, document)
    success_written = False
    ok = (
        document.get("status") == "COMPLETED"
        and not any(
            w.get("warning_type") in CRITICAL_WARNING_TYPES for w in (document.get("warnings") or [])
        )
    )
    if create_success_marker and ok:
        sp = daily_summary_success_path(reference_date)
        adls.write_text(sp, "")
        success_written = True
    return path, success_written


def run_daily_summary_tick() -> dict[str, Any]:
    """Entry point for the timer function."""
    if str(os.getenv("DAILY_SUMMARY_ENABLED", "true")).lower() not in ("1", "true", "yes"):
        return {"skipped": True, "reason": "disabled"}

    now_utc = datetime.now(UTC)
    tz_name = os.getenv("DAILY_SUMMARY_REFERENCE_TIMEZONE", "America/Sao_Paulo")
    reference_date = resolve_reference_date_string(tz_name=tz_name, now_utc=now_utc)
    expected = parse_expected_domains(os.getenv("DAILY_SUMMARY_EXPECTED_DOMAINS", ""))

    conn = os.environ["AzureWebJobsStorage"]
    control_table = os.getenv("INGESTION_CONTROL_TABLE", "IngestionControlApi2026")
    raw_account = os.environ["RAW_STORAGE_ACCOUNT_NAME"]
    create_marker = str(os.getenv("DAILY_SUMMARY_CREATE_SUCCESS_MARKER", "true")).lower() in (
        "1",
        "true",
        "yes",
    )

    adls = AdlsRawWriter(account_name=raw_account)
    doc, _rows = build_daily_summary_document(
        reference_date=reference_date,
        reference_timezone=tz_name,
        expected_domains=expected,
        now_utc=now_utc,
        adls=adls,
        conn=conn,
        control_table=control_table,
    )
    summary_path, marker = persist_daily_summary(
        adls=adls,
        reference_date=reference_date,
        document=doc,
        create_success_marker=create_marker,
    )

    ds = doc.get("summary") or {}
    log_structured(
        logger,
        "info",
        "Daily summary builder finished.",
        reference_date=reference_date,
        domains_expected=expected,
        domains_found=[d for d, st in (doc.get("domain_status") or {}).items() if st != "NOT_FOUND"],
        total_completed=ds.get("total_domains_completed"),
        total_running=ds.get("total_domains_running"),
        total_failed=ds.get("total_domains_failed"),
        total_no_data=ds.get("total_domains_no_data"),
        total_not_found=ds.get("total_domains_not_found"),
        daily_status=doc.get("status"),
        daily_summary_path=summary_path,
        success_marker_created=marker,
        warnings_count=len(doc.get("warnings") or []),
    )
    return {
        "reference_date": reference_date,
        "daily_summary_path": summary_path,
        "status": doc.get("status"),
        "success_marker_created": marker,
        "warnings_count": len(doc.get("warnings") or []),
    }
