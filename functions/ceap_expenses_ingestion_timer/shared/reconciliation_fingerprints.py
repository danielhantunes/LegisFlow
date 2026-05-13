"""Merge fingerprints across dispatcher ticks (reconciliation list resume)."""

from __future__ import annotations

from typing import Any

from .adls_writer import AdlsRawWriter


def fingerprints_json_path(manifest_prefix: str) -> str:
    """``manifest_prefix`` = directory ending with ``pipeline_run_id={pid}``."""
    return f"{manifest_prefix.rstrip('/')}/discovered_fingerprints.json"


def load_fingerprints(adls: AdlsRawWriter, manifest_prefix: str) -> dict[str, dict[str, str]]:
    path = fingerprints_json_path(manifest_prefix)
    data = adls.read_json(path) or {}
    out: dict[str, dict[str, str]] = {}
    for vid, entry in (data.get("fingerprints") or {}).items():
        if not isinstance(entry, dict):
            continue
        out[str(vid)] = {
            "uid": str(entry.get("uid", "") or ""),
            "hash": str(entry.get("hash", "") or ""),
        }
    return out


def save_fingerprints(
    adls: AdlsRawWriter, manifest_prefix: str, fingerprints: dict[str, dict[str, str]]
) -> None:
    path = fingerprints_json_path(manifest_prefix)
    adls.write_json(path, {"fingerprints": fingerprints})
