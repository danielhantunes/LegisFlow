"""Pytest config (repo root): makes ``shared.*`` importable and exposes an
in-memory fake for :class:`AdlsRawWriter` so Raw-layer tests run without any
Azure connectivity.

The fake is duck-typed: any helper accepting an ``AdlsRawWriter`` only relies
on ``write_json``/``write_text``/``path_exists``/``read_json``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
FUNCTION_ROOT = REPO_ROOT / "functions" / "ceap_expenses_ingestion_timer"
if str(FUNCTION_ROOT) not in sys.path:
    sys.path.insert(0, str(FUNCTION_ROOT))


class InMemoryRawWriter:
    """Duck-typed stand-in for :class:`AdlsRawWriter`.

    Stores ``write_json`` payloads as JSON strings (mirrors the production
    writer behaviour) so tests can re-read them via :meth:`read_json`.
    """

    def __init__(self) -> None:
        self.files: dict[str, str] = {}
        self.json_files: dict[str, dict[str, Any]] = {}
        self.text_files: dict[str, str] = {}

    def write_json(self, path: str, payload: dict[str, Any]) -> str:
        content = json.dumps(payload, ensure_ascii=False)
        self.files[path] = content
        self.json_files[path] = payload
        return path

    def write_text(self, path: str, content: str) -> str:
        self.files[path] = content
        self.text_files[path] = content
        return path

    def path_exists(self, path: str) -> bool:
        return path in self.files

    def read_json(self, path: str) -> dict[str, Any] | None:
        if path not in self.json_files:
            return None
        return json.loads(json.dumps(self.json_files[path]))

    def list_subdirectories(self, prefix: str) -> list[str]:
        prefixes: set[str] = set()
        normalised = prefix.rstrip("/") + "/"
        for path in self.files:
            if not path.startswith(normalised):
                continue
            tail = path[len(normalised):]
            head = tail.split("/", 1)[0]
            if head:
                prefixes.add(normalised + head)
        return sorted(prefixes)


@pytest.fixture()
def raw_writer() -> InMemoryRawWriter:
    return InMemoryRawWriter()
