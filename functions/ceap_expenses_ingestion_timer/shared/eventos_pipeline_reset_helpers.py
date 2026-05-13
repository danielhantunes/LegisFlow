"""Pure helpers for eventos pipeline reset (validation + queue body parsing).

No Azure SDK imports here so unit tests run without azure.* installed.
"""

from __future__ import annotations

import base64
import json
import re

# Daily: ``eventos_daily_YYYYMMDD`` (8 trailing digits)
# Microbatch: ``eventos_microbatch_YYYYMMDDHHMM`` (12 trailing digits)
# Reconciliation: ``eventos_reconciliation_YYYYMMDD`` (8 trailing digits)
_EVENTOS_RUN_RE = re.compile(
    r"^eventos_(daily_\d{8}|microbatch_\d{12}|reconciliation_\d{8})$"
)


def is_allowed_eventos_pipeline_run_id(pipeline_run_id: str) -> bool:
    return bool(_EVENTOS_RUN_RE.match((pipeline_run_id or "").strip()))


def safe_path_segment(value: str) -> str:
    return re.sub(r"[^\w.\-]+", "_", value)


def decode_queue_message_text(raw: bytes) -> str:
    if not raw:
        return ""
    try:
        text = raw.decode("utf-8")
        json.loads(text)
        return text
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    try:
        dec = base64.b64decode(raw, validate=True)
        return dec.decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return raw.decode("utf-8", errors="replace")


def message_matches_pipeline_run(raw_body: bytes, pipeline_run_id: str) -> bool:
    text = decode_queue_message_text(raw_body)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return False
    return str(data.get("pipeline_run_id", "") or "").strip() == pipeline_run_id
