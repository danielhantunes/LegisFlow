"""Tests for shared.logger (level gating + LOG_LEVEL)."""

from __future__ import annotations

import json
import logging
import os
from unittest.mock import patch

import pytest

from shared.logger import get_logger, log_structured


@pytest.fixture
def capture_logger() -> logging.Logger:
    name = "legisflow.test_logger_capture"
    log = logging.getLogger(name)
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    class ListHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: list[str] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(record.getMessage())

    h = ListHandler()
    h.setLevel(logging.DEBUG)
    log.addHandler(h)
    log._test_handler = h  # type: ignore[attr-defined]
    return log


def test_log_structured_skips_when_logger_level_above_message(capture_logger: logging.Logger) -> None:
    capture_logger.setLevel(logging.WARNING)
    log_structured(capture_logger, "info", "should not appear", x=1)
    h = capture_logger._test_handler  # type: ignore[attr-defined]
    assert h.records == []


def test_log_structured_emits_when_enabled(capture_logger: logging.Logger) -> None:
    capture_logger.setLevel(logging.INFO)
    log_structured(capture_logger, "info", "hello", k="v")
    h = capture_logger._test_handler  # type: ignore[attr-defined]
    assert len(h.records) == 1
    data = json.loads(h.records[0])
    assert data["message"] == "hello"
    assert data["k"] == "v"


def test_get_logger_respects_log_level_env() -> None:
    with patch.dict(os.environ, {"LOG_LEVEL": "ERROR"}, clear=False):
        lg = get_logger("legisflow.test_log_level_env_unique")
    assert lg.level == logging.ERROR
