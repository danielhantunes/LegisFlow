import json
import logging
import os
from datetime import datetime, timezone
from typing import Any


def _parse_log_level(raw: str | None) -> int:
    """Resolve ``LOG_LEVEL`` (default INFO) to a ``logging`` numeric level."""
    name = (raw or "INFO").strip().upper()
    val = getattr(logging, name, None)
    if isinstance(val, int):
        return val
    try:
        return int(name)
    except ValueError:
        return logging.INFO


def get_logger(name: str = "legisflow.ceap") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(_parse_log_level(os.getenv("LOG_LEVEL")))
    return logger


def log_structured(logger: logging.Logger, level: str, message: str, **fields: Any) -> None:
    level_upper = (level or "INFO").strip().upper()
    level_num = getattr(logging, level_upper, None)
    if not isinstance(level_num, int):
        level_num = logging.INFO
        level_upper = "INFO"
    if not logger.isEnabledFor(level_num):
        return
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "level": level_upper,
        "message": message,
        **fields,
    }
    log_method = getattr(logger, (level or "info").lower(), logger.info)
    log_method(json.dumps(payload, ensure_ascii=True))
