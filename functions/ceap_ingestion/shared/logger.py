import json
import logging
from datetime import datetime, timezone
from typing import Any


def get_logger(name: str = "legisflow.ceap") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def log_structured(logger: logging.Logger, level: str, message: str, **fields: Any) -> None:
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "level": level.upper(),
        "message": message,
        **fields,
    }
    getattr(logger, level.lower(), logger.info)(json.dumps(payload, ensure_ascii=True))
