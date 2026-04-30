import random
import time
from typing import Callable, TypeVar

import requests

T = TypeVar("T")

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
BACKOFF_SECONDS = [2, 5, 12]


def run_with_retry(callable_fn: Callable[[], T], max_attempts: int = 3) -> T:
    last_exception: Exception | None = None
    attempts = min(max_attempts, len(BACKOFF_SECONDS))

    for attempt_idx in range(attempts):
        try:
            return callable_fn()
        except requests.exceptions.Timeout as exc:
            last_exception = exc
        except requests.exceptions.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = response.status_code if response is not None else None
            if status_code not in RETRYABLE_STATUS_CODES:
                raise
            last_exception = exc

        if attempt_idx < attempts - 1:
            delay = BACKOFF_SECONDS[attempt_idx] + random.uniform(0.2, 0.9)
            time.sleep(delay)

    if last_exception:
        raise last_exception
    raise RuntimeError("Retry execution failed without explicit exception.")
