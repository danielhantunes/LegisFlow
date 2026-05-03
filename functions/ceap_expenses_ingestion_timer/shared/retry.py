"""HTTP retry with bounded exponential-style backoff (API-friendly)."""

from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

import requests

T = TypeVar("T")

# Retriable at HTTP layer (transient / throttling / server errors).
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
# Do not retry these at the HTTP layer (caller may still use queue replay).
NON_RETRYABLE_STATUS_CODES = frozenset({400, 401, 403, 404})

# Attempt 1 immediate; before attempts 2–5 wait (seconds) after failure.
BACKOFF_AFTER_FAILURE_SECONDS = (2.0, 5.0, 15.0, 15.0)


def run_with_retry(callable_fn: Callable[[], T], *, max_attempts: int = 5) -> T:
    """
    Run callable_fn up to max_attempts times.

    Retries on: timeouts and HTTP responses in RETRYABLE_STATUS_CODES.
    Does not retry: NON_RETRYABLE_STATUS_CODES (raises immediately).
    """
    last_exception: Exception | None = None
    attempts = max(1, min(max_attempts, 5))

    for attempt_idx in range(attempts):
        try:
            return callable_fn()
        except requests.exceptions.Timeout as exc:
            last_exception = exc
        except requests.exceptions.HTTPError as exc:
            response = exc.response
            status_code = response.status_code if response is not None else None
            if status_code in NON_RETRYABLE_STATUS_CODES:
                raise
            if status_code not in RETRYABLE_STATUS_CODES:
                raise
            last_exception = exc
        except requests.exceptions.RequestException as exc:
            # Connection errors etc.: treat as retriable
            last_exception = exc

        if attempt_idx < attempts - 1:
            delay = BACKOFF_AFTER_FAILURE_SECONDS[attempt_idx] + random.uniform(0.1, 0.6)
            time.sleep(delay)

    if last_exception:
        raise last_exception
    raise RuntimeError("Retry execution failed without explicit exception.")
