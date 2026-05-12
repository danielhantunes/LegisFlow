"""Feature-flag guard for admin/HTTP endpoints (reset, replay, etc.).

Each domain may opt-in via either of:

* ``ENABLE_RESET_FUNCTIONS=true`` (global toggle, applies to every domain)
* ``ENABLE_<DOMAIN>_RESET_FUNCTION=true`` (domain-specific override)

The check is intentionally strict-true (case-insensitive); any other value is
treated as disabled. Functions should call :func:`reset_enabled_for_domain`
before executing a destructive operation.
"""

from __future__ import annotations

import os

GLOBAL_RESET_FLAG_ENV = "ENABLE_RESET_FUNCTIONS"


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() == "true"


def reset_enabled_for_domain(domain_specific_env: str) -> bool:
    """True when either the global or the domain-specific reset flag is set."""
    if _is_truthy(os.getenv(GLOBAL_RESET_FLAG_ENV)):
        return True
    if domain_specific_env and _is_truthy(os.getenv(domain_specific_env)):
        return True
    return False
