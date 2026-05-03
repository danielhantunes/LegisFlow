from __future__ import annotations

import os

import azure.functions as func

from shared.logger import get_logger, log_structured

logger = get_logger()


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    """
    Legacy monolithic CEAP timer (all deputies in one run).

    Default disabled in favour of `ceap_api_2026_dispatcher` + `ceap_api_2026_worker`.
    Set CEAP_LEGACY_MONOLITH_ENABLED=true only for emergency fallback.
    """
    if os.getenv("CEAP_LEGACY_MONOLITH_ENABLED", "false").lower() != "true":
        log_structured(
            logger,
            "info",
            "Legacy CEAP monolith timer is disabled; use ceap_api_2026_dispatcher + queue worker.",
        )
        return

    from shared.legacy_ceap_timer_pipeline import run_legacy_monolith

    run_legacy_monolith(timer)
