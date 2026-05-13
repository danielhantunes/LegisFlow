"""Timer: build consolidated ``daily_summary.json`` across ingestion domains."""

from __future__ import annotations

import azure.functions as func

from shared.daily_summary import run_daily_summary_tick


def main(timer: func.TimerRequest) -> None:  # noqa: ARG001
    run_daily_summary_tick()
