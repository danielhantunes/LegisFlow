# Technical backlog — LegisFlow (prioritized)

Generated from repository state. Items **do not** imply issue tracking already exists; adjust priorities to the product roadmap.

## P0 — Fixes and alignment

1. **Review ADR-003** in `docs/decisions.md`: the code already hosts multiple domains in one Function App; update the ADR or mark it superseded with a pointer to `docs/current_state.md`.
2. **Update `docs/architecture.md` historical sections** if anything still says “CEAP only” in places not updated by the latest merge (grep for “Not implemented” / “CEAP-only”).
3. **Validate `terraform plan` (ingestion)** after queue or app-setting changes in each environment (dev/staging/prod).

## P1 — Pending Azure validation

1. **End-to-end deploy:** `terraform-ingestion-dev` (plan → apply on `main`) + `deploy-function-ceap.yml` with a commit that includes all function folders.
2. **Per-domain smoke test:** one dispatcher tick + one worker message + verify controlled-run `metadata.json` and `_SUCCESS` in ADLS.
3. **Poison queues:** simulate failure (429/500) or exhausted `maxDequeueCount` and confirm writes to poison + handler behavior.
4. **HTTP replay:** reprocess a test `FAILED` partition; confirm `QUEUED` state and new `execution_id`.
5. **HTTP reset:** `dry_run=true` first; only then a real delete in a disposable environment.

## P2 — Technical product improvements

1. **Schedules per environment:** CRONs in “validation” mode (10/20 min); document switching to production cadence via Terraform variables.
2. **Observability:** Kusto dashboards / workbooks filtering on `domain` and `pipeline_run_id` in structured logs.
3. **Discursos replay:** ensure `last_window_date_start` / `last_window_date_end` are populated in `IngestionState` in the worker for replays without query params (today replay may depend on overrides — see `fn_replay_discursos_failed_messages`).
4. **Limits:** review `MAX_LIST_PAGES` / `MAX_MESSAGES_PER_TICK` per domain against real API volume (risk of silent truncation if `links.next` exists beyond the cap).

## P3 — Platform extension

1. **Bronze/Delta:** define layout and Databricks jobs for `raw/camara/{votacoes,eventos,proposicoes,institucional,discursos}/...` (documentation today focuses on CEAP).
2. **Integration tests:** optional CI pipeline with Azurite + mock HTTP or a read-only test subscription.
3. **Feature flags:** clear policy for enabling new dispatchers in production (disable via `AzureWebJobs.<FunctionName>.Disabled` until go-live).

## P4 — Debt / hygiene

1. **Deploy workflow name:** `deploy-function-ceap.yml` deploys the **entire** app package; rename or document that it includes all domains.
2. **Duplicate runbooks:** aggregate links from `current_state.md` → per-domain runbooks as they exist (today only CEAP has a detailed runbook under `docs/runbooks/`).
