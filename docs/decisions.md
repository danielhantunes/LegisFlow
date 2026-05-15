# Technical Decisions

## ADR-001 - Databricks Premium SKU for Unity Catalog

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

LegisFlow MVP requires Unity Catalog-aligned capabilities, including:

- storage credentials
- external locations
- centralized governance and metadata management

These requirements drive the workspace tier decision.

### Decision

Provision Azure Databricks workspace using **Premium** SKU in the MVP.

### Consequences

- Enables Unity Catalog governance foundation from day one.
- Slightly higher cost than lower tiers, but aligned with architecture goals.
- Avoids rework/migration risk when governance requirements are formalized.

## ADR-002 - Manual Databricks Asset Management in MVP

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

Notebooks, jobs, and DLT pipelines will be iterated rapidly during MVP discovery and model stabilization.

### Decision

For MVP, Databricks notebooks, jobs, and DLT pipelines are created and adjusted **manually** inside the workspace.  
The repository does **not** include `deploy-databricks-jobs.yml` in MVP.

### Consequences

- Faster iteration while business logic is still evolving.
- Lower CI/CD complexity in early phase.
- Asset deployment automation is deferred as a future improvement after pipeline stabilization.

## Future Improvement

After notebooks/jobs stabilize, implement automated Databricks asset deployment (jobs, notebooks, DLT) through dedicated CI/CD workflows with environment promotion controls.

## ADR-003 - Single Function App and CEAP-only Function in MVP

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

LegisFlow roadmap includes multiple ingestion endpoints, but MVP must keep operational complexity low and focus on CEAP ingestion reliability.

### Decision

- Use one shared Azure Function App in MVP: `func-legisflow-ingestion-dev`.
- Implement only one function now: `ceap_expenses_ingestion_timer`.
- Do not implement future functions yet:
  - `legislative_events_ingestion_timer`
  - `voting_microbatch_timer`
  - `parliamentary_fronts_ingestion_timer`
  - `propositions_lifecycle_ingestion_timer`
- Do not create deployment workflows for future functions in MVP.
- Future endpoints must be implemented later as separate functions inside the same Function App, not inside CEAP function code.

### Consequences

- Keeps MVP deployment and operations simpler while hardening CEAP ingestion.
- Preserves scalability path by standardizing on a single Function App boundary.
- Avoids premature coupling of unrelated endpoint logic into the CEAP function.

## ADR-004 - No GitHub Variables in MVP Workflows

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

The MVP uses a single dev environment with stable resource names. Requiring GitHub Variables for non-sensitive values increases initial setup friction without clear benefit at this stage.

### Decision

- Use only GitHub Secrets in MVP workflows:
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_SUBSCRIPTION_ID`
- Keep non-sensitive backend and Function App values fixed in workflows for MVP.
- Reintroduce variables/inputs in a future multi-environment phase.

### Consequences

- Faster onboarding and simpler CI/CD setup.
- Lower risk of misconfigured repository variables.
- Less flexibility for resource renaming until multi-environment hardening phase.

## ADR-005 - CEAP API 2026 ingestion via queue and unit control table

- **Date**: 2026-05-03
- **Status**: Accepted

### Context

CEAP despesas per deputy can paginate heavily. A single timer execution that processes all deputies and months risks timeouts and poor fault isolation.

### Decision

- Use a **timer dispatcher** that enqueues bounded batches of work messages (`deputado` + `ano=2026` + `mes`).
- Use a **queue-triggered worker** for each unit, with **HTTP retry + queue retry**, checkpoints per page in a dedicated **Table Storage** control plane (`IngestionControlApi2026`), and **deterministic Raw paths** for idempotent replay.
- Keep the **legacy monolithic timer** in the codebase but **disabled by default** for emergency fallback only.

### Consequences

- Higher operational clarity (poison queue, replay HTTP function, runbook).
- Slightly more moving parts (queues, extra functions) than a single timer.
- Bronze/Silver must still deduplicate semantically; documented separately from Raw immutability policy.

## ADR-006 - CEAP API 2026 dispatcher dual mode (daily window + reconciliation) and partition state

- **Date**: 2026-05-04
- **Status**: Accepted

### Context

CEAP 2026 ingestion needs frequent updates (moving window) without reprocessing the full year every day, and also a broad, idempotent pass per reporting period (monthly reconciliation). The same Function App should not gain a dedicated function only for reconciliation.

### Decision

- A single `ceap_api_2026_dispatcher` timer selects mode by **UTC date** (`CEAP_RECONCILIATION_DAY`): on that day only **reconciliation**; otherwise **daily** (current month + prior months per `CEAP_DAILY_LOOKBACK_MONTHS`), always excluding future months and honoring `CEAP_TARGET_YEAR`.
- Each tick splits into **Phase A (deputy snapshot)** and **Phase B (CEAP enqueue)**. Phase A reuses the daily `/deputados` snapshot when `IngestionControlApi2026._snapshots/deputados_YYYYMMDD` is `COMPLETED` and `_SUCCESS` exists in Raw; it calls `/deputados` only when it must create or recreate the snapshot. In `reconciliation` mode there is fallback to the latest complete snapshot.
- **Pipeline run** is recorded in `IngestionControlApi2026` (`_runs`, `RowKey` = `pipeline_run_id`), with enqueue phase capped by `CEAP_MAX_TASKS_PER_DISPATCH` (default 1000), enqueue completion when the deputy list is fully walked or after consecutive idle ticks, and a **lock** at `_locks/ceap_dispatcher_lock` (TTL 15 minutes).
- Per-partition state in **IngestionState** (`ceap_2026`), including `current_pipeline_run_id` and `mode`; queue idempotency for the same `pipeline_run_id` when status is already QUEUED/RUNNING/SUCCESS.
- A single **worker** reads `mode` from the message; Raw paths include `pipeline_run_id` and `execution_id` so blobs are not overwritten across runs.
- **HTTP replay** remains only for failures (FAILED/POISON), reading IngestionState; it is not the automatic reconciliation mechanism.
- Future months are **never** enqueued.

### Consequences

- Predictable operation: the same day, once the run is COMPLETED, extra ticks only log and do not duplicate messages.
- Heavy reconciliation is spread across many timer executions without moving API work into the dispatcher.
- Downstream (Databricks Bronze/Silver/Gold) stays out of this ADR; analytical deduplication follows existing documented policies.
