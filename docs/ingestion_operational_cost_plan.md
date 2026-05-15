# LegisFlow ingestion — operational cost diagnosis and plan

This document summarizes how the Function App ingests data today, where cost
accrues (executions, Log Analytics traces, ADLS write operations), and what was
changed or remains to validate manually.

## 1. Functions inventory (timers vs HTTP vs queue)

| Function area | Trigger | Notes |
|---------------|---------|------|
| `ceap_api_2026_dispatcher` | Timer (`CEAP_TIMER_SCHEDULE` / `CEAP_API_2026_DISPATCH_SCHEDULE`) | Single timer; code branches Sunday=reconciliation vs daily. |
| `ceap_api_2026_worker` | Queue | One message per deputy/month (approx.). |
| `reference_snapshot_dispatcher` | Timer | One `reference_snapshot_YYYYMMDD` per reference date; short-circuits when enqueue complete. |
| `reference_snapshot_worker` | Queue | Per-endpoint snapshot work. |
| `votacoes_api_dispatcher` | Timer | Microbatch every N minutes (default 10). |
| `votacoes_api_worker` | Queue | Per roll-call (`votacao`) fanout. |
| `proposicoes_daily_dispatcher` | Timer | Daily list + hash-aware fanout. |
| `proposicoes_reconciliation_dispatcher` | Timer | Weekly reconciliation window. |
| `proposicoes_worker` | Queue | Per bill (`proposicao`) × sub-endpoint. |
| `eventos_daily_dispatcher` / `eventos_reconciliation_dispatcher` | Timer | Daily vs weekly recon. |
| `eventos_worker` | Queue | Per evento × 4 sub-endpoints. |
| `discursos_daily_dispatcher` / `discursos_reconciliation_dispatcher` | Timer | Daily JSONL deputies snapshot + hash fanout; weekly recon (prev month → today UTC). |
| `discursos_worker` | Queue | Per deputado. |
| `institucional_dispatcher` | Timer | Daily default. |
| `institucional_worker` | Queue | Per partition. |
| `daily_summary_builder` | Timer | Metadata rollup. |
| `fn_replay_*`, `fn_reset_*`, `fn_start_*_reconciliation` | HTTP | No automatic timer on replay. |

## 2. Terraform default schedules (after cost-oriented defaults)

| Variable | Default (UTC) | Intent |
|----------|-----------------|--------|
| `reference_snapshot_dispatch_schedule` | `0 0 6 * * *` | Reference/deputados-style snapshot once per day (was every 20 min). |
| `discursos_daily_dispatch_schedule` | `0 25 7 * * *` | Daily deputies list + fanout. |
| `discursos_reconciliation_dispatch_schedule` | `0 25 8 * * 0` | Sunday reconciliation window (first of prev month through today). |
| `votacoes_dispatch_schedule` | `0 */10 * * * *` | Kept microbatch for votes (`votacoes`) only. |
| `proposicoes_daily_dispatch_schedule` | `0 15 6 * * *` | Already aligned. |
| `proposicoes_reconciliation_dispatch_schedule` | `0 30 6 * * 0` | Sunday reconciliation. |
| `eventos_*` | `0 15 7…` / `0 15 8…` | Already aligned. |
| `ceap_timer_schedule` | `0 30 7 * * *` | Single timer; Sunday recon in code. |

**Manual validation:** Discursos manual reconciliation HTTP still requires explicit `date_start` / `date_end` in the JSON body; the weekly timer uses `default_discursos_reconciliation_window` (previous calendar month through today, UTC).

## 3. Queues (work)

- CEAP: `CEAP_API_QUEUE_NAME` (default `ceap-api-2026-work`)
- Reference: `REFERENCE_SNAPSHOT_QUEUE_NAME`
- Votes (`votacoes`) / bills (`proposicoes`) / events / speeches (`discursos`) / institutional: per-domain `*_QUEUE_NAME` in Terraform `main.tf`.

## 4. Ingestion state

- **Table:** `IngestionState` (partition per domain, e.g. `proposicoes_2026`).
- **Control runs:** `IngestionControlApi2026` (GenericRunRegistry / CeapRunRegistry).
- **Hash / idempotency:** Several domains use `list_item_hash` / `last_list_item_hash`
or CEAP partition rows; proposicoes daily writes list snapshot + changed JSONL under RAW.

## 5. RAW / ADLS

- **Bills (`proposicoes`) daily:** JSONL snapshot + `changed_records.jsonl` + operation manifest under list batch paths (see `shared/proposicoes_list_batch_paths.py`).
- **CEAP:** Pages per deputy/month; deputies snapshot multi-page JSON (high write count when snapshot rebuilt).
- **Workers** (proposicoes, eventos, …): still page-level JSON per API page in many paths — moving entirely to “one JSONL per run” is a larger refactor (task 7 partial: dispatcher list side already batched for proposicoes).

## 6. Logging (Log Analytics)

- App uses `log_structured` → JSON to stdout (Application Insights traces).
- **Mitigations applied:** `host.json` raises host log level to Warning, caps AI
  ingestion (`maxTelemetryItemsPerSecond`), per-queue send logs at DEBUG,
  `LOG_LEVEL` on app logger, and **queue workers** demoted routine success/skip
  lines to DEBUG so default INFO no longer emits one trace per worker execution.
- **CEAP dispatcher:** all previous `log_structured(..., "info", ...)` demoted to
  `debug` (very chatty `[BEFORE_*]` / per-page deputies logs). Operational truth
  remains in **RAW manifests** (`metadata.json`, `_SUCCESS`).

## 7. Tests

- Run `pytest tests` from repo root.
- After deploy: confirm App Insights ingestion drops; temporarily set
  `LOG_LEVEL=Debug` on the Function App only when diagnosing.

## 8. Risks / manual checks

- **`host.json` logLevel Warning:** fewer built-in host traces; Python
  `log_structured` lines may be filtered depending on category wiring. If traces
  disappear, set `Function` to `Information` locally or use `LOG_LEVEL=DEBUG` on
  the Function App while diagnosing.
- **Discursos split timers:** daily and reconciliation each acquire the same
  dispatcher lock row; avoid overlapping schedules. Reconciliation date span is
  env-independent (previous month through today UTC) on the weekly timer.
- **Reference daily:** if enqueue fails mid-day, next run is next day — monitor
  `enqueue_phase_complete` in control table.
