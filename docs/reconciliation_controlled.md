# Controlled reconciliation (checkpointed)

## Diagnosis (legacy)

| Component | Schedule (example) | Behaviour |
|-----------|-------------------|-----------|
| `proposicoes_reconciliation_dispatcher` | Weekly timer | Single call to `execute_proposicoes_reconciliation_tick` (multi-page resume via `recon_list_next_page`, but one timer fire per week). |
| `eventos_reconciliation_dispatcher` / `discursos_reconciliation_dispatcher` | Weekly | Same pattern: one timer invocation runs one bounded tick. |
| `votacoes_api_dispatcher` | Every 10 min | Reconciliation mode on `VOTACOES_RECONCILIATION_DAY` with resume fields in registry. |
| `ceap_api_2026_dispatcher` | Daily | Sunday reconciliation vs daily; enqueue capped per tick; gaps if not resumed mid-week (see prior analysis). |
| HTTP `fn_start_*_reconciliation` | Manual | One-shot tick per request. |

State and metadata today: **`GenericRunRegistry`** / **`CeapRunRegistry`** on **`IngestionControlApi2026`** (`_runs_*` partitions), **`GenericPartitionStateStore`** on **`IngestionState`**, RAW manifests under each domain’s path conventions.

## New model: `ReconciliationControl`

Physical table: same **`IngestionControlApi2026`** (configurable via `INGESTION_CONTROL_TABLE`).

- **PartitionKey:** `reco_<domain>` (e.g. `reco_proposicoes`).
- **RowKey:** `control_id` (UUID).
- **Key fields:** `status` (`PENDING` \| `RUNNING` \| `PAUSED` \| `COMPLETED` \| `FAILED` \| `CANCELLED` \| `LIMIT_REACHED`), `pipeline_run_id`, `window_start`, `window_end`, `max_tasks_per_run`, `max_runtime_minutes`, `checkpoint_json`, aggregate counters, `last_batch_status`.

Controlled **`proposicoes_recoctl_<16 hex>`** pipeline ids are registered on **`PROPOSICOES_DOMAIN.pipeline_run_id_prefixes`** and allowed by `is_well_formed_pipeline_run_id`.

## Runtime components

1. **Starter** — Weekly `proposicoes_reconciliation_dispatcher` when `PROPOSICOES_USE_CONTROLLED_RECONCILIATION=true` only calls `start_proposicoes_controlled_reconciliation` (creates `RUNNING` control). Legacy behaviour when the flag is false.
2. **Scheduler** — `reconciliation_scheduler` timer (`RECONCILIATION_SCHEDULER_SCHEDULE`, default `0 */20 * * * *`), gated by `ENABLE_RECONCILIATION_SCHEDULER`. Processes **one** `RUNNING` control per domain per tick via `run_proposicoes_controlled_batch` (calls existing tick with `pipeline_run_id` + `max_messages_per_tick_override`).
3. **HTTP** — `fn_reconciliation_control_http` route `api/legisflow/reconciliation/control`, `authLevel=function`, flag `ENABLE_RECONCILIATION_CONTROL_HTTP`. JSON `action`: `start`, `status`, `pause`, `resume`, `cancel`, `run_once`.

## Feature flags (local.settings example)

- `ENABLE_RECONCILIATION_SCHEDULER` — `true` to run the 20-minute driver.
- `ENABLE_RECONCILIATION_CONTROL_HTTP` — `true` for the HTTP control plane.
- `PROPOSICOES_USE_CONTROLLED_RECONCILIATION` — `true` so the weekly timer only **starts** a control row (batches go to the scheduler).
- `PROPOSICOES_RECON_SCHEDULER_MAX_TASKS` / `PROPOSICOES_RECON_SCHEDULER_MAX_RUNTIME_MIN` — starter defaults (500 / 9).

## Next steps (not implemented in this pass)

- **CEAP / eventos / discursos / votacoes:** extend `SCHEDULER_DOMAINS` and add domain-specific batch modules mirroring `reconciliation_proposicoes_controlled.py`.
- **Dedicated physical table** `ReconciliationControl` if partition noise on `IngestionControlApi2026` becomes an issue.
- **LIMIT_REACHED** as a first-class control status when cumulative caps are exceeded (currently batch-level `LIMIT_REACHED` only on max-runtime guard).

## Example HTTP

```http
POST /api/legisflow/reconciliation/control?code=...
Content-Type: application/json

{"action":"start","domain":"proposicoes","target_year":2026,"date_start":"2026-04-01","date_end":"2026-05-12","max_tasks_per_run":300,"max_runtime_minutes":9,"dry_run":false}
```

```http
GET /api/legisflow/reconciliation/control?action=status&domain=proposicoes&control_id=<uuid>&code=...
```
