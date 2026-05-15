# Azure Function App refactoring plan (LegisFlow)

**Goal:** Reduce **function count**, **operational complexity**, **cost** (executions, App Insights, Storage/ADLS writes), and **duplicate queue messages**, while keeping **dispatcher → worker → poison → replay**, with **checkpoints**, **IngestionControl**, **IngestionState**, and **payload_hash**.

This document completes **phase 1** (diagnosis + design). Implementation must be **incremental**, behind feature flags, with a temporary overlap with legacy functions.

---

## 1. Current function inventory (~50 entries)

Each folder containing `function.json` under `functions/ceap_expenses_ingestion_timer/` is one Function in the App (unique per folder name plus root app).

| # | Folder | Trigger (summary) |
|---|--------|-------------------|
| 1 | Root `function.json` | `timerTrigger` (`CEAP_TIMER_SCHEDULE`) — legacy CEAP monolith (often `AzureWebJobs.*.Disabled`) |
| 2 | `ceap_api_2026_dispatcher` | timer |
| 3 | `ceap_api_2026_worker` | queue |
| 4 | `ceap_api_2026_poison_handler` | queue (poison) |
| 5 | `daily_summary_builder` | timer |
| 6 | `discursos_daily_dispatcher` | timer |
| 7 | `discursos_dispatcher` | timer (legacy microbatch) |
| 8 | `discursos_reconciliation_dispatcher` | timer |
| 9 | `discursos_worker` | queue |
| 10 | `discursos_poison_handler` | queue |
| 11 | `eventos_daily_dispatcher` | timer |
| 12 | `eventos_dispatcher` | timer |
| 13 | `eventos_reconciliation_dispatcher` | timer |
| 14 | `eventos_worker` | queue |
| 15 | `eventos_poison_handler` | queue |
| 16 | `institucional_dispatcher` | timer |
| 17 | `institucional_worker` | queue |
| 18 | `institucional_poison_handler` | queue |
| 19 | `proposicoes_daily_dispatcher` | timer |
| 20 | `proposicoes_dispatcher` | timer |
| 21 | `proposicoes_reconciliation_dispatcher` | timer |
| 22 | `proposicoes_worker` | queue |
| 23 | `proposicoes_poison_handler` | queue |
| 24 | `reference_snapshot_dispatcher` | timer |
| 25 | `reference_snapshot_worker` | queue |
| 26 | `reference_snapshot_poison_handler` | queue |
| 27 | `reconciliation_scheduler` | timer (controlled reconciliation — proposicoes) |
| 28 | `votacoes_api_dispatcher` | timer |
| 29 | `votacoes_api_worker` | queue |
| 30 | `votacoes_api_poison_handler` | queue |
| 31 | `fn_current_year_backfill_dispatcher` | http |
| 32 | `fn_reconciliation_control_http` | http |
| 33 | `fn_replay_ceap_failed_messages` | http |
| 34 | `fn_replay_discursos_failed_messages` | http |
| 35 | `fn_replay_eventos_failed_messages` | http |
| 36 | `fn_replay_institucional_failed_messages` | http |
| 37 | `fn_replay_proposicoes_failed_messages` | http |
| 38 | `fn_replay_reference_failed_messages` | http |
| 39 | `fn_replay_votacoes_failed_messages` | http |
| 40 | `fn_reset_ceap_pipeline_run` | http |
| 41 | `fn_reset_discursos_pipeline_run` | http |
| 42 | `fn_reset_eventos_pipeline_run` | http |
| 43 | `fn_reset_institucional_pipeline_run` | http |
| 44 | `fn_reset_proposicoes_pipeline_run` | http |
| 45 | `fn_reset_reference_pipeline_run` | http |
| 46 | `fn_reset_votacoes_pipeline_run` | http |
| 47 | `fn_start_discursos_reconciliation` | http |
| 48 | `fn_start_eventos_reconciliation` | http |
| 49 | `fn_start_proposicoes_reconciliation` | http |
| 50 | `fn_start_votacoes_reconciliation` | http |

**Note:** “Almost 30” in informal discussion reflects cognitive grouping; the repo has **~50** actual Function entries.

---

## 2. Grouping by type

### Timers (dispatchers / starters / utilities)

- CEAP: `ceap_api_2026_dispatcher`, root `function.json` (legacy)
- Proposicoes: `proposicoes_daily_dispatcher`, `proposicoes_dispatcher`, `proposicoes_reconciliation_dispatcher`
- Eventos: `eventos_daily_dispatcher`, `eventos_dispatcher`, `eventos_reconciliation_dispatcher`
- Discursos: `discursos_daily_dispatcher`, `discursos_dispatcher`, `discursos_reconciliation_dispatcher`
- Institucional: `institucional_dispatcher`
- Reference: `reference_snapshot_dispatcher`
- Votacoes: `votacoes_api_dispatcher`
- Control: `reconciliation_scheduler`, `daily_summary_builder`

### Workers (queue)

- `ceap_api_2026_worker`, `proposicoes_worker`, `eventos_worker`, `discursos_worker`, `institucional_worker`, `reference_snapshot_worker`, `votacoes_api_worker`

### Poison handlers (queue)

- One per legacy domain/queue (CEAP, proposicoes, eventos, discursos, institucional, reference, votacoes)

### HTTP — replay

- `fn_replay_*` (7 functions)

### HTTP — reset

- `fn_reset_*` (7 functions)

### HTTP — start reconciliation / backfill

- `fn_start_*_reconciliation` (4), `fn_current_year_backfill_dispatcher`, `fn_reconciliation_control_http`

---

## 3. Keep / replace / remove (after validated migration)

### Keep (concept)

- **Mental model** dispatcher → worker → poison → replay.
- **Tables** `IngestionState` + queues (names may be consolidated).
- **Special domain** votacoes with a higher-frequency timer.

### Replace (target)

| Current (many functions) | Target |
|--------------------------|--------|
| Multiple `*_dispatcher` + `*_reconciliation_dispatcher` + duplicate HTTP starters | `daily_starter_timer` + `generic_dispatcher_timer` |
| 7 `*_worker` | `generic_worker_queue` (single binding; internal routing) |
| 7 `*_poison_handler` | `generic_poison_handler` |
| 7 `fn_replay_*` | `http_replay_failed` |
| Multiple HTTP reset/reconciliation/backfill | `http_control_ingestion` + `http_start_ingestion` (policy: reset operator-only) |
| `reconciliation_scheduler` + `fn_reconciliation_control_http` (partial state) | Fold into `generic_dispatcher_timer` + `http_control_ingestion` when unified `IngestionControl` is ready |

### Remove (only after validation + overlap period)

- Duplicate per-domain dispatchers (`proposicoes_dispatcher` vs `proposicoes_daily_dispatcher`, etc.).
- Redundant HTTP `fn_start_*` once `http_start_ingestion` covers them.
- Per-domain `fn_replay_*` / `fn_reset_*` (replaced by generic endpoints with `domain` + RBAC).
- Root CEAP monolith `function.json` if disabled in all environments.

---

## 4. Target architecture (hybrid)

### Generic (batch / incremental / reconciliation / backfill by control row)

1. **`daily_starter_timer`** — once per day; only creates `IngestionControl` rows `PENDING` with window + initial checkpoint + `next_run_at`.
2. **`generic_dispatcher_timer`** — every N minutes; for each eligible control (`PENDING`/`RUNNING`, `next_run_at <= now`, not `PAUSED`): **one batch** (`max_tasks_per_run`, `max_runtime_minutes`), idempotency **before** enqueue, update checkpoint and manifest.
3. **`generic_worker_queue`** — consumes message (single queue or few queues by SLA); routes to domain handler; revalidates `IngestionState`; skips duplicate RAW for same hash; updates control counters when applicable.
4. **`generic_poison_handler`** — single poison queue; persists structured failure (no full payload in logs).
5. **`http_start_ingestion`** — on-demand daily / reconciliation / current_year_backfill.
6. **`http_get_ingestion_status`** — read `IngestionControl` (+ optional `IngestionState` aggregates).
7. **`http_control_ingestion`** — pause / resume / cancel / controlled reset.
8. **`http_replay_failed`** — replay with `max_tasks`, `dry_run`, `domain` / `control_id`.

### Domain-specific

9. **`votacoes_microbatch_dispatcher`** — keeps 10-minute cadence, short window, **no** full-year scan; wide votacoes reconciliation/backfill can be an `IngestionControl` row processed by `generic_dispatcher_timer`.

**Target total:** **9** functions + optionally `daily_summary_builder` (separate timer, low cost) → **10** if daily summary is kept.

---

## 5. Unified `IngestionControl` model

**Physical table:** may remain `IngestionControlApi2026` or a dedicated table. Recommendation: **separate partition** or stable **RowKey prefix** (`ingestion|...`) to avoid colliding with existing `_runs_*` rows until migration.

**Suggested key**

- `PartitionKey`: fixed `ingestion_control` *or* `ingestion_control|<domain>`.
- `RowKey`: canonical `control_id`, e.g. `proposicoes|daily|2026-05-13` (normalized).

**Minimum fields** (aligned with operations JSON + your spec)

- Identity: `control_id`, `domain`, `mode` (`daily` \| `reconciliation` \| `current_year_backfill` \| …)
- State: `status`, `force`, `dry_run`
- Window: `window_start`, `window_end`, optional `year`
- Scheduling: `next_run_at`, `interval_minutes`
- Limits: `max_tasks_per_run`, `max_runtime_minutes`
- Checkpoint: `checkpoint_type` + sparse fields (`checkpoint_date`, `checkpoint_page`, `checkpoint_id`, …) **or** `checkpoint_json` (more flexible during migration)
- Metrics: `total_seen`, `total_enqueued`, `total_skipped_same_hash`, `total_failed`, `total_completed_workers`
- Timestamps: `started_at`, `updated_at`, `completed_at`
- Link: current batch `pipeline_run_id` (may change per batch or use base id + `batch_seq`)

**Automatic processing rules**

- Only `PENDING` / `RUNNING`.
- `next_run_at` enforces cadence even if the timer fires more often (avoids hammering API/queue).
- `COMPLETED` does not reopen without `force` or explicit HTTP `reset`.

---

## 6. `IngestionState` model (idempotency)

The repo already uses per-domain partitions with `PartitionKey` = domain partition and `RowKey` = endpoint + id. The refactor should **formalize**:

- `last_list_item_hash` / `last_payload_hash` (name alignment)
- `status` (`QUEUED`, `RUNNING`, `SUCCESS`, `FAILURE`, …)
- `current_pipeline_run_id` / `last_pipeline_run_id`
- `last_raw_path` (optional)

**Cost rule:** dispatcher and worker both apply “skip if `SUCCESS` + same hash” unless `force_reprocess`.

---

## 7. Checkpoint strategy by domain

| Domain | `checkpoint_type` | Notes |
|--------|-------------------|--------|
| proposicoes | `date_page` | API `dataInicio`/`dataFim` (today aligned to procedural tracking window; if switching to `dataApresentacao`, document as breaking) |
| eventos | `date_page` | UTC window + `/eventos` list page |
| discursos | `date_page` or `deputy_date` | Deputy fanout + window |
| ceap | `year_month_deputy_id` | Iterate deputy × month with existing CEAP table cursor where possible |
| deputados | `page` or `deputado_id` | If “deputados” is active legislature list, align with `reference`/deputies endpoint |
| reference / institucional | `snapshot` | Parent endpoint + pages + fanout (today institucional vs reference snapshot — decide unified `reference` + `institucional` modes) |
| votacoes (microbatch) | `offset_id` | Separate `last_seen` / `last_processed` (already in current code patterns) |
| votacoes (wide recon/backfill) | `date_id` | Generic control; microbatch stays separate |

---

## 8. Queues and messages

**Option A (migration-friendly):** keep per-domain queues — **but** a single `generic_worker_queue` cannot bind multiple queues in one Azure Functions Python function; typical pattern is **one** `camara-ingestion-work` queue + `domain` in payload, **or** keep N queues and N triggers (**does not** reduce function count).

**Option B (real reduction target):** **one queue** + **one** `queueTrigger`; worker deserializes and routes. Poison: **one** `camara-ingestion-work-poison`.

**Compat:** during migration, the generic worker can **delegate** to existing handlers until producers move.

---

## 9. RAW / storage — cost control

- **Per-batch** manifest (aligned with `changed_records.jsonl` + `manifest.json`).
- Avoid per-item `write_json` at high volume when batch JSONL is enough.
- Dispatcher: do not emit full list bodies to traces/logs; persist audited RAW (existing `_audit` pattern).

---

## 10. Logs / Application Insights

- `host.json`: keep restrictive `logLevel`; review `excludedTypes` / sampling if needed.
- Code policy: **no** `logging.info` in hot loops; **one** structured log per batch (`control_id`, counts, duration).
- Per-entity detail only on `warning`/`error` with truncation.

---

## 11. Migration risks

1. **Big-bang single queue** breaks old consumers — mitigate with feature flag and dual-write period.
2. **RowKey collision** between current `_runs_*` and new `IngestionControl` — separate partition/namespace.
3. **`pipeline_run_id` semantics** (reset/replay/ownership) — update `domain_catalog` and reset helpers.
4. **Terraform / app settings** — reduce CRON env vars via YAML/JSON in blob **or** keep minimal env.
5. **Timeouts** — `generic_dispatcher_timer` must be strictly small batches; continuation via `next_run_at`.
6. **Votacoes alerts** — if present, ensure idempotent alert key `idVotacao|type`.

---

## 12. Files / areas to touch (high level)

- `functions/ceap_expenses_ingestion_timer/**/__init__.py` — major folder restructure (add/remove).
- `shared/domain_catalog.py` — unified queues, new prefixes, routing.
- `shared/run_registry.py`, `shared/generic_partition_state.py`, new `shared/ingestion_control*.py`.
- `shared/queue_messages.py`, `shared/queue_helpers.py`.
- Existing per-domain workers — extract to `handlers/*.py` invoked by generic worker.
- `host.json`, `local.settings.json.example`, `infra/terraform/ingestion/*.tf`.
- `tests/**` — new suite for dispatcher rules (`next_run_at`, `max_tasks`, hash).

---

## 13. Incremental execution plan

1. Central config (YAML/JSON + loader + validation).
2. `IngestionControl` store + read path (do not delete legacy yet).
3. `generic_dispatcher_timer` **read-only** + internal dry-run.
4. `generic_worker_queue` behind flag, pilot domain (**proposicoes**).
5. Generic poison/replay.
6. Generic HTTP (start/status/control/replay).
7. Isolated `votacoes_microbatch_dispatcher`; move wide recon to generic control.
8. Migrate eventos, discursos, CEAP, institucional/reference, deputados.
9. Disable old functions (`AzureWebJobs.<name>.Disabled`) for 1–2 weeks before deleting folders.

---

## 14. “Final rules” compliance

The design explicitly avoids: per-domain dispatchers in the final target (except votacoes microbatch), enqueue without idempotency, finishing large windows in one tick, full payload logging, unlimited replay, and drops traceability/manifest/hash rules.

---

## 15. Recommended next step

Approve this plan and choose **single queue vs per-domain queues** for the generic worker. Then open a PR for phase 1: **config + IngestionControl + tests** only — no function removal yet.
