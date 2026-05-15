# Runbook — CEAP API ingestion failure (2026 pipeline)

## Purpose

Restore resilient CEAP expense ingestion via the Chamber of Deputies Open Data API for the year set in **`CEAP_TARGET_YEAR`** (default **2026**), with granular recovery per deputy and month, without reloading history 2019–2025 (that period uses static files).

## Scope

- **In scope**: CEAP `/despesas` endpoint per `id_deputado`, `ano`, `mes`; pagination; writes to ADLS Raw; per-partition state in **IngestionState**; aggregate runs and dispatcher lock in **IngestionControlApi2026**; work and poison queues.
- **Out of scope**: automatic Bronze/Silver in this runbook (see deduplication in `docs/pipelines/ceap_deduplication_bronze_silver.md`).

## Dispatcher modes overview

The dispatcher runs in **two phases per tick**:

1. **Phase A — deputy snapshot**: ensures a valid copy of `/deputados`. Before calling the API it checks `IngestionControlApi2026._snapshots/deputados_YYYYMMDD` and the `_SUCCESS` marker in Raw. If the snapshot for the current `reference_date` is already `COMPLETED` (and valid), **it reuses it in memory without calling the API**. In `reconciliation` mode, if the current `reference_date` has no complete snapshot yet, it falls back to the latest `COMPLETED` snapshot. Otherwise it calls paginated `GET /deputados`, persists each page to Raw, writes `metadata.json` + `_SUCCESS` at the end, and updates `_snapshots`. This avoids calling `/deputados` every 20 minutes.
2. **Phase B — CEAP enqueue**: walks the in-memory deputy list in chunks of 100, builds CEAP messages per deputy × month, respects **`CEAP_MAX_TASKS_PER_DISPATCH`** (default 1000), and updates **IngestionState** per partition.

The **`ceap_api_2026_dispatcher`** timer (schedule **`CEAP_API_2026_DISPATCH_SCHEDULE`**, default every 20 minutes UTC) picks the mode from the **UTC date**:

| Condition | Mode | Months enqueued | `pipeline_run_id` (example) |
|-----------|------|-----------------|-----------------------------|
| Day ≠ `CEAP_RECONCILIATION_DAY` (default **25**) | **daily** | Current month + earlier months in the window (`CEAP_DAILY_LOOKBACK_MONTHS`), no future months | `ceap_daily_YYYYMMDD` |
| Day = `CEAP_RECONCILIATION_DAY` | **reconciliation** | From `CEAP_RECONCILIATION_START_MONTH` through the current month of the target year | `ceap_reconciliation_YYYYMMDD` |

On reconciliation day the **daily** flow does **not** run; only reconciliation. After that day’s run is **COMPLETED** (and, when there are tasks, workers have finished counters), later timer invocations the same day only log and **do not** enqueue again.

The dispatcher uses a **lock** on the control table (`PartitionKey=_locks`, `RowKey=ceap_dispatcher_lock`, TTL ~15 minutes) to avoid concurrent runs.

## Main components

| Component | Role |
|-----------|------|
| `ceap_api_2026_dispatcher` | Timer: ensures daily `/deputados` snapshot in Raw + `IngestionControlApi2026._snapshots` (reuses when `COMPLETED`); then enqueues up to **`CEAP_MAX_TASKS_PER_DISPATCH`** messages per invocation (fallback: `CEAP_DISPATCH_MAX_MESSAGES`). Updates runs in `_runs` and partition state in **IngestionState**. |
| `ceap_api_2026_worker` | Queue trigger: one message = deputy + year + month + `mode` + `pipeline_run_id`; pagination; checkpoint in **IngestionState**; writes Raw under a per-run path. |
| `ceap_api_2026_poison_handler` | Queue trigger on poison queue: sets partition **`POISON`** in **IngestionState** and increments failures on the automated run when applicable. |
| `fn_replay_ceap_failed_messages` | HTTP (`authLevel=function`, route `replay/ceap-api-2026`): re-enqueues partitions from **IngestionState** (not the legacy unit table). |
| `IngestionState` | Partition **`ceap_2026`**; `RowKey` = `despesas|{id_deputado}|{ano}|{mes}`. Typical states: `QUEUED`, `RUNNING`, `SUCCESS`, `FAILED`, `POISON`, etc. |
| `IngestionControlApi2026` | **`_runs`**: control per `pipeline_run_id` (counters, dispatcher cursors, `enqueue_phase_complete`). **`_locks`**: dispatcher lock. |
| Queues `ceap-api-2026-work` and `ceap-api-2026-work-poison` | Work and persistent failures after retries (`host.json` / `maxDequeueCount`). |
| `AzureWebJobsStorage` | Primary Function App connection string; used by **queue triggers** (`ceap_api_2026_worker` and `ceap_api_2026_poison_handler`). |
| `CEAP_QUEUE_STORAGE` | Optional for queue clients in code (dispatcher/replay); trigger listeners use `AzureWebJobsStorage`. |
| Application Insights | Structured JSON logs (`execution_id`, `pipeline_run_id`, `mode`, `id_deputado`, `mes`, `raw_path`, etc.). |

### Relevant app settings (summary)

| Setting | Purpose |
|---------|---------|
| `CEAP_TARGET_YEAR` | CEAP API year (default 2026). |
| `CEAP_API_2026_DISPATCH_SCHEDULE` | Dispatcher CRON (default `0 */20 * * * *`). |
| `CEAP_RECONCILIATION_DAY` | UTC day for monthly reconciliation (default 25). |
| `CEAP_DAILY_LOOKBACK_MONTHS` | How many past months to include in the daily window (default 1). |
| `CEAP_STALE_AFTER_MINUTES` | Window to treat `QUEUED`/`RUNNING` as stale (default 60; use 5 in dev for quick tests). |
| `CEAP_REFERENCE_TIMEZONE` | Time zone used only for the `reference_date` segment of Raw deputy blobs (default `America/Sao_Paulo`). Daily/reconciliation mode still follows **UTC date**. |
| `CEAP_RECONCILIATION_START_MONTH` | First month in reconciliation (default 1). |
| `CEAP_MAX_TASKS_PER_DISPATCH` | Max CEAP expense messages enqueued per dispatcher run (default 1000). Does not affect deputy snapshot collection. |
| `CEAP_API_QUEUE_NAME` / `CEAP_API_POISON_QUEUE_NAME` | Queue names. |
| `INGESTION_STATE_TABLE` / `INGESTION_CONTROL_TABLE` | Table names if overridden in the environment. |
| `CEAP_REPROCESS_QUEUE` | If `true`, worker may reprocess a partition already `SUCCESS` for the same `pipeline_run_id` (rare). |

## Failure symptoms

- Poison queue growth or messages stuck on the main queue for long periods.
- **IngestionState** partitions stuck in stable **`FAILED`**, **`POISON`**, or **`RUNNING`** without progress (for example after an incident).
- Gaps in Raw under the prefix below (varies by `pipeline_run_id` and `execution_id`).
- Recurring 429/5xx in logs; 401/403 indicate identity/RBAC on ADLS.

## Raw path (ADLS)

The worker writes files in this layout:

```text
raw/camara/ceap/api/despesas/reference_year={ano}/reference_month={MM}/
  pipeline_run_id={pipeline_run_id}/execution_id={execution_id}/deputado_id={id}/page_{n}.json
```

Each function invocation gets a new **`execution_id`**; repeating the same period in another run produces distinct prefixes (no overwrite of another run’s blobs).

The dispatcher also writes snapshots of the deputy list (dispatch source and future dimension base):

```text
raw/camara/deputados/api/list/reference_date={YYYY-MM-DD}/
  pipeline_run_id={pipeline_run_id}/execution_id={snapshot_execution_id}/page_{n}.json
  metadata.json
  _SUCCESS                       # only when the snapshot is complete
```

`{YYYY-MM-DD}` is the **civil date** in `CEAP_REFERENCE_TIMEZONE` (default `America/Sao_Paulo`), not UTC — folders align with the Brazilian calendar. `snapshot_execution_id` is stable within the same `pipeline_run_id` (all pages of the same run go under the same `execution_id=...` subfolder even when pagination spans multiple ticks).

### Deputy snapshot validation

Each tick the dispatcher writes/updates `metadata.json` at `reference_date={YYYY-MM-DD}/` with current state. When `/deputados` pagination ends (empty response), besides `metadata.json` with `status=COMPLETED`, a `_SUCCESS` marker is created in the same folder.

`metadata.json` fields:

| Field | Description |
|-------|-------------|
| `endpoint` | Always `deputados`. |
| `reference_date` | Civil date (`CEAP_REFERENCE_TIMEZONE`) used in the folder. |
| `reference_timezone` | Time zone used to derive `reference_date`. |
| `pipeline_run_id` | Run that produced this copy. |
| `execution_id` | `snapshot_execution_id` (`execution_id=...` subfolder). |
| `status` | `IN_PROGRESS` or `COMPLETED`. |
| `total_pages` / `record_count` | Snapshot totals. |
| `files_written` | Equals `total_pages` when the snapshot is consistent. |
| `started_at` / `completed_at` | Timestamps (ISO UTC). |
| `error_message` | Empty when there is no failure. |

A snapshot is **valid for use** only when **all** of the following hold:

- `_SUCCESS` exists under `reference_date={YYYY-MM-DD}/`;
- `metadata.status = COMPLETED`;
- `record_count > 0`;
- `total_pages > 0`;
- `files_written == total_pages`.

If the current date’s folder fails any criterion, the dispatcher **does not** use it as reference: it logs a warning and picks the latest `reference_date=...` folder that satisfies the rules as **fallback**. The run row in `IngestionControlApi2026._runs` records the choice:

| Run field | Meaning |
|-----------|---------|
| `deputies_pages_written` / `deputies_records_count` | Accumulated writes for today’s snapshot (even if still incomplete). |
| `deputies_snapshot_status` | `IN_PROGRESS` while paginating; `COMPLETED` after `_SUCCESS`. |
| `deputies_snapshot_first_execution_id` | `snapshot_execution_id` of the snapshot being built. |
| `deputies_snapshot_date` / `deputies_snapshot_path` / `deputies_snapshot_record_count` | Point to the **valid** snapshot the run uses (current when complete, or fallback). |
| `deputies_snapshot_source` | `current_run`, `fallback_completed`, or `none`. |
| `deputies_snapshot_completed_at` | When `_SUCCESS` was written. |

## Logs (Application Insights)

1. Azure Portal → Function App (e.g. `func-legisflow-ingestion-dev`) → Application Insights → **Logs**.
2. Search `traces` with JSON: `pipeline_run_id`, `mode`, `execution_id`, `id_deputado`, `ano`, `mes`, `final_status`, `raw_path`, `pages_processed`, `record_count`.
3. Filter by function name (`ceap_api_2026_worker`, `ceap_api_2026_dispatcher`, `ceap_api_2026_poison_handler`).

## IngestionState (partition per deputy/month)

- **Function App** storage account → **Tables** → `IngestionState` (or `INGESTION_STATE_TABLE`).
- **`PartitionKey`**: `ceap_2026`.
- **`RowKey`**: `despesas|{id_deputado}|{ano}|{mes}` (pipe separator).
- Useful fields: `status`, `current_pipeline_run_id`, `last_mode`, `last_successful_page`, `last_error`, `last_dispatched_at`, `last_finished_at`, `record_count`, `raw_path`.

### Finding failures

Filter `status` in **`FAILED`**, **`POISON`**, or inspect stale **`RUNNING`** (`last_started_at` / `updated_at`).

## IngestionControlApi2026 (runs and lock)

- Same storage → table `IngestionControlApi2026`.
- **`PartitionKey=_runs`**, **`RowKey`**: `ceap_daily_YYYYMMDD` or `ceap_reconciliation_YYYYMMDD` — consolidated run summary.
  - Fields: `run_type`, `status`, `target_year`, `months_to_process`, `enqueue_phase_complete`, `total_tasks_expected`, `total_tasks_queued`, `total_tasks_success`, `total_tasks_failed`, `total_tasks_running`, `total_tasks_pending`, `total_tasks_poison`, `started_at`, `updated_at`, `completed_at`, `last_error`, cursors `next_pagina`, `next_idx`, `next_month_idx`.
  - Deputy snapshot fields: `deputies_pages_written`, `deputies_records_count`, `deputies_snapshot_status`, `deputies_snapshot_first_execution_id`, `deputies_snapshot_date`, `deputies_snapshot_path`, `deputies_snapshot_record_count`, `deputies_snapshot_source` (`reused_today` | `reused_fallback` | `created_today` | `none`), `deputies_snapshot_completed_at`.
  - Possible statuses: `STARTED`, `QUEUING`, `QUEUED`, `RUNNING`, `COMPLETED`, `PARTIAL`, `FAILED`.
  - **Confirmed success** only when: `status=COMPLETED`, `total_tasks_success == total_tasks_expected`, `total_tasks_failed=0`, `total_tasks_poison=0`, `total_tasks_running=0`, `total_tasks_pending=0`.
- **`PartitionKey=_locks`**, **`RowKey=ceap_dispatcher_lock`** — active dispatcher lock (`locked_until`, `locked_by`).
- **`PartitionKey=_snapshots`**, **`RowKey=deputados_YYYYMMDD`** — daily `/deputados` snapshot control (independent of the CEAP expense run).
  - Fields: `endpoint=deputados`, `reference_date`, `status` (`IN_PROGRESS` | `COMPLETED` | `FAILED`), `pipeline_run_id`, `execution_id`, `started_at`, `completed_at`, `total_pages`, `record_count`, `raw_path`, `last_error`, `updated_at`.
  - The dispatcher **reuses** a snapshot only when: `status=COMPLETED`, `record_count>0`, `total_pages>0`, `raw_path` set **and** `_SUCCESS` exists at `raw_path/_SUCCESS`.
  - **`daily`** mode reuses today’s snapshot; if missing, creates a new one.
  - **`reconciliation`** mode reuses the latest complete snapshot (`reference_date < today`) when today’s is not complete yet; if none is valid, creates a new one before Phase B.

### One-command completion check (daily)

```bash
az storage entity query \
  --account-name <storage_account> \
  --table-name IngestionControlApi2026 \
  --auth-mode key \
  --filter "PartitionKey eq '_runs' and RowKey eq 'ceap_daily_YYYYMMDD'" \
  -o table
```

### Reconciliation

```bash
az storage entity query \
  --account-name <storage_account> \
  --table-name IngestionControlApi2026 \
  --auth-mode key \
  --filter "PartitionKey eq '_runs' and RowKey eq 'ceap_reconciliation_YYYYMMDD'" \
  -o table
```

## Poison queue

- Function storage → **Queues** → `ceap-api-2026-work-poison` (or `CEAP_API_POISON_QUEUE_NAME`).
- Each message is a partition that failed after exhausting main-queue retries.
- Handler **`ceap_api_2026_poison_handler`** sets **IngestionState** to **`POISON`** (and the automated run when `pipeline_run_id` is daily/reconciliation).

## Automatic dispatcher vs failing partitions

- For the **same** `pipeline_run_id`, the dispatcher **does not** duplicate messages if the partition is already **`QUEUED`**, **`RUNNING`**, or **`SUCCESS`**.
- **`FAILED`** or **`POISON`** partitions can be picked up by a **new** run (another day or another `pipeline_run_id`) when the dispatcher visits them again, or by **manual HTTP replay** (below).
- Monthly automatic reconciliation **does not** replace replay: replay is for operational recovery after fixes or investigation.

## Reprocess messages (replay)

Call the HTTP function with the **function key** (Portal → Function → **Get Function Url** / key).

**Route:** `https://<function-app>.azurewebsites.net/api/replay/ceap-api-2026`

Query parameters:

| Parameter | Description |
|-----------|-------------|
| `code` | Required: function key. |
| `statuses` | Comma-separated list (default **`FAILED,POISON`**). |
| `endpoint` | Default `ceap`; use `*` to skip endpoint filter. |
| `id_deputado`, `ano`, `mes` | Optional filters. |
| `full` | `true` resets page checkpoint (`last_successful_page` → 0) and refetches all pages. |
| `pipeline_run_id` | Optional: forces value on message and partition; if omitted, uses `ceap_replay_YYYYMMDD` (manual run does not update daily/reconciliation counters the same way as the automatic pipeline). |

Examples:

- Reprocess all failing partitions:  
  `.../api/replay/ceap-api-2026?code=<key>&statuses=FAILED,POISON`
- One deputy:  
  `.../api/replay/ceap-api-2026?code=<key>&statuses=FAILED&id_deputado=204521`
- One month, full re-fetch:  
  `.../api/replay/ceap-api-2026?code=<key>&ano=2026&mes=3&full=true`

## Validate recovery

1. **IngestionState**: partition `status=SUCCESS`, `last_finished_at` / `last_success_at` set when applicable.
2. **Raw**: `page_{n}.json` for all expected pages (last API response has no `next` link).
3. **IngestionControlApi2026** (`_runs`): for automated runs, `total_tasks_success` / `total_tasks_failed` aligned with run closure (`COMPLETED` or `PARTIAL` when there are failures).
4. **Queues**: main queue depth drops; poison has no repeated new items for the same partition after handling.

## Errors by HTTP code

| Code | Expected action |
|------|-----------------|
| 429, 5xx | Backoff in function (HTTP retry); then **queue** retry; last resort **poison**. |
| 400 | Check parameters (`ano`, `mes`, `id_deputado`); terminal failure on partition; fix data and **replay** with `full=true` if needed. |
| 401 / 403 | Review managed identity RBAC on ADLS and account configuration. |
| 404 | Invalid or missing resource; validate `id_deputado` / period. |
| Timeout | Review `functionTimeout` and API limits; isolation is already per message (one partition per message). |

## Duplicates and idempotency

- **Queue / state**: same `pipeline_run_id` does not duplicate messages for the same partition while it is `QUEUED`/`RUNNING`/`SUCCESS`.
- **Raw**: paths include `pipeline_run_id` and `execution_id`; different runs do not overwrite the same blobs.
- **Bronze/Silver** (when jobs exist): semantic deduplication remains in `docs/pipelines/ceap_deduplication_bronze_silver.md`.

## Close the incident

- Queues stable; poison cleared or known messages documented.
- Critical partitions **`SUCCESS`** or accepted failures documented.
- Root cause, actions, and follow-up recorded (including Bronze/Silver alignment when that layer exists).

## Fault tolerance summary

- **Timeout / 5xx / 429**: HTTP retry with backoff; invocation failure → queue retry; after max dequeue → **poison** + partition **`POISON`** in IngestionState.
- **400 / 401 / 403 / 404** (terminal where applicable): partition **`FAILED`** in worker; fix source; **HTTP replay**.
- **Failure after writing a page**: resume at `last_successful_page + 1` for the same `pipeline_run_id`, or `full=true` on replay to refetch all pages.
- **Isolated failure**: does not block other deputies/months (independent messages).
