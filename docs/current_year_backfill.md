# Current-year API backfill (manual HTTP)

LegisFlow expects **historical data through 2025** from static files. For the **current calendar year**, ingestion is complemented via the Chamber of Deputies Open Data API using a **manual** Azure Function (no timer for this path).

## Function

- **Name:** `fn_current_year_backfill_dispatcher`
- **Route:** `api/legisflow/backfill/current-year` (Azure prepends `api/`)
- **Auth:** `authLevel=function` (function key required)
- **Feature flag:** `ENABLE_CURRENT_YEAR_BACKFILL_FUNCTION=true` or the HTTP handler returns 404

## Implemented domains

| Domain       | Status                                         |
|-------------|-------------------------------------------------|
| proposicoes | Implemented (list + hash-aware enqueue)        |
| votacoes, eventos, discursos, ceap | `NOT_IMPLEMENTED` until handlers exist |

## Request body (JSON) or query

- `year` — optional; default UTC calendar year. Past years require `force=true`.
- `start_date` / `end_date` — optional ISO dates; default `YYYY-01-01` .. today (UTC). Window outside `year` requires `force=true`.
- `domains` — **required** list, e.g. `["proposicoes"]`.
- `dry_run` — default `true`: no queue messages, no RAW, no state updates; returns counts only.
- `force` — allow reprocessing when hash unchanged (workers honor `force_reprocess`).
- `max_tasks` — cap on messages enqueued this call (default 1000). Above 5000 requires `confirm_max_tasks=true`.
- `confirm_max_tasks` — must be true when `max_tasks` > 5000.

## `pipeline_run_id`

Format: `current_year_backfill_YYYYMMDDHHMMSS` (UTC, 14 digits, no extra underscore). This shape matches `is_well_formed_pipeline_run_id` in `shared.domain_catalog`.

## Examples

Dry run (safe default):

```bash
curl -sS -X POST "https://<app>.azurewebsites.net/api/legisflow/backfill/current-year?code=<FUNCTION_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"domains":["proposicoes"],"dry_run":true,"max_tasks":100}'
```

Controlled real run:

```bash
curl -sS -X POST "https://<app>.azurewebsites.net/api/legisflow/backfill/current-year?code=<FUNCTION_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"domains":["proposicoes"],"dry_run":false,"max_tasks":100}'
```

## Cost notes

Each non–dry-run message is one queue item for existing workers (`proposicoes-api-work`). Listing pages still calls the public API; use `dry_run` and low `max_tasks` first.
