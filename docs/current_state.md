# LegisFlow — current technical state

Continuity document for new sessions. **Last reviewed against code and Terraform in the repo** (does not guarantee what is applied in each Azure environment).

## 1. Function App and runtime

- **Function project:** `functions/ceap_expenses_ingestion_timer/` (Python 3.11, bundle ~4, 10-minute timeout, queue `maxDequeueCount` 5 — see `host.json`).
- **Infra (Terraform `ingestion`):** `azurerm_function_app_flex_consumption` with `RAW_STORAGE_ACCOUNT_NAME` pointing at the ADLS account from module `base`, `LAKEHOUSE_FILESYSTEM_NAME` = `lakehouse`, `CEAP_QUEUE_STORAGE` on the Function’s storage account (work queues).
- **Legacy:** timer `ceap_expenses_ingestion_timer` (monolith) exists in the package; `local.settings.json.example` sets `AzureWebJobs.ceap_expenses_ingestion_timer.Disabled` = `true` and `CEAP_LEGACY_MONOLITH_ENABLED` = `false`. The active path is **CEAP API 2026** + newer domains.

## 2. Domains implemented in code (by function folder)

Each domain (except “classic” CEAP) follows: **dispatcher (timer)** → **work queue** → **worker (queue)** → **poison** + **HTTP replay** + **HTTP reset** (reset gated by flags).

| Domain | Dispatcher | Worker | Poison | Replay HTTP | Reset HTTP |
|--------|------------|--------|--------|-------------|------------|
| **CEAP** (`ceap`) | `ceap_api_2026_dispatcher` | `ceap_api_2026_worker` | `ceap_api_2026_poison_handler` | `fn_replay_ceap_failed_messages` | `fn_reset_ceap_pipeline_run` |
| **reference** | `reference_snapshot_dispatcher` | `reference_snapshot_worker` | `reference_snapshot_poison_handler` | `fn_replay_reference_failed_messages` | `fn_reset_reference_pipeline_run` |
| **votacoes** | `votacoes_api_dispatcher` | `votacoes_api_worker` | `votacoes_api_poison_handler` | `fn_replay_votacoes_failed_messages` | `fn_reset_votacoes_pipeline_run` |
| **proposicoes** | `proposicoes_daily_dispatcher`, `proposicoes_reconciliation_dispatcher` | `proposicoes_worker` | `proposicoes_poison_handler` | `fn_replay_proposicoes_failed_messages` | `fn_reset_proposicoes_pipeline_run` |
| **eventos** | `eventos_daily_dispatcher`, `eventos_reconciliation_dispatcher` | `eventos_worker` | `eventos_poison_handler` | `fn_replay_eventos_failed_messages` | `fn_reset_eventos_pipeline_run` |
| **institucional** | `institucional_dispatcher` | `institucional_worker` | `institucional_poison_handler` | `fn_replay_institucional_failed_messages` | `fn_reset_institucional_pipeline_run` |
| **discursos** | `discursos_daily_dispatcher`, `discursos_reconciliation_dispatcher` | `discursos_worker` | `discursos_poison_handler` | `fn_replay_discursos_failed_messages` | `fn_reset_discursos_pipeline_run` |

Additional folders may exist for legacy microbatch (`proposicoes_dispatcher`, `eventos_dispatcher`, `discursos_dispatcher`) and cross-cutting timers/HTTP — see `docs/azure_function_app_refactor_plan.md`.

**Declarative catalog:** `shared/domain_catalog.py` registers `ceap`, `reference`, `votacoes`, `proposicoes`, `eventos`, `institucional`, `discursos` (queues, table partition keys, `pipeline_run_id` prefixes, endpoints). Production CEAP still uses dedicated modules (`ceap_*`); the CEAP entry in the catalog is partly **descriptive** (comment in file).

## 3. Queues (default / example names)

Defined in `local.settings.json.example` and created in Terraform `ingestion` (`azurerm_storage_queue` + `app_settings`):

| Domain | Work | Poison |
|--------|------|--------|
| CEAP | `ceap-api-2026-work` | `ceap-api-2026-work-poison` |
| reference | `reference-snapshot-work` | `reference-snapshot-work-poison` |
| votacoes | `votacoes-api-work` | `votacoes-api-work-poison` |
| proposicoes | `proposicoes-api-work` | `proposicoes-api-work-poison` |
| eventos | `eventos-api-work` | `eventos-api-work-poison` |
| institucional | `institucional-api-work` | `institucional-api-work-poison` |
| discursos | `discursos-api-work` | `discursos-api-work-poison` |

## 4. Chamber of Deputies Open Data API coverage in ingestion code

- **CEAP:** `/deputados/{id}/despesas` (+ `/deputados` snapshot in CEAP dispatcher — see CEAP runbook).
- **reference:** `partidos`, `legislaturas`, `deputados`, `frentes`, `orgaos` (paginated list per snapshot endpoint).
- **votacoes:** `/votacoes` (list) + `/votacoes/{id}/votos`.
- **proposicoes:** `/proposicoes` (list with procedural date window, `dataInicio`/`dataFim`) + `/proposicoes/{id}/autores` + `/proposicoes/{id}/tramitacoes`.
- **eventos:** `/eventos` (windowed list) + `/eventos/{id}/deputados|orgaos|pauta|votacoes`.
- **institucional:** parent `/orgaos`, `/partidos`, `/frentes`, `/legislaturas` + sub-routes `membros`, `lideres`, `mesa` per `domain_catalog`.
- **discursos:** `/deputados` (snapshot in discursos dispatcher) + `/deputados/{id}/discursos` with `dataInicio`/`dataFim` in worker.

HTTP path detail is in each `function.json` (`replay/*` and `legisflow/reset/*` routes).

## 5. State and control (Azure Tables)

- **`IngestionState`:** per-partition progress; partition keys **per domain** (e.g. `ceap_2026`, `eventos_2026`, … — see `DomainSpec.state_partition_key`).
- **`IngestionControlApi2026`:** runs, locks, snapshots; partition keys per domain (`_runs`, `_runs_eventos`, …).
- **Locks:** dispatchers use one lock row per domain (e.g. `ceap_dispatcher_lock`, `eventos_dispatcher_lock`) to avoid concurrent ticks.
- **CEAP:** manifest/control reconciliation with `IngestionState` (includes partitions matching `current_pipeline_run_id` **or** `last_pipeline_run_id` in count filters, per `ceap_partition_state.py`).

## 6. RAW layer (ADLS Gen2)

- **Filesystem:** `lakehouse` (default).
- **Common prefix:** `raw/camara/...` per domain; JSON per page + `metadata.json` and `_SUCCESS` when the run is **strictly** complete (contract in `shared/metadata.py` and per-domain manifests).
- **“Empty” dirs in Terraform `base`:** minimal list in `infra/terraform/base/main.tf` (`lakehouse_directories`); **new** `raw/camara/<domain>/...` branches are created **implicitly** on first write (ADLS Gen2 behavior).

## 7. Manifests / metadata modules

- **Generic:** `shared/metadata.py` (v1.0 contract, `hash_strategy`, `audit_fields_applied` where used).
- **CEAP:** `shared/ceap_raw_manifest.py`, `shared/deputies_snapshot.py`.
- **reference:** `shared/reference_raw_manifest.py` (+ reset helpers).
- **votacoes / proposicoes / eventos / institucional / discursos:** matching `shared/*_raw_manifest.py` + `shared/*_run.py` for workers.

## 8. Automated tests

- `tests/` at repo root (pytest); covers catalog, metadata, audit envelope, CEAP reset/manifest, queues, and domains `votacoes`, `proposicoes`, `eventos`, `institucional`, `discursos` (runs and reset helpers). **Does not replace** Azure integration tests.

## 9. CI/CD (observed workflows)

- `terraform-tfstate-backend-dev.yml` — state backend.
- `terraform-base-dev.yml` — RG + ADLS + initial dirs, etc.
- `terraform-ingestion-dev.yml` — ingestion Function App + queues + app settings (reads base outputs).
- `terraform-databricks-dev.yml` — Databricks workspace.
- `deploy-function-ceap.yml` — Python package deploy.

## 10. Open issues / documented inconsistencies

1. **`docs/decisions.md` ADR-003** still describes “CEAP only”; **code** already includes multiple domains in the same Function App — treat ADR as historical until formally revised.
2. **Environment:** this file describes the **repository**; `terraform apply` / deploy may lead or lag Git in each subscription.
3. **Bronze/Silver/Gold** for new RAW prefixes: pipelines documented in-repo focus on CEAP; **Databricks consumption** of other domains is not described here as complete.
4. **Discursos replay:** payload may reuse `last_window_date_*` from partition when present; otherwise replay may need explicit `date_start`/`date_end` (implemented in HTTP handler).

## 11. Technical decisions already in code

- **Single catalog** (`domain_catalog.py`) for queue names, table partition keys, and generic-domain `pipeline_run_id` contracts.
- **RAW audit:** `_audit`, `_payload_hash`, `_record_uid`, `_record_hash`; business keys with nested paths (e.g. `deputado_.id`, `faseEvento.titulo`) via `shared/raw_audit.py`.
- **HTTP reset:** off by default (`ENABLE_RESET_FUNCTIONS` and per-domain flags); admin reset for dev/test.
- **Poison:** dedicated queue per domain + handler updating `IngestionState` and run counters when applicable.

## 12. Preserved historical documentation

- `docs/runbooks/ceap_api_ingestion_2026.md`
- `docs/pipelines/ceap_deduplication_bronze_silver.md`
- `docs/decisions.md` (ADRs; note on ADR-003 above)
