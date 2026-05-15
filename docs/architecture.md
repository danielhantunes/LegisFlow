# LegisFlow Architecture Overview

**About this document:** It describes the target architecture and the CEAP flow in detail. As of 2026 the same Function App hosts **multiple** ingestion domains (reference, votacoes, proposicoes, eventos, institucional, discursos) in addition to CEAP API 2026 — see `docs/current_state.md` and `docs/pipeline_status.md` for lists and status. Section **12.1** reflects the current code layout; `docs/decisions.md` ADR-003 remains the historical record of the original MVP decision.

## 1) Solution Goal

LegisFlow implements an Azure-native Lakehouse platform for Brazilian Chamber of Deputies legislative data, with emphasis on CEAP expense analytics, traceability, idempotent ingestion, and low operational cost.

The architecture is production-oriented, but constrained to a portfolio-friendly MVP.

## 2) Scope and Data Domains

### In Scope (MVP)

- CEAP expense analytics by deputy
- Legislative events calendar analytics
- Attendance and absenteeism monitoring

### API ingestion to RAW (implemented in repo, same Function App)

The Python Function project under `functions/ceap_expenses_ingestion_timer/` now contains **timer dispatchers**, **queue workers**, **poison handlers**, and **HTTP replay/reset** helpers for:

- **CEAP** (`ceap_api_2026_*`) — primary production path for deputy expense (CEAP) data
- **reference** — dimension snapshots (parties, legislatures, deputies, frentes, orgaos)
- **votacoes** — list `/votacoes` + per roll-call `votos`
- **proposicoes** — list + authors + procedural history (`tramitacoes`) per bill
- **eventos** — list + per-event deputados/orgaos/pauta/votacoes
- **institucional** — daily fanout from orgaos/partidos/frentes/legislaturas to membros/lideres/mesa
- **discursos** — deputies snapshot + per-deputy discursos window

Declarative configuration lives in `shared/domain_catalog.py`. Queues and app settings are provisioned in Terraform module `infra/terraform/ingestion`.

### Optional Extension (still broader than code)

- DLT quality/SLA automation and full analytical coverage for all new RAW prefixes in Databricks (see `docs/pipeline_status.md`).

## 3) Platform Components

- **Azure Functions**: one shared Function App for ingestion workloads in MVP
- **ADLS Gen2 (dedicated account)**: persistent Lakehouse storage for Raw/Bronze/Silver/Gold/logs/checkpoints
- **Azure Databricks**: PySpark transformations and Delta modeling
- **Delta Lake**: analytical table format for Bronze/Silver/Gold
- **Application Insights**: telemetry, failures, and dependency monitoring
- **Azure Table Storage or Queue-based control model**: ingestion state tracking and resumability
- **Terraform**: infrastructure provisioning and state management
- **GitHub Actions (OIDC)**: CI/CD for infrastructure and deployment

## 4) Environment and Deployment Boundaries

- Subscription: `woltrix-legisflow` (pre-existing)
- Region: `eastus2` (Terraform variable)
- Main resource group: `rg-legisflow-dev`
- Terraform state resource group: `rg-legisflow-dev-tfstate`

The architecture uses separate resource groups for workload and Terraform backend to improve lifecycle isolation and governance.

## 5) End-to-End Data Flow

1. **CEAP API 2026 (current MVP path):** The `ceap_api_2026_dispatcher` timer (default every 20 minutes UTC) picks the mode by calendar date: on normal days **daily** (rolling window: current month + prior month, bounded by `CEAP_DAILY_LOOKBACK_MONTHS`); on `CEAP_RECONCILIATION_DAY` (default 25) only **reconciliation** runs (January through current month — no daily on that day). The dispatcher enqueues batches up to `CEAP_MAX_TASKS_PER_DISPATCH` to `ceap-api-2026-work`, persists the run in `IngestionControlApi2026` (`_runs` and counters), and uses a ~15-minute **lock** on `_locks/ceap_dispatcher_lock` to avoid concurrent ticks. The same dispatcher treats `/deputados` as a **daily snapshot:** each tick first checks `IngestionControlApi2026._snapshots/deputados_YYYYMMDD` and the `_SUCCESS` marker in Raw — if the snapshot is already `COMPLETED` (`record_count>0`, `total_pages>0`, `raw_path` set), it loads deputies in memory and **does not call** `/deputados` that cycle. In `reconciliation` there is automatic fallback to the latest complete snapshot when today’s is not ready. Real pagination runs only when no valid snapshot exists, writing under `raw/camara/deputados/api/list/reference_date={YYYY-MM-DD}/pipeline_run_id=.../execution_id=.../page_{n}.json` (`reference_date` uses `CEAP_REFERENCE_TIMEZONE`, default `America/Sao_Paulo`), then `metadata.json` + `_SUCCESS` and `_snapshots` updates. The CEAP run stores the effective snapshot in `deputies_snapshot_*` fields plus `snapshot_reused` / `snapshot_created`. After the day’s run reaches `enqueue_phase_complete` and (when there is work) the run record shows completion, further ticks the same day **do not** enqueue again. Each queue message carries `mode`, `pipeline_run_id` (e.g. `ceap_daily_YYYYMMDD` / `ceap_reconciliation_YYYYMMDD`), and `dispatched_at`.
2. **Worker** `ceap_api_2026_worker` handles **daily** and **reconciliation** from the same queue: pagination on `/deputados/{id}/despesas`, checkpoints in **IngestionState** (`PartitionKey=ceap_2026`, `RowKey=despesas|{id}|{ano}|{mes}`), automated run counters in `IngestionControlApi2026` when `pipeline_run_id` is daily/reconciliation, and Raw in ADLS with `reference_year` / `reference_month` / `pipeline_run_id` / `execution_id` / `deputado_id` / `page_{n}.json` so runs do not overwrite each other.
3. **Poison** `ceap-api-2026-work-poison` + `ceap_api_2026_poison_handler` mark the partition **POISON** in IngestionState and increment failures on the automated run when applicable.
4. **Replay** HTTP `fn_replay_ceap_failed_messages` reads **IngestionState** (e.g. FAILED/POISON), re-enqueues with optional `pipeline_run_id` or `ceap_replay_YYYYMMDD`; it **does not** replace scheduled monthly reconciliation — manual reprocessing only. Runbook: `docs/runbooks/ceap_api_ingestion_2026.md`.
5. **History 2019–2025:** static files (outside this API flow).
6. **Legacy disabled:** monolithic `ceap_expenses_ingestion_timer` remains in the package but is off by default (`AzureWebJobs...Disabled` + `CEAP_LEGACY_MONOLITH_ENABLED=false`); emergency use only.
7. Databricks jobs materialize Bronze → Silver → Gold; CEAP API deduplication is in `docs/pipelines/ceap_deduplication_bronze_silver.md`. Bronze/Delta consumption for **non-CEAP** RAW prefixes is not described here as complete — see `docs/pipeline_status.md`.

### 5.1) Other domains: API → queue → RAW (summary)

Common pattern: **timer dispatcher** (with lock in `IngestionControlApi2026`) lists or discovers IDs, writes list pages to ADLS when applicable, enqueues JSON `DomainWorkMessage` to the domain **work** queue; **queue worker** paginates the sub-endpoint, writes RAW with the audit envelope (`shared/raw_audit.py`), updates `IngestionState` and run counters; **poison queue** + handler mark `POISON`; **HTTP replay** re-enqueues `FAILED`/`POISON`; **HTTP reset** (via `ENABLE_*` flags) cleans artifacts by `pipeline_run_id` in a controlled environment.

Flow: API → Azure Functions → **Queue Storage (same account as `CEAP_QUEUE_STORAGE` / Function connection)** → **ADLS Gen2 (lakehouse)**; Databricks remains the next step for Delta modeling. Queue names, `pipeline_run_id` conventions, and paths: `docs/current_state.md`, `docs/raw_layer.md`, `shared/domain_catalog.py`.

## 6) Layering Strategy (Lakehouse)

## Raw

Purpose: immutable source landing zone.

Rules:

- Preserve payloads exactly as received.
- Do not deduplicate, filter, or apply business rules.
- Keep API JSON and CEAP CSV/ZIP originals.
- Never delete historical raw records.

## Bronze (Delta)

Purpose: structured ingestion layer with minimal harmonization.

Adds technical metadata:

- `_source_file`
- `_source_endpoint`
- `_ingestion_date`
- `_pipeline_run_id`
- `_execution_id`
- `_loaded_at`
- `_payload_hash`
- `_record_hash`

## Silver (Delta)

Purpose: standardized and quality-controlled curated layer.

Processing includes:

- Type casting and schema normalization
- CNPJ/CPF normalization
- Deduplication
- Beneficiary classification:
  - `DEPUTADO`
  - `LIDERANCA`
  - `OUTROS`
- Dimension modeling (deputy, supplier, category, month/date, party, state)

## Gold (Delta)

Purpose: analytics-ready facts, dimensions, and views.

Core objects include:

- `gold.fato_despesas`
- `gold.dim_deputado`
- `gold.dim_fornecedor`
- `gold.dim_categoria_despesa`
- `gold.dim_mes`
- Deputy-only and leadership-specific analytical views

## 7) CEAP Rules and Business Semantics

- `ideCadastro` populated -> deputy-linked expense
- `ideCadastro` null + `txNomeParlamentar` starts with `LID.` -> `LIDERANCA`
- Leadership records are preserved in all technical layers
- Leadership expenses are excluded from deputy/party/state core indicators
- `nuDeputadoId` is preserved as technical CEAP identifier
- Analytical date for expenses: `datEmissao`
- API competency controls: `numAno` and `numMes`

This separation avoids analytical distortion while preserving traceability.

## 8) Historical and Incremental Ingestion Strategy

Historical baseline:

- Legislature: 57th
- Analytical start date: `2023-02-01`
- Backfill source: CEAP annual files `2023`, `2024`, `2025`

Current year ingestion:

- Source: API (`/deputados/{id}/despesas`)
- Coverage: `2026` until current date

Incremental cadence:

- Daily load: current month + previous month
- Periodic reconciliation: full current year

## 9) State Control and Idempotency Model

State store persists per-partition progress with keys like:

- `pipeline_run_id`
- `execution_id`
- `entity`
- `partition_key` (`despesas|{deputado_id}|{ano}|{mes}`)
- `deputado_id`, `ano`, `mes`, `page`
- `status`, `attempt_count`, `last_page_processed`
- timestamps, `raw_path`, `error_message`

Statuses:

- `PENDING`
- `RUNNING`
- `SUCCESS`
- `FAILED`
- `STALE`

Idempotency rules:

- `SUCCESS`: skip unless reprocess mode enabled
- `PENDING`: process
- `FAILED`: retry while `attempt_count < max_retries`
- stale `RUNNING`: mark `STALE` and reprocess
- enforce lock to prevent concurrent duplicate partition execution

This design allows continuation across multiple Function executions.

## 10) Resilience and Retry Strategy

HTTP retry policy for API calls:

- max attempts: 3
- backoff pattern: 2s, 5s, 10-15s with jitter
- retry for: `408, 429, 500, 502, 503, 504`, and timeouts
- no retry for: `400, 401, 403, 404`

Persistent failures are logged for audit and operational follow-up.

## 11) Terraform and State Architecture

Terraform module structure:

- `infra/terraform/bootstrap-tfstate`
- `infra/terraform/base`
- `infra/terraform/ingestion`
- `infra/terraform/databricks`

Bootstrap module provisions:

- `rg-legisflow-dev-tfstate`
- `stlegisflowdevtfstate`
- `tfstate` container

Each module uses isolated backend key:

- `base-dev.tfstate`
- `ingestion-dev.tfstate`
- `databricks-dev.tfstate`

## 12) CI/CD Architecture (GitHub Actions)

Current MVP workflows:

1. `terraform-tfstate-backend-dev.yml`
2. `terraform-base-dev.yml`
3. `terraform-ingestion-dev.yml`
4. `terraform-databricks-dev.yml`
5. `deploy-function-ceap.yml`

MVP intentionally excludes Databricks asset deployment workflow and future function deployment workflows.

## 12.1) Function App and function inventory (current code)

- **One Function App** (Terraform dev name): `func-legisflow-ingestion-dev` (variable `function_app_name`).
- **Legacy timer** `ceap_expenses_ingestion_timer` (root `function.json`) remains in the package but is **disabled by default** (`AzureWebJobs.ceap_expenses_ingestion_timer.Disabled`, `CEAP_LEGACY_MONOLITH_ENABLED=false`).
- **Core domain folders** (each folder = one deployed function; see `function.json` in repo):

| Domain | Dispatchers / timers | Worker | Poison | Replay HTTP | Reset HTTP |
|--------|----------------------|--------|--------|-------------|------------|
| CEAP | `ceap_api_2026_dispatcher` | `ceap_api_2026_worker` | `ceap_api_2026_poison_handler` | `fn_replay_ceap_failed_messages` | `fn_reset_ceap_pipeline_run` |
| reference | `reference_snapshot_dispatcher` | `reference_snapshot_worker` | `reference_snapshot_poison_handler` | `fn_replay_reference_failed_messages` | `fn_reset_reference_pipeline_run` |
| votacoes | `votacoes_api_dispatcher` | `votacoes_api_worker` | `votacoes_api_poison_handler` | `fn_replay_votacoes_failed_messages` | `fn_reset_votacoes_pipeline_run` |
| proposicoes | `proposicoes_daily_dispatcher`, `proposicoes_reconciliation_dispatcher`, optional legacy `proposicoes_dispatcher` | `proposicoes_worker` | `proposicoes_poison_handler` | `fn_replay_proposicoes_failed_messages` | `fn_reset_proposicoes_pipeline_run` |
| eventos | `eventos_daily_dispatcher`, `eventos_reconciliation_dispatcher`, optional `eventos_dispatcher` | `eventos_worker` | `eventos_poison_handler` | `fn_replay_eventos_failed_messages` | `fn_reset_eventos_pipeline_run` |
| institucional | `institucional_dispatcher` | `institucional_worker` | `institucional_poison_handler` | `fn_replay_institucional_failed_messages` | `fn_reset_institucional_pipeline_run` |
| discursos | `discursos_daily_dispatcher`, `discursos_reconciliation_dispatcher`, optional `discursos_dispatcher` | `discursos_worker` | `discursos_poison_handler` | `fn_replay_discursos_failed_messages` | `fn_reset_discursos_pipeline_run` |

- **Cross-cutting / ops:** `daily_summary_builder`, `reconciliation_scheduler`, `fn_reconciliation_control_http`, `fn_current_year_backfill_dispatcher`; manual reconciliation starters `fn_start_*_reconciliation` (four domains). Exact count varies with feature flags — see `docs/azure_function_app_refactor_plan.md` for a full inventory.

Older roadmap names such as `legislative_events_ingestion_timer` do not match folder names 1:1; behavior lives under **eventos** / **votacoes** (and related HTTP starters).

Shared libraries (`shared/`) hold API client, ADLS writer, run registry, partition state, queue messages, Raw audit/metadata, and domain-specific `*_raw_manifest.py` / `*_run.py` / `*_pipeline_reset*.py`.

Authentication model:

- OIDC with `azure/login@v2`
- required permissions:
  - `id-token: write`
  - `contents: read`
- no client secret usage
- main branch subject binding for federated credentials
- only GitHub Secrets are required in MVP (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`)

## 13) Observability and Operations

Application Insights covers:

- function executions and latency
- exceptions and dependency failures
- call volume and runtime diagnostics

Operational ingestion status remains in dedicated state control storage. Monitoring and control state are complementary, not interchangeable.

## 14) Security and Cost Posture

Security:

- Managed Identity for Function-to-ADLS access
- RBAC over storage permissions
- GitHub Secrets only for sensitive values
- MVP workflows use fixed non-sensitive values to reduce setup overhead
- no Azure Key Vault in MVP

Cost:

- Azure Functions Consumption/Flex Consumption
- partitioned ingestion to control runtime
- ephemeral Databricks job clusters
- ADLS as persistent low-cost storage
- mandatory resource tags for governance

## 15) Why This Architecture Fits the MVP

This design balances engineering rigor and practical feasibility:

- strong reproducibility via Terraform + CI/CD
- production-like reliability via stateful idempotent ingestion
- scalable lakehouse modeling via Delta and Databricks
- cost-conscious operation through serverless and ephemeral compute
- analytical correctness through explicit leadership classification rules

The result is a credible, portfolio-ready platform that demonstrates modern DataOps and cloud data engineering practices.
