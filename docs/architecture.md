# LegisFlow Architecture Overview

## 1) Solution Goal

LegisFlow implements an Azure-native Lakehouse platform for Brazilian Chamber of Deputies legislative data, with emphasis on CEAP expense analytics, traceability, idempotent ingestion, and low operational cost.

The architecture is production-oriented, but constrained to a portfolio-friendly MVP.

## 2) Scope and Data Domains

### In Scope (MVP)

- CEAP expense analytics by deputy
- Legislative events calendar analytics
- Attendance and absenteeism monitoring

### Optional Extension

- Voting micro-batch ingestion and DLT quality/SLA pipeline

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

1. **CEAP API 2026 (MVP atual)**: o timer `ceap_api_2026_dispatcher` (a cada 20 minutos, UTC) decide o modo por data: em dias normais **daily** (janela móvel: mês corrente + o mês anterior, limitado a `CEAP_DAILY_LOOKBACK_MONTHS`); no dia `CEAP_RECONCILIATION_DAY` (padrão 25) roda só **reconciliation** (janeiro → mês atual, sem daily nesse dia). O dispatcher enfileira lotes de até `CEAP_MAX_TASKS_PER_DISPATCH` para a fila `ceap-api-2026-work`, grava o run em `IngestionControlApi2026` (`_runs` + contadores) e usa **lock** de 15 min em `_locks/ceap_dispatcher_lock` para evitar concorrência. O mesmo dispatcher trata `/deputados` como **snapshot diário**: cada tick verifica primeiro `IngestionControlApi2026._snapshots/deputados_YYYYMMDD` e o marcador `_SUCCESS` em Raw — se o snapshot já estiver `COMPLETED` (`record_count>0`, `total_pages>0`, `raw_path` preenchido), o dispatcher carrega-o em memória e **não chama** `/deputados` nesse ciclo. Em modo `reconciliation` há fallback automático para o snapshot completo mais recente quando o do dia atual ainda não está pronto. Só quando nenhum snapshot válido existe é que paginação real ocorre, gravando em `raw/camara/deputados/api/list/reference_date={YYYY-MM-DD}/pipeline_run_id=.../execution_id=.../page_{n}.json` (com `reference_date` em `CEAP_REFERENCE_TIMEZONE`, predefinição `America/Sao_Paulo`), seguido por `metadata.json` + `_SUCCESS` na pasta da data e atualização de `_snapshots`. O run CEAP regista em `deputies_snapshot_*` o snapshot efetivamente usado e os flags `snapshot_reused`/`snapshot_created`. Após o run do dia ganhar `enqueue_phase_complete` e, quando houver tarefas, o processamento atingir conclusão no registo do run, execuções subsequentes no mesmo dia **não** reenfileiram. Cada mensagem inclui `mode`, `pipeline_run_id` (ex.: `ceap_daily_YYYYMMDD` / `ceap_reconciliation_YYYYMMDD`) e `dispatched_at`.
2. **Worker** `ceap_api_2026_worker` trata **daily** e **reconciliation** da mesma fila: paginação em `/deputados/{id}/despesas`, checkpoint na tabela **IngestionState** (`PartitionKey=ceap_2026`, `RowKey=despesas|{id}|{ano}|{mes}`), contadores de run automatizado em `IngestionControlApi2026` quando `pipeline_run_id` é daily/reconciliation, e Raw em ADLS com `reference_year` / `reference_month` / `pipeline_run_id` / `execution_id` / `deputado_id` / `page_{n}.json` para evitar sobrescrita entre runs.
3. **Poison** `ceap-api-2026-work-poison` + `ceap_api_2026_poison_handler` marcam a partição como **POISON** em IngestionState e incrementam falhas no run automatizado quando aplicável.
4. **Replay** HTTP `fn_replay_ceap_failed_messages` lê **IngestionState** (estados como FAILED/POISON), reenfileira com `pipeline_run_id` opcional ou `ceap_replay_YYYYMMDD`, e **não** substitui a reconciliação mensal; serve só para reprocessamento manual. Runbook: `docs/runbooks/ceap_api_ingestion_2026.md`.
5. **Histórico 2019–2025**: ficheiros estáticos (fora deste fluxo API).
6. **Legado desativado**: `ceap_expenses_ingestion_timer` monolítico permanece no pacote mas desativado por predefinição (`AzureWebJobs...Disabled` + `CEAP_LEGACY_MONOLITH_ENABLED=false`); só para emergência.
7. Databricks jobs materializam Bronze → Silver → Gold Delta layers; deduplicação CEAP API descrita em `docs/pipelines/ceap_deduplication_bronze_silver.md`.

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

## 12.1) Function App and Function Scope (MVP)

- One Function App: `func-legisflow-ingestion-dev`
- Implemented now: `ceap_expenses_ingestion_timer`
- Not implemented yet:
  - `legislative_events_ingestion_timer`
  - `voting_microbatch_timer`
  - `parliamentary_fronts_ingestion_timer`
  - `propositions_lifecycle_ingestion_timer`

Future endpoints must be delivered later as separate functions inside the same Function App, not inside the CEAP function implementation.

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
