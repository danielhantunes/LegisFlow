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

1. Timer-triggered CEAP function (`ceap_expenses_ingestion_timer`) starts ingestion run.
2. Function ingests deputies first from API and writes raw payloads.
3. Function builds the eligible deputy list dynamically (no fixed list of 513).
4. Function creates/updates expense partitions by `deputado_id x ano x mes`.
5. Function processes partitions with retry, lock, and status tracking.
6. Raw payloads are stored in ADLS with run/execution metadata in path.
7. When all partitions complete, Function triggers Databricks processing job.
8. Databricks jobs materialize Bronze -> Silver -> Gold Delta layers.

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
