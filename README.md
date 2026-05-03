# LegisFlow - Azure Lakehouse for Legislative Data Analytics

LegisFlow is a portfolio project that implements a modern, reproducible, and cost-efficient Azure Lakehouse for Brazilian legislative data analytics.

The platform ingests data from the Brazilian Chamber of Deputies open data ecosystem, preserves immutable raw data, and processes analytical layers with Databricks, PySpark, and Delta Lake.

## Objective

Build a production-like data engineering platform to:

- Ingest CEAP parliamentary expense data from official annual files and Dados Abertos Camara API.
- Process data through Raw, Bronze, Silver, and Gold layers.
- Keep strict traceability and idempotent ingestion behavior.
- Enable analytical use cases focused on expenses by deputy while preserving leadership records separately.

## MVP Scope

Primary scope:

1. CEAP parliamentary expense analysis by deputy
2. Analytical calendar of legislative events
3. Parliamentary attendance and absenteeism monitoring

Optional scope:

- Voting micro-batch pipeline with SLA monitoring and replay runbook

## High-Level Architecture

Sources (Dados Abertos API and CEAP files) are ingested by Azure Functions into ADLS Gen2 Raw storage. Databricks jobs transform the data into Delta tables across Bronze, Silver, and Gold layers.

Flow:

`Sources -> Azure Functions -> ADLS Gen2 Raw -> Databricks PySpark -> Delta Bronze/Silver/Gold -> Dashboards`

## Core Architectural Decisions

- Raw data is immutable and preserved exactly as received.
- Analytical layers use Delta Lake (not plain Parquet tables) for ACID, MERGE, and schema governance.
- Heavy transformations run only in Databricks, not in Azure Functions.
- Ingestion state is persisted externally (Table Storage or Queue-based control model) to support resume/retry and idempotency.
- Databricks uses a dedicated ADLS Gen2 lakehouse account (not Databricks-managed internal storage).
- Infrastructure is provisioned with Terraform modules and deployed with GitHub Actions using OIDC.

## Azure Environment

- Subscription name: `woltrix-legisflow`
- Deployment environment: `dev`
- Main region: `eastus2`
- Main resource group: `rg-legisflow-dev`
- Terraform state resource group: `rg-legisflow-dev-tfstate`

## Repository Structure

```text
legisflow/
  README.md
  docs/
    architecture.md
    decisions.md
    runbooks/
      ceap_api_ingestion_2026.md
    pipelines/
      ceap_deduplication_bronze_silver.md
  infra/
    terraform/
      bootstrap-tfstate/
      base/
      ingestion/
      databricks/
  functions/
    ceap_expenses_ingestion_timer/
      shared/
      ceap_api_2026_dispatcher/
      ceap_api_2026_worker/
      ceap_api_2026_poison_handler/
      fn_replay_ceap_failed_messages/
    votacoes_microbatch/
  databricks/
    notebooks/
      bronze/
      silver/
      gold/
      quality/
    dlt/
    jobs/
  src/
    pyspark/
      bronze/
      silver/
      gold/
      quality/
      utils/
  tests/
    unit/
    integration/
  config/
  .github/
    workflows/
```

## Delivery Plan (Incremental)

1. Foundation (folders, docs, architecture baseline)
2. Terraform bootstrap for remote state
3. Base Terraform (resource group, ADLS, observability, identities)
4. Ingestion Terraform (Function App, storage, state services)
5. CEAP API 2026 ingestion (dispatcher + queue worker + control table + replay HTTP) and idempotent Raw paths
6. Databricks Bronze/Silver/Gold pipelines
7. Data quality rules and tests
8. Databricks jobs and deployment automation
9. Optional voting micro-batch module
10. Final runbooks and hardening documentation

## CI/CD Principles

- Use GitHub Actions with Azure OIDC (`azure/login@v2`)
- Keep infrastructure and application deployments separated
- Use Terraform backends in Azure Blob Storage with per-module state keys
- Keep workflows manually triggered (`workflow_dispatch`) for controlled MVP deployments
- Use only `main` branch for MVP release flow

## GitHub Configuration

Secrets:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`

## Security and Cost Principles

- Managed Identity and RBAC for ADLS access
- No hardcoded secrets in code
- No Azure Key Vault in MVP (documented as future enhancement)
- Use ephemeral Databricks job clusters
- Prefer Azure Functions Consumption/Flex Consumption
- Tag all resources for governance and cost tracking

## Next Steps

The next implementation step is creating the Databricks Terraform module and deployment workflows:

- `.github/workflows/terraform-databricks-dev.yml`
