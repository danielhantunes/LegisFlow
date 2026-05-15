# Pipeline status — LegisFlow

Legend: **Healthy** = implemented in the repository with a full dispatcher → queue → worker → RAW flow; **Partial** = partially implemented or depends on Azure deploy/validation; **Not started** = no matching implementation in this repo.

## 1. API ingestion → RAW (Azure Functions)

| Pipeline | Code status | Notes |
|----------|-------------|-------|
| **CEAP 2026** (`ceap_api_2026_*`) | **Healthy** | Dispatcher with daily/reconciliation, deputies snapshot, manifest reconciliation with `IngestionState`, CEAP queues. |
| **reference snapshot** | **Healthy** | Parties, legislatures, deputies, frentes, orgaos; timer + worker + poison + replay + reset. |
| **votacoes** | **Healthy** | List `/votacoes` + fanout `/votacoes/{id}/votos`; microbatch every few minutes. |
| **proposicoes** | **Healthy** | List + fanout authors/tramitacoes; microbatch. |
| **eventos** | **Healthy** | List + fanout 4 sub-routes; microbatch. |
| **institucional** | **Healthy** | Parents + fanout membros/lideres/mesa; daily run id. |
| **discursos** | **Healthy** | `/deputados` snapshot in dispatcher + fanout `/deputados/{id}/discursos` with window; microbatch. |
| **CEAP monolith** (`ceap_expenses_ingestion_timer`) | **Disabled** | Still in package; disabled in example config and Terraform ingestion. |

## 2. Infrastructure (Terraform)

| Module | Status |
|--------|--------|
| `bootstrap-tfstate` | **Healthy** (dedicated workflow) — remote backend. |
| `base` | **Healthy** — ADLS lakehouse, RG, initial directories. |
| `ingestion` | **Healthy** in code — Flex Function + queues + app settings for **all** domains listed in `current_state.md`. |
| `databricks` | **Healthy** (workspace) — notebook/job automation outside repo by MVP decision (see `docs/decisions.md`). |

**Partial:** “In production” state depends on last `terraform apply` / workflow and branch (`terraform-ingestion-dev` applies only from `main` per workflow design).

## 3. Quality and tests

| Area | Status |
|------|--------|
| Unit tests (`tests/`) | **Healthy** locally (pytest) — no Azure required. |
| E2E against Câmara API + Azure | **Not** automated in this repo (operational gap). |

## 4. Downstream consumption (Bronze / Databricks)

| Area | Status |
|------|--------|
| CEAP Bronze/Silver deduplication doc | **Exists** (`docs/pipelines/ceap_deduplication_bronze_silver.md`). |
| Delta pipelines for **new** RAW prefixes (`eventos`, `discursos`, …) | **Not documented** here as implemented; assume **out of scope** for the Functions codebase until notebooks/jobs exist. |

## 5. Known blockers

- No **build** blockers reported for current workspace; Azure validation (quotas, RBAC, `terraform plan`) is deploy-owned.
- **Possible doc inconsistency:** `docs/decisions.md` ADR-003 says “CEAP only”; code already has multiple domains — see `docs/current_state.md` section “Open issues”.

## 6. Previously observed technical issues (historical)

- Terraform **409** on duplicate ADLS paths between `base` and implicit app-created paths — mitigated by minimal `lakehouse_directories` list (historical context; do not reopen without review).
