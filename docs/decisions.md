# Technical Decisions

## ADR-001 - Databricks Premium SKU for Unity Catalog

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

LegisFlow MVP requires Unity Catalog-aligned capabilities, including:

- storage credentials
- external locations
- centralized governance and metadata management

These requirements drive the workspace tier decision.

### Decision

Provision Azure Databricks workspace using **Premium** SKU in the MVP.

### Consequences

- Enables Unity Catalog governance foundation from day one.
- Slightly higher cost than lower tiers, but aligned with architecture goals.
- Avoids rework/migration risk when governance requirements are formalized.

## ADR-002 - Manual Databricks Asset Management in MVP

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

Notebooks, jobs, and DLT pipelines will be iterated rapidly during MVP discovery and model stabilization.

### Decision

For MVP, Databricks notebooks, jobs, and DLT pipelines are created and adjusted **manually** inside the workspace.  
The repository does **not** include `deploy-databricks-jobs.yml` in MVP.

### Consequences

- Faster iteration while business logic is still evolving.
- Lower CI/CD complexity in early phase.
- Asset deployment automation is deferred as a future improvement after pipeline stabilization.

## Future Improvement

After notebooks/jobs stabilize, implement automated Databricks asset deployment (jobs, notebooks, DLT) through dedicated CI/CD workflows with environment promotion controls.

## ADR-003 - Single Function App and CEAP-only Function in MVP

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

LegisFlow roadmap includes multiple ingestion endpoints, but MVP must keep operational complexity low and focus on CEAP ingestion reliability.

### Decision

- Use one shared Azure Function App in MVP: `func-legisflow-ingestion-dev`.
- Implement only one function now: `ceap_expenses_ingestion_timer`.
- Do not implement future functions yet:
  - `legislative_events_ingestion_timer`
  - `voting_microbatch_timer`
  - `parliamentary_fronts_ingestion_timer`
  - `propositions_lifecycle_ingestion_timer`
- Do not create deployment workflows for future functions in MVP.
- Future endpoints must be implemented later as separate functions inside the same Function App, not inside CEAP function code.

### Consequences

- Keeps MVP deployment and operations simpler while hardening CEAP ingestion.
- Preserves scalability path by standardizing on a single Function App boundary.
- Avoids premature coupling of unrelated endpoint logic into the CEAP function.

## ADR-004 - No GitHub Variables in MVP Workflows

- **Date**: 2026-04-30
- **Status**: Accepted

### Context

The MVP uses a single dev environment with stable resource names. Requiring GitHub Variables for non-sensitive values increases initial setup friction without clear benefit at this stage.

### Decision

- Use only GitHub Secrets in MVP workflows:
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_SUBSCRIPTION_ID`
- Keep non-sensitive backend and Function App values fixed in workflows for MVP.
- Reintroduce variables/inputs in a future multi-environment phase.

### Consequences

- Faster onboarding and simpler CI/CD setup.
- Lower risk of misconfigured repository variables.
- Less flexibility for resource renaming until multi-environment hardening phase.

## ADR-005 - CEAP API 2026 ingestion via queue and unit control table

- **Date**: 2026-05-03
- **Status**: Accepted

### Context

CEAP despesas per deputy can paginate heavily. A single timer execution that processes all deputies and months risks timeouts and poor fault isolation.

### Decision

- Use a **timer dispatcher** that enqueues bounded batches of work messages (`deputado` + `ano=2026` + `mes`).
- Use a **queue-triggered worker** for each unit, with **HTTP retry + queue retry**, checkpoints per page in a dedicated **Table Storage** control plane (`IngestionControlApi2026`), and **deterministic Raw paths** for idempotent replay.
- Keep the **legacy monolithic timer** in the codebase but **disabled by default** for emergency fallback only.

### Consequences

- Higher operational clarity (poison queue, replay HTTP function, runbook).
- Slightly more moving parts (queues, extra functions) than a single timer.
- Bronze/Silver must still deduplicate semantically; documented separately from Raw immutability policy.

## ADR-006 - CEAP API 2026 dispatcher dual mode (daily window + reconciliation) and partition state

- **Date**: 2026-05-04
- **Status**: Accepted

### Context

Ingestão CEAP 2026 precisa de atualização frequente (janela móvel) sem reprocessar o ano inteiro todos os dias, mas também de uma passagem larga e idempotente por competência (reconciliação mensal). O mesmo Function App não deve ganhar uma nova função dedicada só para reconciliação.

### Decision

- Um único timer `ceap_api_2026_dispatcher` escolhe o modo por **data UTC** (`CEAP_RECONCILIATION_DAY`): nesse dia só **reconciliation**; nos demais, **daily** (mês atual + meses anteriores conforme `CEAP_DAILY_LOOKBACK_MONTHS`), sempre filtrando meses futuros e respeitando `CEAP_TARGET_YEAR`.
- Registo de **pipeline run** em `IngestionControlApi2026` (`_runs`, `RowKey` = `pipeline_run_id`), com fase de enqueue limitada por `CEAP_MAX_TASKS_PER_DISPATCH`, conclusão da fase de enqueue via ticks ociosos consecutivos, e **lock** em `_locks/ceap_dispatcher_lock` (TTL 15 minutos).
- Estado por partição em **IngestionState** (`ceap_2026`), incluindo `current_pipeline_run_id` e `mode`; fila idempotente para o mesmo `pipeline_run_id` quando status já é QUEUED/RUNNING/SUCCESS.
- O **worker** único interpreta `mode` na mensagem; Raw inclui `pipeline_run_id` e `execution_id` no caminho para não sobrescrever blobs entre execuções.
- **Replay** HTTP continua só para falhas (FAILED/POISON), lendo IngestionState; não é o mecanismo da reconciliação automática.
- Meses futuros **nunca** são enfileirados.

### Consequences

- Operação previsível: no mesmo dia, após o run ficar COMPLETED, ticks extras só registam log e não duplicam mensagens.
- Reconciliação pesada é fatiada em muitas execuções do timer, sem mover trabalho de API para o dispatcher.
- Downstream (Databricks Bronze/Silver/Gold) permanece fora deste ADR; deduplicação analítica segue políticas já documentadas.
