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
