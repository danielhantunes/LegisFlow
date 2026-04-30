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
