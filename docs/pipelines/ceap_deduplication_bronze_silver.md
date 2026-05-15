# CEAP — Deduplication in Bronze and Silver

## Context

2026 API ingestion is **idempotent at the Raw file level** (fixed path per page). Re-runs and overlaps can still produce the same logical content in different files if the Raw layout changes later, or duplicate records inside JSON payloads. Bronze and Silver should **deduplicate semantically** before business metrics.

## Suggested keys (CEAP expenses API)

From typical fields in the per-deputy expenses API, use a stable composite key when available:

- `id_deputado` (or equivalent technical id in the payload)
- `ano`, `mes` (already shared by the ingestion unit)
- `codDocumento`
- `dataDocumento`
- `urlDocumento`
- `numDocumento` (if present in payload)
- `tipoDespesa` (if needed to disambiguate)
- `valorDocumento` (optional; be careful with float comparison — prefer normalized decimal string)

## Delta / Spark pattern (example)

In Bronze, after expanding the `dados` array from each Raw file:

1. Compute `record_hash` = canonical hash of the columns above (normalized).
2. `dropDuplicates(["record_hash"])` or `QUALIFY ROW_NUMBER() OVER (PARTITION BY record_hash ORDER BY _loaded_at DESC) = 1`.

## SQL pattern (Delta Lake)

```sql
-- Illustrative: adjust column names to the real API schema in Bronze.
CREATE OR REPLACE TEMP VIEW ceap_dedup AS
SELECT *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id_deputado, ano, mes, codDocumento, dataDocumento, urlDocumento, COALESCE(numDocumento, '')
      ORDER BY _loaded_at DESC
    ) AS rn
  FROM bronze.ceap_despesas_api
) t
WHERE rn = 1;
```

## Notes

- **Raw** keeps the full payload; deduplication is Bronze/Silver responsibility.
- If the API fields change, version the `record_hash` logic and document the change.
