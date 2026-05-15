# RAW layer (ADLS Gen2) — LegisFlow

## 1. Purpose

The RAW layer stores Open Data API JSON responses (and run metadata) in a **traceable**, **idempotent** way per `pipeline_run_id`, ready for downstream consumption (Bronze/Delta, etc.).

## 2. Storage

- **Account:** lakehouse storage account provisioned by Terraform `base` (name varies; reference via output `lakehouse_storage_account_name`).
- **Filesystem:** `lakehouse` (configurable via `LAKEHOUSE_FILESYSTEM_NAME`).
- **Directory creation:** `AdlsRawWriter` writes files with full paths; you **do not** need to pre-create every folder in Terraform `base` for new domains — ADLS materializes the tree on first write. `base` keeps a **minimal** set of initial directories (`lakehouse_directories` in `infra/terraform/base/main.tf`) for governance; avoid duplicating paths that only the app creates (historical 409 conflicts).

## 3. Prefix convention

General pattern:

```text
raw/camara/<domain>/api/...
```

**Implemented** examples in code:

| Domain | Main areas under `raw/camara/` |
|--------|----------------------------------|
| CEAP | `ceap/api/despesas/...`, `deputados/api/list/...` (deputy snapshot) |
| reference | `partidos`, `legislaturas`, `deputados`, `frentes`, `orgaos` (each with `api/list/...`) |
| votacoes | `votacoes/api/list`, `votacoes/api/votos`, `_metadata/runs/...` |
| proposicoes | `proposicoes/api/list`, `autores`, `tramitacoes`, `_metadata/...` |
| eventos | `eventos/api/list`, `deputados`, `orgaos`, `pauta`, `votacoes`, `_metadata/...` |
| institucional | `institucional/api/parents/...`, `orgaos|partidos|frentes|legislaturas/...`, `_metadata/...` |
| discursos | `discursos/api/discursos/...`, `discursos/api/deputies_snapshot/...`, `_metadata/...` |

Exact paths (including `pipeline_run_id=`, `execution_id=`, `page_n.json`) live in `shared/*_raw_manifest.py` and dispatcher/worker writers.

## 4. File naming

- **API pages:** typically `page_{n}.json` per paginated page.
- **Run manifest:** `metadata.json` (central contract in `shared/metadata.py`, version `1.0`).
- **Completion:** zero-byte `_SUCCESS` at the same logical level as the manifest when the run is **strictly** `COMPLETED` (rules in `validate_completed_metadata` / `PROFILE_*` profiles).

## 5. RAW partitioning strategy

Common combinations (not all domains use all of them):

- `reference_date=`, `reference_year=`, `reference_month=`
- `pipeline_run_id=`
- `execution_id=`
- Entity IDs: `deputado_id=`, `evento_id=`, `parent_id=`, `votacao_id=`, etc.

Goal: **avoid collisions** across parallel runs and allow **replay** without accidentally overwriting history (many writers explicitly overwrite the same path within the **same** run — controlled replay idempotency).

## 6. `pipeline_run_id`

- **Deterministic** run identifier per domain (e.g. `ceap_daily_YYYYMMDD`, `votacoes_microbatch_YYYYMMDDHHMM`, `reference_snapshot_YYYYMMDD`).
- Prefixes and formats declared in `shared/domain_catalog.py` and helpers (`*_run_id`, `*_reconciliation_run_id`).
- **Isolation:** shared tables (`IngestionState`, `IngestionControlApi2026`) use a distinct **PartitionKey** per domain so counts do not mix.

## 7. Traceability and audit

Per page (or per item in `dados`, depending on domain):

- **`_audit`:** ingestion metadata (`_pipeline_run_id`, `_execution_id`, `_source_endpoint`, `_raw_path`, `_ingested_at_utc`, …).
- **`_payload_hash`:** hash of the payload excluding audit fields.
- **`_record_uid` / `_record_hash`:** stable business key + record hash (`payload_and_record_hash_v1` strategy documented in `metadata.json` as `hash_strategy` when the manifest includes it).
- Business keys may use **dotted notation** (e.g. `proposicao_.id`, `parlamentar.id`) resolved in `shared/raw_audit.py`.

## 8. Poison queue

- Each domain has a **`*-poison`** sibling to the work queue.
- `*_poison_handler` triggers mark the affected partition **POISON** in `IngestionState` and adjust run counters in `IngestionControlApi2026` when `pipeline_run_id` belongs to that domain.

## 9. Replay (HTTP)

- `replay/<domain>` routes (see `fn_replay_*`): read `IngestionState`, filter by state (e.g. `FAILED,POISON`), re-send JSON messages to the work queue, reset state to **QUEUED**.
- They **do not** replace scheduled run semantics; they are for **manual** operational recovery.

## 10. Reset (HTTP)

- `legisflow/reset/*-pipeline-run` routes: targeted cleanup by `pipeline_run_id` (tables, queues, ADLS paths) with **dry-run** by default and flags `ENABLE_RESET_FUNCTIONS` / `ENABLE_<DOMAIN>_RESET_FUNCTION`.
- Intended use: **dev/test**; `pipeline_run_id` format validation per domain in `*_pipeline_reset_helpers.py`.

## 11. Code references

- Contract: `shared/metadata.py`
- ADLS writes: `shared/adls_writer.py`
- Envelope: `shared/raw_audit.py`
- Queue messages: `shared/queue_messages.py`
