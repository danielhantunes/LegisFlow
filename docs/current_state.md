# LegisFlow — estado técnico atual

Documento de continuidade para novos chats. **Última revisão alinhada ao código e Terraform no repositório** (sem garantir o que já foi aplicado em cada ambiente Azure).

## 1. Function App e runtime

- **Projeto de funções:** `functions/ceap_expenses_ingestion_timer/` (Python 3.11, bundle ~4, timeout 10 min, filas `maxDequeueCount` 5 — ver `host.json`).
- **Infra (Terraform `ingestion`):** `azurerm_function_app_flex_consumption` com `RAW_STORAGE_ACCOUNT_NAME` apontando para a conta ADLS do módulo `base`, `LAKEHOUSE_FILESYSTEM_NAME` = `lakehouse`, `CEAP_QUEUE_STORAGE` na conta de storage da própria Function (filas de trabalho).
- **Legado:** timer `ceap_expenses_ingestion_timer` (monólito) existe no pacote mas em `local.settings.json.example` está `AzureWebJobs.ceap_expenses_ingestion_timer.Disabled` = `true` e `CEAP_LEGACY_MONOLITH_ENABLED` = `false`. O fluxo ativo é **CEAP API 2026** + domínios novos.

## 2. Domínios implementados no código (por pasta de função)

Cada domínio (exceto CEAP “clássico”) segue o padrão: **dispatcher (timer)** → **fila work** → **worker (queue)** → **poison** + **HTTP replay** + **HTTP reset** (reset protegido por flags).

| Domínio | Dispatcher | Worker | Poison | Replay HTTP | Reset HTTP |
|---------|------------|--------|--------|-------------|------------|
| **CEAP** (`ceap`) | `ceap_api_2026_dispatcher` | `ceap_api_2026_worker` | `ceap_api_2026_poison_handler` | `fn_replay_ceap_failed_messages` | `fn_reset_ceap_pipeline_run` |
| **reference** | `reference_snapshot_dispatcher` | `reference_snapshot_worker` | `reference_snapshot_poison_handler` | `fn_replay_reference_failed_messages` | `fn_reset_reference_pipeline_run` |
| **votacoes** | `votacoes_dispatcher` | `votacoes_worker` | `votacoes_poison_handler` | `fn_replay_votacoes_failed_messages` | `fn_reset_votacoes_pipeline_run` |
| **proposicoes** | `proposicoes_daily_dispatcher`, `proposicoes_reconciliation_dispatcher` | `proposicoes_worker` | `proposicoes_poison_handler` | `fn_replay_proposicoes_failed_messages` | `fn_reset_proposicoes_pipeline_run` |
| **eventos** | `eventos_daily_dispatcher`, `eventos_reconciliation_dispatcher` | `eventos_worker` | `eventos_poison_handler` | `fn_replay_eventos_failed_messages` | `fn_reset_eventos_pipeline_run` |
| **institucional** | `institucional_dispatcher` | `institucional_worker` | `institucional_poison_handler` | `fn_replay_institucional_failed_messages` | `fn_reset_institucional_pipeline_run` |
| **discursos** | `discursos_dispatcher` | `discursos_worker` | `discursos_poison_handler` | `fn_replay_discursos_failed_messages` | `fn_reset_discursos_pipeline_run` |

**Catálogo declarativo:** `shared/domain_catalog.py` regista `ceap`, `reference`, `votacoes`, `proposicoes`, `eventos`, `institucional`, `discursos` (filas, partition keys em tabelas, prefixos de `pipeline_run_id`, endpoints). O CEAP em produção continua a usar módulos dedicados (`ceap_*`); o catálogo para CEAP é em parte **descritivo** (comentário no ficheiro).

## 3. Filas (nomes por defeito / exemplo)

Definidas em `local.settings.json.example` e criadas no Terraform `ingestion` (`azurerm_storage_queue` + `app_settings`):

| Domínio | Work | Poison |
|---------|------|--------|
| CEAP | `ceap-api-2026-work` | `ceap-api-2026-work-poison` |
| reference | `reference-snapshot-work` | `reference-snapshot-work-poison` |
| votacoes | `votacoes-api-work` | `votacoes-api-work-poison` |
| proposicoes | `proposicoes-api-work` | `proposicoes-api-work-poison` |
| eventos | `eventos-api-work` | `eventos-api-work-poison` |
| institucional | `institucional-api-work` | `institucional-api-work-poison` |
| discursos | `discursos-api-work` | `discursos-api-work-poison` |

## 4. Endpoints da API Câmara cobertos pelo código de ingestão

- **CEAP:** `/deputados/{id}/despesas` (+ snapshot `/deputados` no dispatcher CEAP — ver runbook CEAP).
- **reference:** `partidos`, `legislaturas`, `deputados`, `frentes`, `orgaos` (listagens paginadas por endpoint de snapshot).
- **votacoes:** `/votacoes` (lista) + `/votacoes/{id}/votos`.
- **proposicoes:** `/proposicoes` (lista com janela por data de tramitação) + `/proposicoes/{id}/autores` + `/proposicoes/{id}/tramitacoes`.
- **eventos:** `/eventos` (lista com janela) + `/eventos/{id}/deputados|orgaos|pauta|votacoes`.
- **institucional:** parents `/orgaos`, `/partidos`, `/frentes`, `/legislaturas` + sub-rotas `membros`, `lideres`, `mesa` conforme `domain_catalog`.
- **discursos:** `/deputados` (snapshot no dispatcher discursos) + `/deputados/{id}/discursos` com `dataInicio`/`dataFim` no worker.

Detalhe de paths HTTP está nos `function.json` (rotas `replay/*` e `legisflow/reset/*`).

## 5. Estado e controlo (Azure Tables)

- **`IngestionState`:** progresso por partição; partition keys **por domínio** (ex.: `ceap_2026`, `eventos_2026`, … — ver `DomainSpec.state_partition_key`).
- **`IngestionControlApi2026`:** runs, locks e snapshots; partition keys por domínio (`_runs`, `_runs_eventos`, …).
- **Locks:** dispatchers usam linha de lock por domínio (ex.: `ceap_dispatcher_lock`, `eventos_dispatcher_lock`) para evitar ticks concorrentes.
- **CEAP:** reconciliação de manifest/control com `IngestionState` (inclui partidas com `current_pipeline_run_id` **ou** `last_pipeline_run_id` no filtro de contagens, conforme implementação em `ceap_partition_state.py`).

## 6. Camada RAW (ADLS Gen2)

- **Filesystem:** `lakehouse` (default).
- **Prefixo comum:** `raw/camara/...` por domínio; ficheiros JSON por página + `metadata.json` e marcador `_SUCCESS` quando o run está **estritamente** concluído (contrato em `shared/metadata.py` e manifests por domínio).
- **Diretórios “vazios” no Terraform `base`:** lista mínima em `infra/terraform/base/main.tf` (`lakehouse_directories`); **novos ramos** `raw/camara/<domínio>/...` são criados **implicitamente** na primeira escrita pela Function (comportamento ADLS Gen2).

## 7. Manifests / metadata existentes (módulos)

- **Genérico:** `shared/metadata.py` (contrato v1.0, `hash_strategy`, `audit_fields_applied` onde aplicável).
- **CEAP:** `shared/ceap_raw_manifest.py`, `shared/deputies_snapshot.py`.
- **reference:** `shared/reference_raw_manifest.py` (e helpers de reset).
- **votacoes / proposicoes / eventos / institucional / discursos:** `shared/*_raw_manifest.py` correspondentes + `shared/*_run.py` para workers.

## 8. Testes automatizados

- Pasta `tests/` na raiz do repositório (pytest); cobertura inclui catálogo, metadata, audit envelope, CEAP reset/manifest, filas, e domínios `votacoes`, `proposicoes`, `eventos`, `institucional`, `discursos` (runs e reset helpers). **Não substituem** testes de integração em Azure.

## 9. CI/CD (workflows observados)

- `terraform-tfstate-backend-dev.yml` — backend de state.
- `terraform-base-dev.yml` — RG + ADLS + diretórios iniciais, etc.
- `terraform-ingestion-dev.yml` — Function App ingestion + filas + app settings (lê outputs do base).
- `terraform-databricks-dev.yml` — workspace Databricks.
- `deploy-function-ceap.yml` — deploy do pacote Python.

## 10. Problemas abertos / inconsistências documentadas

1. **`docs/decisions.md` ADR-003** ainda descreve “apenas CEAP”; o **código** já inclui vários domínios na mesma Function App — tratar ADR como histórico até revisão formal.
2. **Ambiente:** este ficheiro descreve o **repositório**; `terraform apply` / deploy podem estar à frente ou atrás do Git em cada subscription.
3. **Bronze/Silver/Gold** para os novos prefixos RAW: pipelines documentados no repo focam CEAP; **consumo Databricks** dos novos domínios não está descrito neste doc como concluído.
4. **Replay discursos:** payload pode reutilizar `last_window_date_*` da partição se existir; se não existir, o replay pode precisar de `date_start`/`date_end` explícitos (comportamento implementado no handler HTTP).

## 11. Decisões técnicas já incorporadas no código

- **Um catálogo** (`domain_catalog.py`) para nomes de filas, partition keys e contratos de `pipeline_run_id` dos domínios genéricos.
- **Auditoria RAW:** `_audit`, `_payload_hash`, `_record_uid`, `_record_hash`; chaves de negócio com paths aninhados (ex.: `deputado_.id`, `faseEvento.titulo`) via `shared/raw_audit.py`.
- **Reset HTTP:** desligado por defeito (`ENABLE_RESET_FUNCTIONS` e flags por domínio); reset admin é para dev/test.
- **Poison:** fila dedicada por domínio + handler que atualiza `IngestionState` e contadores do run quando aplicável.

## 12. Documentação histórica preservada

- `docs/runbooks/ceap_api_ingestion_2026.md`
- `docs/pipelines/ceap_deduplication_bronze_silver.md`
- `docs/decisions.md` (ADRs; ver nota sobre ADR-003 acima)
