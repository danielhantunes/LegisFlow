# Camada RAW (ADLS Gen2) — LegisFlow

## 1. Objetivo

A camada RAW guarda respostas JSON da API Dados Abertos (e metadados de corrida) de forma **rastreável** e **idempotente** por `pipeline_run_id`, preparando consumo downstream (Bronze/Delta, etc.).

## 2. Storage

- **Conta:** storage account do lakehouse provisionado pelo Terraform `base` (nome variável; referência via output `lakehouse_storage_account_name`).
- **Filesystem:** `lakehouse` (configurável por `LAKEHOUSE_FILESYSTEM_NAME`).
- **Criação de diretórios:** o `AdlsRawWriter` grava ficheiros com path completo; **não** é obrigatório pré-criar cada pasta no Terraform `base` para novos domínios — o ADLS materializa a árvore na primeira escrita. O `base` mantém um conjunto **mínimo** de diretórios iniciais (`lakehouse_directories` em `infra/terraform/base/main.tf`) por governança; evitar duplicar paths que já são criados só pela app (histórico de conflitos 409).

## 3. Convenção de prefixos

Padrão geral:

```text
raw/camara/<domínio>/api/...
```

Exemplos **implementados** no código:

| Domínio | Áreas principais sob `raw/camara/` |
|---------|--------------------------------------|
| CEAP | `ceap/api/despesas/...`, `deputados/api/list/...` (snapshot deputados) |
| reference | `partidos`, `legislaturas`, `deputados`, `frentes`, `orgaos` (cada um com `api/list/...`) |
| votacoes | `votacoes/api/list`, `votacoes/api/votos`, `_metadata/runs/...` |
| proposicoes | `proposicoes/api/list`, `autores`, `tramitacoes`, `_metadata/...` |
| eventos | `eventos/api/list`, `deputados`, `orgaos`, `pauta`, `votacoes`, `_metadata/...` |
| institucional | `institucional/api/parents/...`, `orgaos|partidos|frentes|legislaturas/...`, `_metadata/...` |
| discursos | `discursos/api/discursos/...`, `discursos/api/deputies_snapshot/...`, `_metadata/...` |

Os paths exatos (incluindo `pipeline_run_id=`, `execution_id=`, `page_n.json`) estão nos módulos `shared/*_raw_manifest.py` e nos writers dos dispatchers/workers.

## 4. Nomenclatura de ficheiros

- **Páginas API:** tipicamente `page_{n}.json` por página paginada.
- **Manifesto de corrida:** `metadata.json` (contrato central em `shared/metadata.py`, versão `1.0`).
- **Conclusão:** ficheiro zero-byte `_SUCCESS` no mesmo nível lógico do manifesto quando o run está **estritamente** `COMPLETED` (regras em `validate_completed_metadata` / perfis `PROFILE_*`).

## 5. Estratégia de particionamento (RAW)

Combinações comuns (nem todas aplicam a todos os domínios):

- `reference_date=`, `reference_year=`, `reference_month=`
- `pipeline_run_id=`
- `execution_id=`
- IDs de entidade: `deputado_id=`, `evento_id=`, `parent_id=`, `votacao_id=`, etc.

Objetivo: **evitar colisão** entre execuções paralelas e permitir **replay** sem sobrescrever histórico inadvertidamente (muitos writers fazem overwrite explícito do mesmo path dentro do **mesmo** run — comportamento idempotente de replay controlado).

## 6. `pipeline_run_id`

- Identificador **determinístico** da corrida por domínio (ex.: `ceap_daily_YYYYMMDD`, `votacoes_microbatch_YYYYMMDDHHMM`, `reference_snapshot_YYYYMMDD`).
- Prefixos e formatos declarados em `shared/domain_catalog.py` e helpers (`*_run_id`, `*_reconciliation_run_id`).
- **Isolamento:** tabelas partilhadas (`IngestionState`, `IngestionControlApi2026`) usam **PartitionKey** distinto por domínio para não misturar contagens.

## 7. Rastreabilidade e auditoria

Por página (ou por item em `dados`, conforme domínio):

- **`_audit`:** metadados de ingestão (`_pipeline_run_id`, `_execution_id`, `_source_endpoint`, `_raw_path`, `_ingested_at_utc`, …).
- **`_payload_hash`:** hash do payload sem campos de auditoria.
- **`_record_uid` / `_record_hash`:** chave de negócio estável + hash do registo (estratégia `payload_and_record_hash_v1` documentada em `metadata.json` como `hash_strategy` onde o manifesto o inclui).
- Chaves de negócio podem usar **notação pontilhada** (ex.: `proposicao_.id`, `parlamentar.id`) resolvida em `shared/raw_audit.py`.

## 8. Poison queue

- Cada domínio tem fila **`*-poison`** irmã da fila de trabalho.
- Triggers `*_poison_handler` marcam a partição afetada como **POISON** em `IngestionState` e ajustam contadores do run em `IngestionControlApi2026` quando o `pipeline_run_id` pertence ao domínio.

## 9. Replay (HTTP)

- Rotas `replay/<domínio>` (ver `fn_replay_*`): leem `IngestionState`, filtram por estados (ex.: `FAILED,POISON`), reenviam mensagens JSON para a fila work e repõem estado **QUEUED**.
- **Não** substituem a semântica de corridas agendadas; servem recuperação operacional manual.

## 10. Reset (HTTP)

- Rotas `legisflow/reset/*-pipeline-run`: limpeza dirigida por `pipeline_run_id` (tabelas, filas, paths ADLS) com **dry-run** por defeito e flags `ENABLE_RESET_FUNCTIONS` / `ENABLE_<DOMAIN>_RESET_FUNCTION`.
- Uso pretendido: **dev/test**; validação de formato de `pipeline_run_id` por domínio nos módulos `*_pipeline_reset_helpers.py`.

## 11. Referências de código

- Contrato: `shared/metadata.py`
- Escrita ADLS: `shared/adls_writer.py`
- Envelope: `shared/raw_audit.py`
- Mensagens de fila: `shared/queue_messages.py`
