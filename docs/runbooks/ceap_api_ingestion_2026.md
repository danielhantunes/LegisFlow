# Runbook — Falha na ingestão CEAP API 2026

## Objetivo

Restaurar a ingestão resiliente de despesas CEAP via API da Câmara (Dados Abertos) para o ano configurado em **`CEAP_TARGET_YEAR`** (predefinição **2026**), com recuperação granular por deputado e mês, sem recarregar histórico 2019–2025 (esse período usa ficheiros estáticos).

## Escopo

- **Incluído**: endpoint CEAP/despesas por `id_deputado`, `ano`, `mes`; paginação; escrita em ADLS Raw; estado por partição em **IngestionState**; corridas agregadas e lock do dispatcher em **IngestionControlApi2026**; filas de trabalho e poison.
- **Fora de escopo**: Bronze/Silver automáticos neste runbook (ver deduplicação em `docs/pipelines/ceap_deduplication_bronze_silver.md`).

## Visão geral de modos (dispatcher)

O dispatcher executa em **duas fases por tick**:

1. **Fase A — snapshot de deputados**: garante uma cópia válida de `/deputados`. Antes de chamar a API, consulta `IngestionControlApi2026._snapshots/deputados_YYYYMMDD` e o marcador `_SUCCESS` no Raw. Se o snapshot da `reference_date` atual já está `COMPLETED` (e válido), **reaproveita-o em memória sem chamar a API**. Em modo `reconciliation`, se a `reference_date` atual ainda não tem snapshot completo, faz fallback para o snapshot `COMPLETED` mais recente. Caso contrário, chama `GET /deputados` paginado, persiste cada página em Raw, escreve `metadata.json` + `_SUCCESS` no fim e atualiza `_snapshots`. Isto evita chamar `/deputados` a cada 20 min.
2. **Fase B — enfileiramento CEAP**: percorre a lista de deputados in-memory em chunks de 100, gera mensagens CEAP por deputado × mês, respeita o limite de **`CEAP_MAX_TASKS_PER_DISPATCH`** (default 1000) e atualiza `IngestionState` por partição.

O timer **`ceap_api_2026_dispatcher`** (agenda em **`CEAP_API_2026_DISPATCH_SCHEDULE`**, predefinição a cada 20 minutos, UTC) escolhe o modo pela **data UTC**:

| Situação | Modo | Meses enfileirados | `pipeline_run_id` (exemplo) |
|----------|------|----------------------|-------------------------------|
| Dia ≠ `CEAP_RECONCILIATION_DAY` (default **25**) | **daily** | Mês corrente + meses anteriores na janela (`CEAP_DAILY_LOOKBACK_MONTHS`), sem meses futuros | `ceap_daily_YYYYMMDD` |
| Dia = `CEAP_RECONCILIATION_DAY` | **reconciliation** | De `CEAP_RECONCILIATION_START_MONTH` até ao mês atual do ano alvo | `ceap_reconciliation_YYYYMMDD` |

Nesse dia de reconciliação **não** corre o fluxo daily; só reconciliation. Após o run do dia estar **COMPLETED** (e, quando há tarefas, os workers terem concluído os contadores), execuções seguintes do timer no mesmo dia apenas registam log e **não** enfileiram de novo.

O dispatcher usa **lock** na tabela de controlo (`PartitionKey=_locks`, `RowKey=ceap_dispatcher_lock`, TTL ~15 minutos) para evitar execuções concorrentes.

## Componentes principais

| Componente | Função |
|------------|--------|
| `ceap_api_2026_dispatcher` | Timer: garante snapshot diário de `/deputados` em Raw + `IngestionControlApi2026._snapshots` (reusa quando `COMPLETED`); depois enfileira até **`CEAP_MAX_TASKS_PER_DISPATCH`** mensagens por execução (fallback: `CEAP_DISPATCH_MAX_MESSAGES`). Atualiza corridas em `_runs` e estado das partições em **IngestionState**. |
| `ceap_api_2026_worker` | Queue trigger: uma mensagem = deputado + ano + mês + `mode` + `pipeline_run_id`; paginação; checkpoint na **IngestionState**; grava Raw com caminho por run. |
| `ceap_api_2026_poison_handler` | Queue trigger na fila poison: marca a partição como **`POISON`** na **IngestionState** e incrementa falhas no run automatizado quando aplicável. |
| `fn_replay_ceap_failed_messages` | HTTP (`authLevel=function`, rota `replay/ceap-api-2026`): reenfileira partições a partir da **IngestionState** (não usa a tabela de unidades antiga). |
| `IngestionState` | Partição **`ceap_2026`**; `RowKey` = `despesas\|{id_deputado}\|{ano}\|{mes}`. Estados típicos: `QUEUED`, `RUNNING`, `SUCCESS`, `FAILED`, `POISON`, etc. |
| `IngestionControlApi2026` | **`_runs`**: controlo por `pipeline_run_id` (contadores, cursores do dispatcher, `enqueue_phase_complete`). **`_locks`**: lock do dispatcher. |
| Filas `ceap-api-2026-work` e `ceap-api-2026-work-poison` | Trabalho e falhas persistentes após retries (`host.json` / `maxDequeueCount`). |
| `AzureWebJobsStorage` | Connection string principal da Function App; usada pelos **queue triggers** (`ceap_api_2026_worker` e `ceap_api_2026_poison_handler`). |
| `CEAP_QUEUE_STORAGE` | Opcional para clientes de fila no código (dispatcher/replay); os listeners de trigger usam `AzureWebJobsStorage`. |
| Application Insights | Logs JSON estruturados (`execution_id`, `pipeline_run_id`, `mode`, `id_deputado`, `mes`, `raw_path`, etc.). |

### App settings relevantes (resumo)

| Setting | Papel |
|---------|--------|
| `CEAP_TARGET_YEAR` | Ano da API CEAP (default 2026). |
| `CEAP_API_2026_DISPATCH_SCHEDULE` | CRON do dispatcher (default `0 */20 * * * *`). |
| `CEAP_RECONCILIATION_DAY` | Dia UTC dedicado à reconciliação mensual (default 25). |
| `CEAP_DAILY_LOOKBACK_MONTHS` | Quantos meses para trás incluir na janela daily (default 1). |
| `CEAP_STALE_AFTER_MINUTES` | Janela para considerar `QUEUED`/`RUNNING` como órfãos (default 60; em dev pode usar 5 para teste rápido). |
| `CEAP_REFERENCE_TIMEZONE` | Fuso usado só no segmento `reference_date` dos blobs Raw de deputados (default `America/Sao_Paulo`). O modo daily/reconciliation continua por **data UTC**. |
| `CEAP_RECONCILIATION_START_MONTH` | Primeiro mês na reconciliação (default 1). |
| `CEAP_MAX_TASKS_PER_DISPATCH` | Limite de mensagens de despesas (CEAP) enfileiradas por execução do dispatcher (default 1000). Não afeta a coleta do snapshot de deputados. |
| `CEAP_API_QUEUE_NAME` / `CEAP_API_POISON_QUEUE_NAME` | Nomes das filas. |
| `INGESTION_STATE_TABLE` / `INGESTION_CONTROL_TABLE` | Nomes das tabelas (se sobrespostos no ambiente). |
| `CEAP_REPROCESS_QUEUE` | Se `true`, o worker pode voltar a processar uma partição já `SUCCESS` para o mesmo `pipeline_run_id` (uso raro). |

## Sintomas de falha

- Crescimento da fila poison ou mensagens presas na fila principal por longos períodos.
- Partições em **IngestionState** com `FAILED` estável, **`POISON`**, ou **`RUNNING`** sem progresso (por exemplo após incidente).
- Lacunas no Raw no prefixo abaixo (varia por `pipeline_run_id` e `execution_id`).
- Erros 429/5xx recorrentes nos logs; 401/403 indicam identidade/RBAC no ADLS.

## Caminho Raw (ADLS)

O worker grava ficheiros no formato:

```text
raw/camara/ceap/api/despesas/reference_year={ano}/reference_month={MM}/
  pipeline_run_id={pipeline_run_id}/execution_id={execution_id}/deputado_id={id}/page_{n}.json
```

Cada execução da função gera um **`execution_id`** novo; repetir a mesma competência noutro run produz prefixos distintos (sem sobrescrever blobs de outro run).

O dispatcher também grava snapshots da listagem de deputados (fonte de dispatch e base para futura dimensão):

```text
raw/camara/deputados/api/list/reference_date={YYYY-MM-DD}/
  pipeline_run_id={pipeline_run_id}/execution_id={snapshot_execution_id}/page_{n}.json
  metadata.json
  _SUCCESS                       # apenas quando o snapshot fica completo
```

O `{YYYY-MM-DD}` é a **data civil** no fuso `CEAP_REFERENCE_TIMEZONE` (predefinição `America/Sao_Paulo`), não a data UTC — assim as pastas alinham com o calendário brasileiro. O `snapshot_execution_id` é estável dentro do mesmo `pipeline_run_id` (todas as páginas do mesmo run vão para a mesma subpasta `execution_id=...`, mesmo quando o dispatcher percorre a paginação ao longo de múltiplos ticks).

### Validação de snapshot de deputados

Em cada tick o dispatcher escreve/atualiza `metadata.json` no nível `reference_date={YYYY-MM-DD}/` com o estado corrente. Quando a paginação do `/deputados` chega ao fim (resposta vazia), além de gravar a `metadata.json` com `status=COMPLETED`, é criado o marcador `_SUCCESS` na mesma pasta.

Campos do `metadata.json`:

| Campo | Descrição |
|-------|-----------|
| `endpoint` | Sempre `deputados`. |
| `reference_date` | Data civil (`CEAP_REFERENCE_TIMEZONE`) usada na pasta. |
| `reference_timezone` | Fuso aplicado para gerar `reference_date`. |
| `pipeline_run_id` | Run que gerou esta cópia. |
| `execution_id` | `snapshot_execution_id` (subpasta `execution_id=...`). |
| `status` | `IN_PROGRESS` ou `COMPLETED`. |
| `total_pages` / `record_count` | Acumulado do snapshot. |
| `files_written` | Igual a `total_pages` quando o snapshot está consistente. |
| `started_at` / `completed_at` | Marcas temporais (ISO UTC) do run. |
| `error_message` | Em branco quando não há falha. |

Um snapshot só é considerado **válido para consumo** quando, simultaneamente:

- existe `_SUCCESS` na pasta `reference_date={YYYY-MM-DD}/`;
- `metadata.status = COMPLETED`;
- `record_count > 0`;
- `total_pages > 0`;
- `files_written == total_pages`.

Se a pasta da data corrente não cumpre todos esses critérios, o dispatcher **não** a usa como referência: emite `warning` e procura a pasta `reference_date=...` mais recente que satisfaça as regras acima como **fallback**. O run no `IngestionControlApi2026._runs` regista a escolha:

| Campo no run | Significado |
|---------------|-------------|
| `deputies_pages_written` / `deputies_records_count` | Acumulado escrito para o snapshot do dia atual (mesmo que ainda incompleto). |
| `deputies_snapshot_status` | `IN_PROGRESS` enquanto o dispatcher está a paginar; `COMPLETED` após `_SUCCESS`. |
| `deputies_snapshot_first_execution_id` | `snapshot_execution_id` do snapshot que está a ser construído. |
| `deputies_snapshot_date` / `deputies_snapshot_path` / `deputies_snapshot_record_count` | Apontam para o snapshot **válido** que o run usa como referência (corrente quando completo, ou fallback). |
| `deputies_snapshot_source` | `current_run`, `fallback_completed` ou `none`. |
| `deputies_snapshot_completed_at` | Carimbo do momento em que `_SUCCESS` foi escrito. |

## Consultar logs (Application Insights)

1. Portal Azure → Function App (ex.: `func-legisflow-ingestion-dev`) → Application Insights → **Logs**.
2. Procurar `traces` com JSON: `pipeline_run_id`, `mode`, `execution_id`, `id_deputado`, `ano`, `mes`, `final_status`, `raw_path`, `pages_processed`, `record_count`.
3. Filtrar por nome da função (`ceap_api_2026_worker`, `ceap_api_2026_dispatcher`, `ceap_api_2026_poison_handler`).

## Consultar IngestionState (partição por deputado/mês)

- Storage Account da **Function App** → **Tables** → `IngestionState` (ou o valor de `INGESTION_STATE_TABLE`).
- **`PartitionKey`**: `ceap_2026`.
- **`RowKey`**: `despesas|{id_deputado}|{ano}|{mes}` (pipe como separador).
- Campos úteis: `status`, `current_pipeline_run_id`, `last_mode`, `last_successful_page`, `last_error`, `last_dispatched_at`, `last_finished_at`, `record_count`, `raw_path`.

### Identificar falhas

Filtrar por `status` em **`FAILED`**, **`POISON`**, ou inspeccionar **`RUNNING`** antigo (timestamp `last_started_at` / `updated_at`).

## Consultar IngestionControlApi2026 (corridas e lock)

- Mesma storage → Tabela `IngestionControlApi2026`.
- **`PartitionKey=_runs`**, **`RowKey`**: `ceap_daily_YYYYMMDD` ou `ceap_reconciliation_YYYYMMDD` — resumo consolidado da corrida.
  - Campos: `run_type`, `status`, `target_year`, `months_to_process`, `enqueue_phase_complete`, `total_tasks_expected`, `total_tasks_queued`, `total_tasks_success`, `total_tasks_failed`, `total_tasks_running`, `total_tasks_pending`, `total_tasks_poison`, `started_at`, `updated_at`, `completed_at`, `last_error`, cursores `next_pagina`, `next_idx`, `next_month_idx`.
  - Campos do snapshot de deputados: `deputies_pages_written`, `deputies_records_count`, `deputies_snapshot_status`, `deputies_snapshot_first_execution_id`, `deputies_snapshot_date`, `deputies_snapshot_path`, `deputies_snapshot_record_count`, `deputies_snapshot_source` (`reused_today` | `reused_fallback` | `created_today` | `none`), `deputies_snapshot_completed_at`.
  - Status possíveis: `STARTED`, `QUEUING`, `QUEUED`, `RUNNING`, `COMPLETED`, `PARTIAL`, `FAILED`.
  - **Sucesso confirmado** apenas quando: `status=COMPLETED`, `total_tasks_success == total_tasks_expected`, `total_tasks_failed=0`, `total_tasks_poison=0`, `total_tasks_running=0`, `total_tasks_pending=0`.
- **`PartitionKey=_locks`**, **`RowKey=ceap_dispatcher_lock`** — lock ativo do dispatcher (`locked_until`, `locked_by`).
- **`PartitionKey=_snapshots`**, **`RowKey=deputados_YYYYMMDD`** — controle do snapshot diário de `/deputados` (independente do run CEAP).
  - Campos: `endpoint=deputados`, `reference_date`, `status` (`IN_PROGRESS` | `COMPLETED` | `FAILED`), `pipeline_run_id`, `execution_id`, `started_at`, `completed_at`, `total_pages`, `record_count`, `raw_path`, `last_error`, `updated_at`.
  - O dispatcher só **reaproveita** o snapshot quando: `status=COMPLETED`, `record_count>0`, `total_pages>0`, `raw_path` preenchido **e** o marcador `_SUCCESS` existe em `raw_path/_SUCCESS`.
  - O modo `daily` reusa o snapshot do dia atual; se não existir, cria um novo.
  - O modo `reconciliation` reusa o snapshot completo mais recente (com `reference_date < hoje`) quando o do dia atual ainda não está completo; se nenhum estiver válido, cria um novo antes da Fase B.

### Verificar conclusão com um único comando

Daily:

```bash
az storage entity query \
  --account-name <storage_account> \
  --table-name IngestionControlApi2026 \
  --auth-mode key \
  --filter "PartitionKey eq '_runs' and RowKey eq 'ceap_daily_YYYYMMDD'" \
  -o table
```

Reconciliation:

```bash
az storage entity query \
  --account-name <storage_account> \
  --table-name IngestionControlApi2026 \
  --auth-mode key \
  --filter "PartitionKey eq '_runs' and RowKey eq 'ceap_reconciliation_YYYYMMDD'" \
  -o table
```

## Verificar poison queue

- Storage Account da Function → **Queues** → `ceap-api-2026-work-poison` (ou nome em `CEAP_API_POISON_QUEUE_NAME`).
- Cada mensagem corresponde a uma partição que falhou após esgotar retries da fila principal.
- O handler **`ceap_api_2026_poison_handler`** atualiza **IngestionState** para **`POISON`** (e o run automatizado, quando o `pipeline_run_id` é daily/reconciliation).

## Dispatcher automático vs partições em falha

- Para o **mesmo** `pipeline_run_id`, o dispatcher **não** duplica mensagens se a partição já está **`QUEUED`**, **`RUNNING`** ou **`SUCCESS`**.
- Partições **`FAILED`** ou **`POISON`** podem ser cobertas por um **novo** run (outro dia ou outro `pipeline_run_id`) quando o dispatcher as voltar a visitar, ou por **replay HTTP** manual (ver abaixo).
- A reconciliação mensal automática **não** substitui o replay: o replay é para recuperação operacional após correções ou investigação.

## Reprocessar mensagens (replay)

Chamar a função HTTP com **function key** (Portal → Função → **Get Function Url** / chave).

**Rota:** `https://<function-app>.azurewebsites.net/api/replay/ceap-api-2026`

Parâmetros de query:

| Parâmetro | Descrição |
|-----------|-----------|
| `code` | Obrigatório: function key. |
| `statuses` | Lista separada por vírgulas (predefinição **`FAILED,POISON`**). |
| `endpoint` | Predefinição `ceap`; use `*` para não filtrar por endpoint. |
| `id_deputado`, `ano`, `mes` | Filtros opcionais. |
| `full` | `true` para repor checkpoint de página (`last_successful_page` → 0) e refazer todas as páginas. |
| `pipeline_run_id` | Opcional: força o valor na mensagem e na partição; se omitido, usa `ceap_replay_YYYYMMDD` (run manual não atualiza contadores de corridas daily/reconciliation da mesma forma que o pipeline automático). |

Exemplos:

- Reprocessar todas as partições em falha:  
  `.../api/replay/ceap-api-2026?code=<key>&statuses=FAILED,POISON`
- Um deputado:  
  `.../api/replay/ceap-api-2026?code=<key>&statuses=FAILED&id_deputado=204521`
- Um mês, com re-fetch completo:  
  `.../api/replay/ceap-api-2026?code=<key>&ano=2026&mes=3&full=true`

## Validar recuperação

1. **IngestionState**: partição com `status=SUCCESS`, `last_finished_at` / `last_success_at` preenchidos quando aplicável.
2. **Raw**: existem `page_{n}.json` para todas as páginas esperadas (última resposta sem link `next` na API).
3. **IngestionControlApi2026** (`_runs`): para corridas automáticas, `total_tasks_success` / `total_tasks_failed` alinhados com o encerramento do run (`COMPLETED` ou `PARTIAL` quando há falhas).
4. **Filas**: profundidade da fila principal a descer; poison sem novos itens repetidos para a mesma partição após tratamento.

## Erros por código HTTP

| Código | Ação esperada |
|--------|----------------|
| 429, 5xx | Backoff na função (retry HTTP); depois retry da **fila**; em último caso **poison**. |
| 400 | Rever parâmetros (`ano`, `mes`, `id_deputado`); marcar falha terminal na partição; corrigir dados e usar **replay** com `full=true` se aplicável. |
| 401 / 403 | Rever RBAC da identidade gerida no ADLS e configuração da conta. |
| 404 | Recurso inválido ou inexistente; validar `id_deputado` / competência. |
| Timeout | Rever `functionTimeout` e limites da API; isolamento já é por mensagem (uma partição por mensagem). |

## Duplicidade e idempotência

- **Fila / estado**: o mesmo `pipeline_run_id` não gera mensagens duplicadas para a mesma partição enquanto está `QUEUED`/`RUNNING`/`SUCCESS`.
- **Raw**: caminhos incluem `pipeline_run_id` e `execution_id`; runs diferentes não sobrepõem os mesmos blobs.
- **Bronze/Silver** (quando existirem jobs): deduplicação semântica continua documentada em `docs/pipelines/ceap_deduplication_bronze_silver.md`.

## Encerramento do incidente

- Filas estáveis; poison tratada ou mensagens conhecidas registadas.
- Partições críticas em **`SUCCESS`** ou falhas aceites documentadas.
- Registo de causa raiz, ações e follow-up (incluindo alinhamento Bronze/Silver quando essa camada existir).

## Estratégia de tolerância a falhas (resumo)

- **Timeout / 5xx / 429**: retry HTTP com backoff; falha da execução → retry da fila; após max dequeue → **poison** + partição **`POISON`** em IngestionState.
- **400 / 401 / 403 / 404** (terminal onde aplicável): partição **`FAILED`** no worker; corrigir à origem; **replay HTTP**.
- **Falha após gravar página**: retoma em `last_successful_page + 1` para o mesmo `pipeline_run_id`, ou `full=true` no replay para refazer todas as páginas.
- **Falha isolada**: não bloqueia outros deputados/meses (mensagens independentes).
