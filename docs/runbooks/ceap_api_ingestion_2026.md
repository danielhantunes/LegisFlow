# Runbook — Falha na ingestão CEAP API 2026

## Objetivo

Restaurar a ingestão resiliente de despesas CEAP via API da Câmara (Dados Abertos) para o ano **2026**, com recuperação granular por deputado e mês, sem recarregar histórico 2019–2025 (esse período usa ficheiros estáticos).

## Escopo

- **Incluído**: endpoint CEAP/despesas por `id_deputado`, `ano=2026`, `mes` 1–12; paginação por página; escrita em ADLS Raw com caminho determinístico; controlo em Azure Table Storage; filas de trabalho e poison.
- **Fora de escopo**: Bronze/Silver automáticos neste runbook (ver deduplicação em `docs/pipelines/ceap_deduplication_bronze_silver.md`).

## Componentes principais

| Componente | Função |
|------------|--------|
| `ceap_api_2026_dispatcher` | Timer: lista deputados (API) com cursor; enfileira no máximo `CEAP_DISPATCH_MAX_MESSAGES` unidades (deputado + mês) por execução. |
| `ceap_api_2026_worker` | Queue trigger: uma mensagem = um deputado + mês + 2026; paginação; checkpoint em tabela de controlo; grava Raw. |
| `ceap_api_2026_poison_handler` | Queue trigger na fila poison: marca unidade como `failed` na tabela após esgotar retries da fila. |
| `fn_replay_ceap_failed_messages` | HTTP (`authLevel=function`): reenfileira unidades por filtros (`statuses`, `id_deputado`, `ano`, `mes`, `full`). |
| Tabela `IngestionControlApi2026` | Modelo lógico **ingestion_control_api_2026** (nome físico alfanumérico por restrição do Azure). |
| Filas `ceap-api-2026-work` e `ceap-api-2026-work-poison` | Distribuição de trabalho e isolamento de falhas persistentes. |
| Application Insights | Telemetria e logs JSON estruturados (`execution_id`, `id_deputado`, `mes`, página, etc.). |

## Sintomas de falha

- Crescimento da fila poison ou mensagens visíveis na fila principal por longos períodos.
- Linhas na tabela de controlo com `status=failed` ou `retrying` estável.
- Lacunas no Raw em `raw/camara/ceap/api/ano=2026/mes=MM/deputado_id=ID/page=NNNN/response.json`.
- Erros 429/5xx recorrentes nos logs; 401/403 indicam identidade/RBAC.

## Consultar logs (Application Insights)

1. Portal Azure → Function App `func-legisflow-ingestion-dev` → Application Insights → **Logs**.
2. Procurar `traces` / custom logs com campos JSON: `execution_id`, `id_deputado`, `ano`, `mes`, `current_page`, `http_status_code`, `raw_path`.
3. Filtrar por nome da função (`ceap_api_2026_worker`, `ceap_api_2026_dispatcher`, etc.).

## Consultar a tabela de controlo

- Storage Account da **Function App** (conta dedicada às funções, não o ADLS): **Tables** → `IngestionControlApi2026`.
- Partição das unidades: `ceap`; `RowKey` ≈ `{ano}_{mes}_{id_deputado}`.
- Cursor do dispatcher: partição `_dispatch`, linha `ceap_api_2026` (`next_pagina`, `next_idx`, `next_mes`).

### Identificar `failed`

Filtrar entidades com `status` = `failed` ou consultar via Storage Explorer / Azure Data Explorer (se ligado).

## Verificar poison queue

- Storage Account da Function → **Queues** → `ceap-api-2026-work-poison`.
- Cada mensagem corresponde a uma unidade que falhou após `maxDequeueCount` (definido em `host.json`, default 5).
- O handler `ceap_api_2026_poison_handler` marca a linha de controlo como `failed` com mensagem explicativa.

## Dispatcher e unidades `failed`

O dispatcher **não** volta a enfileirar automaticamente unidades com `status=failed` (evita loops infinitos). Para incluir `failed` no ciclo automático de dispatch (uso raro), defina `CEAP_REPROCESS_DISPATCH=true` nas app settings da Function App.

## Reprocessar mensagens (replay)

Chamar a função HTTP com **function key** (Portal → Função → **Get Function Url** / chave).

Base URL: `https://<function-app>.azurewebsites.net/api/replay/ceap-api-2026`

Parâmetros de query:

| Parâmetro | Descrição |
|-----------|-----------|
| `code` | Obrigatório: function key. |
| `statuses` | Lista separada por vírgulas (default `failed,retrying`). |
| `endpoint` | Default `ceap`; use `*` para todos os endpoints na tabela. |
| `id_deputado`, `ano`, `mes` | Filtros opcionais. |
| `full` | `true` para repor `last_successful_page` a 0 e refazer todas as páginas (idempotente no Raw). |

Exemplos:

- Reprocessar todos os `failed`:  
  `.../api/replay/ceap-api-2026?code=<key>&statuses=failed`
- Um deputado:  
  `.../api/replay/ceap-api-2026?code=<key>&statuses=failed&id_deputado=204521`
- Um mês:  
  `.../api/replay/ceap-api-2026?code=<key>&statuses=failed&ano=2026&mes=3`

## Validar recuperação

1. Tabela: unidade passa a `success` com `finished_at` preenchido.
2. Raw: existem `response.json` para todas as páginas esperadas (última página tipicamente sem link `next`).
3. Fila principal: profundidade a descer; poison sem novos itens para a mesma unidade.

## Erros por código HTTP

| Código | Ação esperada |
|--------|----------------|
| 429, 5xx | Backoff na função (até 5 tentativas HTTP); em persistência de falha, retry da **fila**; depois poison se necessário. |
| 400 | Rever parâmetros (`ano`, `mes`, `id_deputado`); sem retry HTTP infinito; corrigir dados e usar replay com `full=true` se aplicável. |
| 401 / 403 | Rever RBAC da identidade gerida no ADLS e configuração da conta; não é problema de paginação. |
| 404 | Registo inválido ou recurso inexistente; validar `id_deputado`. |
| Timeout | Reduzir carga por mensagem já está feita (uma unidade); rever `functionTimeout` e limites da API. |

## Duplicidade de dados

- O Raw é **idempotente por caminho** (sobrescreve o mesmo ficheiro).
- Duplicados semânticos na Bronze/Silver: aplicar deduplicação descrita em `docs/pipelines/ceap_deduplication_bronze_silver.md`.

## Encerramento do incidente

- Filas estáveis; poison esvaziada ou mensagens conhecidas e registadas.
- Unidades críticas em `success` ou `skipped` com justificação.
- Entrada de incidente (manual) com causa raiz, ações e follow-up em deduplicação Bronze/Silver se necessário.

## Estratégia de tolerância a falhas (resumo)

- **Timeout / 5xx / 429**: retry HTTP com backoff; depois falha da execução → retry da fila; após max dequeue → poison + linha `failed`.
- **400 / 401 / 403 / 404**: marcar `failed` (ou concluir sem retry onde aplicável); corrigir à origem; replay HTTP.
- **Falha após gravar página**: reprocessamento retoma em `last_successful_page + 1` (ou `full=true` para refazer tudo).
- **Falha isolada**: não bloqueia outros deputados/meses (mensagens independentes).
