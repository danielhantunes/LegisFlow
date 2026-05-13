# Backlog técnico — LegisFlow (priorizado)

Gerado a partir do estado do repositório. Itens **não** implicam que já exista issue tracking; ajustar prioridades ao roadmap do produto.

## P0 — Correções e alinhamento

1. **Rever ADR-003** em `docs/decisions.md`: o código já inclui múltiplos domínios na mesma Function App; atualizar ADR ou marcar como superseded com referência a `docs/current_state.md`.
2. **Atualizar `docs/architecture.md` secção histórica** se ainda referir “apenas CEAP” em sítios não atualizados pelo merge recente (grep por “Not implemented” / “CEAP-only”).
3. **Validar `terraform plan` (ingestion)** após alterações de filas/app settings em cada ambiente (dev/staging/prod).

## P1 — Validações pendentes (Azure)

1. **Deploy end-to-end:** `terraform-ingestion-dev` (plan → apply na `main`) + `deploy-function-ceap.yml` com commit que inclui todas as pastas de função.
2. **Smoke test por domínio:** um tick de dispatcher + uma mensagem de worker + verificar `metadata.json` e `_SUCCESS` em ADLS para run controlado.
3. **Filas poison:** simular falha (429/500) ou `maxDequeueCount` esgotado e confirmar escrita em poison + handler.
4. **Replay HTTP:** reprocessar uma partição `FAILED` de teste; confirmar estado `QUEUED` e novo `execution_id`.
5. **Reset HTTP:** `dry_run=true` primeiro; só então delete real em ambiente descartável.

## P2 — Melhorias de produto técnico

1. **Schedules por ambiente:** CRONs em modo “validação” (10/20 min); documentar passagem a cadência de produção por variável Terraform.
2. **Observabilidade:** dashboards Kusto / workbooks filtrando por `domain` e `pipeline_run_id` nos logs estruturados.
3. **Replay discursos:** garantir preenchimento de `last_window_date_start` / `last_window_date_end` em `IngestionState` no worker para replays sem query params (hoje o replay pode depender de overrides — ver código `fn_replay_discursos_failed_messages`).
4. **Limites:** revisar `MAX_LIST_PAGES` / `MAX_MESSAGES_PER_TICK` por domínio face ao volume real da API (risco de truncagem silenciosa se `links.next` existir além do cap).

## P3 — Extensão da plataforma

1. **Bronze/Delta:** definir layout e jobs Databricks para `raw/camara/{votacoes,eventos,proposicoes,institucional,discursos}/...` (hoje foco documental em CEAP).
2. **Testes de integração:** pipeline CI opcional com Azurite + mock HTTP ou subscription de teste read-only.
3. **Feature flags:** política clara para ativar novos dispatchers em produção (desligar por `AzureWebJobs.<FunctionName>.Disabled` até go-live).

## P4 — Dívida / higiene

1. **Nome do workflow de deploy:** `deploy-function-ceap.yml` deploya o **pacote** inteiro da app; renomear ou documentar que inclui todos os domínios.
2. **Duplicação de runbooks:** agregar links de `current_state.md` → runbooks por domínio quando existirem (hoje runbook detalhado só CEAP em `docs/runbooks/`).
