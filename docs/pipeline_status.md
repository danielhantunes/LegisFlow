# Estado dos pipelines — LegisFlow

Legenda: **Funciona** = implementado no repositório com fluxo completo dispatcher → fila → worker → RAW; **Parcial** = parte implementada ou dependente de deploy/validação em Azure; **Não iniciado** = não há implementação correspondente no repo.

## 1. Ingestão API → RAW (Azure Functions)

| Pipeline | Estado no código | Notas |
|----------|------------------|--------|
| **CEAP 2026** (`ceap_api_2026_*`) | **Funciona** | Dispatcher com daily/reconciliation, snapshot deputados, reconciliação de manifest com `IngestionState`, filas CEAP. |
| **reference snapshot** | **Funciona** | Partidos, legislaturas, deputados, frentes, órgãos; timer + worker + poison + replay + reset. |
| **votacoes** | **Funciona** | Lista `/votacoes` + fanout `/votacoes/{id}/votos`; microbatch por minuto. |
| **proposicoes** | **Funciona** | Lista + fanout autores/tramitações; microbatch. |
| **eventos** | **Funciona** | Lista + fanout 4 sub-rotas; microbatch. |
| **institucional** | **Funciona** | Parents + fanout membros/líderes/mesa; run id diário. |
| **discursos** | **Funciona** | Snapshot `/deputados` no dispatcher + fanout `/deputados/{id}/discursos` com janela; microbatch. |
| **CEAP monólito** (`ceap_expenses_ingestion_timer`) | **Desativado** | Mantido no pacote; disabled por configuração exemplo e Terraform ingestion. |

## 2. Infraestrutura (Terraform)

| Módulo | Estado |
|--------|--------|
| `bootstrap-tfstate` | **Funciona** (workflow dedicado) — backend remoto. |
| `base` | **Funciona** — ADLS lakehouse, RG, diretórios iniciais. |
| `ingestion` | **Funciona** no código — Function Flex + filas + app settings para **todos** os domínios listados em `current_state.md`. |
| `databricks` | **Funciona** (workspace) — automação de notebooks/jobs fora do repo por decisão MVP (ver `docs/decisions.md`). |

**Parcial:** o estado “em produção” depende do último `terraform apply` / workflow e da branch (`terraform-ingestion-dev` aplica só a partir de `main`).

## 3. Qualidade e testes

| Área | Estado |
|------|--------|
| Testes unitários (`tests/`) | **Funciona** localmente (pytest) — sem Azure. |
| Testes E2E contra API Câmara + Azure | **Não** cobertos automaticamente neste repo (tratar como gap operacional). |

## 4. Consumo downstream (Bronze / Databricks)

| Área | Estado |
|------|--------|
| Documentação deduplicação CEAP Bronze/Silver | **Existe** (`docs/pipelines/ceap_deduplication_bronze_silver.md`). |
| Pipelines Delta para **novos** prefixos RAW (`eventos`, `discursos`, …) | **Não documentado** neste repositório como implementado; assumir **fora de escopo** do código de Functions até haver notebooks/jobs. |

## 5. Blockers conhecidos

- Nenhum **blocker de compilação** reportado no estado atual do workspace; validação em Azure (quotas, RBAC, `terraform plan`) é responsabilidade do deploy.
- **Possível inconsistência documental:** `docs/decisions.md` ADR-003 afirma “só CEAP”; o código já contém múltiplos domínios — ver `docs/current_state.md` secção “Problemas abertos”.

## 6. Problemas técnicos já observados (histórico)

- Conflito Terraform **409** em paths ADLS duplicados entre `base` e criação implícita pela app — mitigado por lista mínima de `lakehouse_directories` (contexto histórico; não reabrir sem revisão).
