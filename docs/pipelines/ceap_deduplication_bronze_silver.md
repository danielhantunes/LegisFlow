# CEAP — Deduplicação na Bronze e Silver

## Contexto

A ingestão API 2026 é **idempotente ao nível do ficheiro Raw** (caminho fixo por página). Mesmo assim, reprocessamentos e sobreposições podem produzir o mesmo conteúdo em ficheiros distintos se o layout Raw mudar no futuro, ou registos repetidos dentro de payloads JSON. As camadas Bronze e Silver devem **deduplicar semanticamente** antes de métricas de negócio.

## Chaves sugeridas (API despesas CEAP)

Com base nos campos habituais da API de despesas por deputado, use uma chave composta estável quando disponível:

- `id_deputado` (ou identificador técnico equivalente no payload)
- `ano`, `mes` (já partilhados pela unidade de ingestão)
- `codDocumento`
- `dataDocumento`
- `urlDocumento`
- `numDocumento` (se existir no payload)
- `tipoDespesa` (se necessário para desambiguar)
- `valorDocumento` (opcional; cuidado com comparação float — preferir valor normalizado em string decimal)

## Padrão Delta / Spark (exemplo)

Na Bronze, após expandir o array `dados` de cada ficheiro Raw:

1. Calcular `record_hash` = hash canónico das colunas acima (normalizadas).
2. `dropDuplicates(["record_hash"])` ou `QUALIFY ROW_NUMBER() OVER (PARTITION BY record_hash ORDER BY _loaded_at DESC) = 1`.

## Padrão SQL (Delta Lake)

```sql
-- Exemplo ilustrativo: ajustar nomes de colunas ao schema real da API na Bronze.
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

## Notas

- A camada **Raw** mantém o payload integral; a deduplicação é responsabilidade da Bronze/Silver.
- Se a API alterar campos, versionar a lógica de `record_hash` e documentar mudança.
