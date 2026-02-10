# Databricks Data Engineer Associate - Hands-on Project

Este repositório contém um pequeno projecto aplicando boas práticas:

- Arquitetura Medallion (Bronze / Silver / Gold)
- Delta Lake (ACID, Time Travel, MERGE)
- Spark SQL
- Jobs & Workflows (orquestração)

## Estrutura

- `notebooks/` — Notebooks exportados do Databricks (formato `.py`)
- `sql/` — Scripts SQL (views e queries principais)
- `docs/` — Documentação da arquitetura e do pipeline
- `conf/` — Ficheiros de configuração (ex.: definição de jobs)
- `tests/` — Espaço reservado para testes futuros de data quality

## Como usar

1. Importar os notebooks `.py` para um workspace Databricks.
2. Ajustar nomes de schema/tabelas se necessário.
3. Executar na seguinte ordem:
   - `01_bronze_ingest`
   - `02_delta_operations`
   - `03_silver_sanitize`
   - `04_gold_aggregate`
   - `05_pipeline_notify_success`
