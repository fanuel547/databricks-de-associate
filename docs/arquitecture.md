# Data Engineering Pipeline - Databricks (Projeto de Estudo)

## Visão Geral

Pipeline em arquitetura de medallion (Bronze, Silver, Gold) usando Databricks, Spark e Delta Lake.

- Bronze: de_associate.bronze_sales_raw
- Silver: de_associate.silver_sales_clean
- Gold: de_associate.gold_sales_summary

## Fluxo

1. Ingestão CSV → tabela Bronze (managed Delta)
2. Limpeza e tipagem → tabela Silver
3. Agregações por país e categoria → tabela Gold
4. Orquestração via Jobs (bronze → silver → gold → notify)

## Tecnologias

- Databricks Free Edition
- Spark SQL / PySpark
- Delta Lake
- Databricks Jobs/Workflows
