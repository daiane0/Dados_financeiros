## Visão Geral

Este projeto é um Data Warehouse (DW) que tem como objetivo consolidar dados públicos disponibilizados pelo Banco Central, processá-los e disponibilizá-los para análises e relatórios. Estou utilizando Apache Airflow para orquestração do pipeline ETL (Extração, Transformação e Carga) e PySpark para processamento de dados em larga escala. O projeto está dividido em três principais componentes: extração de dados, processamento e armazenamento no Data Warehouse. O Data Warehouse é implementado em PostgreSQL.

### Componentes do Projeto

- **Extração**: Os dados são acessados a partir de diversas APIs e armazenados em um banco de dados PostgreSQL "finance_raw_data" antes do processamento.
- **Processamento**: Utilização de PySpark para transformar os dados.
- **Armazenamento**: Dados processados são carregados no Data Warehouse para análises e relatórios.

### Status Atual

Atualmente, estou trabalhando na modelagem do DW e coletando dados de mais algumas fontes, para começar a tratar e automatizar todo o pipeline dos dados.



