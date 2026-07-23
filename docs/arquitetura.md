# Arquitetura do Pipeline

Padrão Medallion (Bronze → Silver → Gold), orquestrado pelo Apache Airflow, com camadas
Bronze/Silver em HDFS e camada Gold em um Data Warehouse dimensional PostgreSQL.

```mermaid
flowchart LR
    subgraph Fontes["Fontes de Dados"]
        PG[("PostgreSQL de origem<br/>(remoto, fornecido pelo curso)<br/>empenhos, unidade_gestora, ...")]
        API["API REST<br/>Ceará Transparente<br/>(contratos)"]
    end

    subgraph Bronze["Camada Bronze — HDFS (raw)"]
        BPG["/data/bronze/postgres/*<br/>Parquet"]
        BAPI["/data/bronze/api/contratos/*<br/>JSON por página"]
    end

    subgraph Silver["Camada Silver — HDFS (staging)"]
        S["/data/silver/*<br/>Parquet particionado ano=/mes="]
    end

    subgraph Gold["Camada Gold — PostgreSQL DW"]
        DIM["dim_tempo, dim_credor (SCD2),<br/>dim_orgao, dim_modalidade"]
        FATO["fato_contrato, fato_empenho,<br/>fato_ordem_bancaria"]
    end

    subgraph ML["Fluxo 2 — ML / IA"]
        M1["Modelo 1<br/>Isolation Forest<br/>(anomalias contratuais)"]
        M2["Modelo 2<br/>XGBoost quantílico<br/>(previsão trimestral)"]
        LLM["API OpenAI (LLM)<br/>relatório narrativo"]
    end

    PG -->|DAG1: extract_postgres| BPG
    API -->|DAG1: extract_api| BAPI
    BPG -->|DAG1: validate_bronze| BPG
    BAPI -->|DAG1: validate_bronze| BAPI

    BPG -->|DAG2: bronze_to_silver| S
    BAPI -->|DAG2: bronze_to_silver| S

    S -->|DAG3: silver_to_gold + dw_loader| DIM
    S -->|DAG3: silver_to_gold + dw_loader| FATO
    DIM --- FATO

    FATO -->|DAG4: anomaly_detection| M1
    FATO -->|DAG4: payment_forecast| M2
    M1 -->|score_anomalia| FATO
    M2 -->|previsao_pagamento_trimestral| Gold
    M1 --> LLM
    M2 --> LLM
    LLM -->|relatorio_insights +<br/>reports/*.md| Saida["Relatório Final"]

    classDef airflow fill:#e8f0fe,stroke:#4285f4,color:#1a237e;
    class BPG,BAPI,S,DIM,FATO airflow;
```

## DAGs (Apache Airflow) e encadeamento via Datasets

```mermaid
flowchart LR
    A["dag_bronze_extract<br/>(schedule: @daily)"] -->|Dataset: bronze| B["dag_silver_transform<br/>(schedule: [bronze])"]
    B -->|Dataset: silver| C["dag_gold_load<br/>(schedule: [silver])"]
    C -->|Dataset: gold| D["dag_ml_inference<br/>(schedule: [gold])"]
```

Cada DAG só é disparada quando a anterior publica seu `Dataset` de saída (data-aware
scheduling do Airflow 2.x) — não há `ExternalTaskSensor` nem acoplamento por horário fixo.

## Componentes de infraestrutura (docker-compose)

| Serviço | Papel |
|---|---|
| `postgres-dw` | Data Warehouse (camada Gold) |
| `postgres-airflow` | Metastore interno do Airflow (não é fonte nem DW) |
| `namenode` / `datanode` | HDFS — camadas Bronze e Silver |
| `airflow-webserver` / `airflow-scheduler` / `airflow-init` | Orquestração |
| `jupyter` | EDA e treinamento dos modelos de ML |

O relatório narrativo (`models/report_generator.py`) chama a **API da OpenAI** diretamente
pela internet — não há container dedicado para isso, o que mantém o stack mais leve.

**Desvio intencional em relação ao enunciado:** o banco "de origem" não é containerizado —
usamos diretamente o PostgreSQL remoto fornecido para o curso
(`srv1236151.hstgr.cloud:5433/dados_publicos`), que já contém dados reais de execução
orçamentária do Ceará. Isso evita duplicar ~1,4 milhão de linhas localmente e mantém o
pipeline testado contra dados reais, não sintéticos.
