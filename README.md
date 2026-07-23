# Pipeline de Dados Governamentais do Estado do Ceará

Trabalho Final — Curso de Python para Engenharia de Dados e IA.

Pipeline completo (ETL + Data Warehouse + Hadoop + Machine Learning) que integra o banco
PostgreSQL de execução orçamentária do Ceará com a API REST do portal Ceará Transparente,
orquestrado pelo Apache Airflow.

Veja também: [docs/arquitetura.md](docs/arquitetura.md) (diagrama) e
[docs/dicionario_dados.md](docs/dicionario_dados.md) (dicionário de dados do DW).

## Arquitetura (resumo)

```
PostgreSQL (origem, remoto) ──┐
                               ├─► Bronze (HDFS) ─► Silver (HDFS) ─► Gold (Postgres DW) ─► ML/IA
API REST Ceará Transparente ──┘
```

4 DAGs encadeadas via Airflow Datasets: `dag_bronze_extract` → `dag_silver_transform` →
`dag_gold_load` → `dag_ml_inference`. Detalhes em [docs/arquitetura.md](docs/arquitetura.md).

## Pré-requisitos

- Docker Desktop (com Docker Compose v2/v5)
- ~6 GB de RAM livres para o Docker (Airflow + Hadoop + Postgres x2)
- Acesso à internet (API pública do Ceará Transparente, banco de origem remoto e,
  opcionalmente, a API da OpenAI)

## 1. Configuração

```bash
cp .env.example .env
# Edite .env e preencha SRC_PG_PASSWORD com a senha do banco de origem fornecida pelo curso.
# Preencha também OPENAI_API_KEY se quiser o relatório narrado por LLM (ver passo 3).
```

As demais variáveis já vêm com valores padrão coerentes com o `docker-compose.yml`.

## 2. Subir o stack

```bash
docker compose up -d --build
```

Isso sobe: `postgres-dw`, `postgres-airflow`, `namenode` + `datanode` (HDFS),
`airflow-webserver`, `airflow-scheduler` e `jupyter`.

Aguarde 1-2 minutos para os healthchecks estabilizarem. Acompanhe com:

```bash
docker compose ps
docker compose logs -f airflow-webserver
```

O schema do Data Warehouse (`sql/dw_schema.sql`) é aplicado **automaticamente** na
primeira inicialização do `postgres-dw` (montado em `/docker-entrypoint-initdb.d/`).

## 3. Componente de IA Generativa (opcional)

Preencha `OPENAI_API_KEY` no `.env` para que o relatório final seja narrado por um LLM
(`gpt-4o-mini` por padrão, ajustável via `OPENAI_MODEL`). Sem a chave — ou se a chamada à
API falhar por qualquer motivo — o pipeline **não falha**: `models/report_generator.py`
recorre automaticamente a um relatório gerado por template determinístico (ver seção
"Componente de IA Generativa" abaixo). Esse componente é listado como opcional/avançado
no enunciado do trabalho.

## 4. Acessar as interfaces

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8080 | usuário/senha em `.env` (`AIRFLOW_ADMIN_USER`/`AIRFLOW_ADMIN_PASSWORD`, padrão `admin`/`admin`) |
| Jupyter | http://localhost:8888 | token em `.env` (`JUPYTER_TOKEN`, padrão `trabalho-final`) |
| HDFS NameNode UI | http://localhost:9870 | — |
| DW PostgreSQL | `localhost:5434` | `DW_PG_USER` / `DW_PG_PASSWORD` em `.env` |

## 5. Executar o pipeline

No Airflow (http://localhost:8080), habilite e dispare (▶) a DAG `dag_bronze_extract`.
As demais DAGs (`dag_silver_transform` → `dag_gold_load` → `dag_ml_inference`) são
disparadas **automaticamente** em sequência, via Airflow Datasets, assim que a anterior
conclui com sucesso — não é preciso acioná-las manualmente.

Acompanhe a execução em **Grid** (visão de cada DAG) ou no **Datasets** view do Airflow.

### Extração incremental

A primeira execução usa `EXTRACT_START_DATE` (`.env`) como início da janela. Ao final de
cada execução bem-sucedida, `dag_bronze_extract` grava a Airflow Variable
`last_extracted_date`, que passa a ser o início da janela na execução seguinte.

### Ajustando o volume de dados

O banco de origem tem ~1,4 milhão de linhas em `empenhos` e a API tem ~617 mil contratos
(desde 2007). Para manter o pipeline executável em ambiente de estudo, `.env` vem com uma
janela padrão de 6 meses (`EXTRACT_START_DATE=2026-01-01` a `EXTRACT_END_DATE=2026-06-30`,
~125 mil empenhos + ~29 mil contratos). Para um histórico mais robusto (em especial para o
Modelo 2 de previsão trimestral, que precisa de vários trimestres por órgão), aumente
`EXTRACT_START_DATE` (ex.: `2022-01-01`) e reprocesse.

## 6. Notebook de EDA e treinamento dos modelos

Abra `notebooks/eda_e_treinamento_ml.ipynb` no Jupyter (http://localhost:8888) **depois**
que `dag_gold_load` tiver concluído ao menos uma vez (o notebook lê diretamente do DW).

## 7. Rodar os testes unitários

```bash
python -m venv .venv && source .venv/bin/activate  # ou .venv\Scripts\activate no Windows
pip install -r requirements.txt
pytest tests/ -v
```

Os testes usam apenas mocks (sem tocar HDFS, banco ou rede) — cobrem a lógica de
paginação da API, normalização/deduplicação Bronze→Silver e montagem das dimensões/fatos
Silver→Gold.

## Fluxo 2 — Modelos de ML e IA

### Modelo 1 — Detecção de anomalias contratuais (`models/anomaly_detection.py`)
Isolation Forest não supervisionado sobre valor, modalidade, tipo de objeto, dias de
vigência, flag de emergência e histórico de infrações do credor. Produz um score 0-1
gravado em `dw.fato_contrato.score_anomalia`. Sem rótulos ground-truth, a avaliação é
qualitativa (ver notebook) — o score deve ser tratado como priorização para análise
humana, não como veredito automático.

### Modelo 2 — Previsão de pagamentos trimestrais (`models/payment_forecast.py`)
XGBoost com regressão quantílica (quantis 0.1/0.5/0.9) por órgão, usando como features o
trimestre, o ano eleitoral, o valor pago no trimestre anterior, o mesmo trimestre do ano
anterior e o valor contratado acumulado. Órgãos com histórico insuficiente (< 5
trimestres) recebem uma previsão de fallback por persistência do último valor. Resultados
em `dw.previsao_pagamento_trimestral`.

### Componente de IA Generativa (`models/report_generator.py`)
Usa a API da OpenAI (`OPENAI_API_KEY`/`OPENAI_MODEL` no `.env`) para narrar os resultados
dos dois modelos em linguagem acessível a gestores públicos. Sem chave configurada, ou se a
chamada à API falhar, recorre a um relatório por template determinístico — a DAG de ML
nunca falha por causa do LLM. Saída: arquivo em `reports/relatorio_<data>.md` e registro em
`dw.relatorio_insights`.

## Estrutura do repositório

```
├── dags/                    # DAGs do Airflow (bronze, silver, gold, ml_inference)
├── src/
│   ├── extractors/          # Postgres (origem) e API REST (contratos)
│   ├── transformers/        # Bronze -> Silver -> Gold
│   ├── loaders/              # Upsert/SCD2 no Data Warehouse
│   └── utils/                # config, HDFS client, validação, datasets Airflow
├── models/                   # Modelos de ML e gerador de relatório (IA generativa)
├── notebooks/                 # EDA e treinamento
├── sql/dw_schema.sql          # DDL do Data Warehouse dimensional
├── tests/                     # Testes unitários (mocks, sem I/O real)
├── docs/                       # Diagrama de arquitetura e dicionário de dados
├── docker/                     # Dockerfiles do Airflow e do Jupyter
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Fontes de dados

- **PostgreSQL de origem** (fornecido para este trabalho): `srv1236151.hstgr.cloud:5433`,
  banco `dados_publicos` — tabelas `empenhos`, `ordem_bancaria_orcamentaria`,
  `unidade_gestora`, `acao`, `categoria`, `elemento`, `fonte_recurso`, `funcao`, `grupo`,
  `natureza`.
- **API REST Ceará Transparente**: https://api-dados-abertos.cearatransparente.ce.gov.br
  (pública, sem autenticação) — endpoint `/transparencia/contratos/contratos`, paginado,
  filtrável por `data_assinatura_inicio`/`data_assinatura_fim` (formato `DD/MM/AAAA`).

## Limitações conhecidas (documentadas para transparência)

- A ligação `fato_empenho.sk_contrato` → `fato_contrato` é best-effort (ver
  [docs/dicionario_dados.md](docs/dicionario_dados.md)) porque `empenhos.codcontrato` nem
  sempre corresponde a `contratos.num_contrato` nos dados de origem.
- O Modelo 2 depende da profundidade do histórico carregado no DW; com a janela padrão de
  6 meses, a maioria dos órgãos cai no fallback de previsão ingênua (ver seção acima).
- `dim_orgao.esfera` é aproximada a partir de `unidade_gestora.nomepoder`, já que o banco
  de origem não possui um campo de "esfera" explícito nessa tabela.
