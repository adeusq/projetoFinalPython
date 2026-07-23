# Dicionário de Dados — Data Warehouse (camada Gold)

Schema PostgreSQL: `dw`. Script de criação: [`sql/dw_schema.sql`](../sql/dw_schema.sql).

## dw.dim_tempo

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_tempo | BIGSERIAL PK | Surrogate key |
| data | DATE (único) | Data calendário |
| ano | SMALLINT | Ano |
| trimestre | SMALLINT | Trimestre (1-4) |
| mes | SMALLINT | Mês (1-12) |
| dia | SMALLINT | Dia do mês |
| dia_semana | SMALLINT | 0=segunda ... 6=domingo |
| nome_dia_semana | TEXT | Nome do dia da semana em português |
| nome_mes | TEXT | Nome do mês em português |
| ano_mes | CHAR(7) | `YYYY-MM`, útil para agrupamentos |

## dw.dim_credor (SCD2)

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_credor | BIGSERIAL PK | Surrogate key |
| cpf_cnpj | TEXT | Chave de negócio (somente dígitos) |
| nome | TEXT | Razão social / nome |
| tipo_pessoa | CHAR(2) | `PF` ou `PJ`, derivado do tamanho do documento |
| historico_infringement | INTEGER | Maior `infringement_status` observado para o credor |
| data_inicio | DATE | Início de vigência desta versão do registro |
| data_fim | DATE (nullable) | Fim de vigência (NULL = versão vigente) |
| versao_atual | BOOLEAN | TRUE para a versão vigente |

Fonte: união dos credores de `contratos` (API) e `empenhos` (Postgres origem), deduplicados
por `cpf_cnpj`. Uma nova versão é criada quando o **nome** associado ao mesmo CPF/CNPJ muda
(ex.: alteração de razão social); alterações apenas em `historico_infringement` atualizam a
versão vigente **sem** criar uma nova linha (não é considerado um atributo versionado).

## dw.dim_orgao

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_orgao | BIGSERIAL PK | Surrogate key |
| codigo_orgao | TEXT (único) | Código do órgão (`unidade_gestora.codigoorgao` / `contratos.cod_orgao`) |
| nome | TEXT | Nome do órgão |
| cnpj | TEXT | CNPJ do órgão |
| tipo_administracao | TEXT | Direta, Fundos, etc. |
| esfera | TEXT | Aproximado a partir de `unidade_gestora.nomepoder` (Legislativo/Executivo/Judiciário) |

**Nota de modelagem:** o banco de origem opera em duas granularidades distintas —
`empenhos.codigoug` referencia `unidade_gestora.codigo` (unidade gestora, granularidade
fina), enquanto `contratos.cod_orgao` referencia `unidade_gestora.codigoorgao` (órgão,
granularidade ampla). `dim_orgao` foi modelada na granularidade de **órgão** para permitir
que fatos originados de ambas as fontes convirjam para a mesma dimensão; empenhos são
"subidos" de unidade gestora para órgão via o mapa auxiliar construído em
`build_ug_to_orgao_map()` (`src/transformers/silver_to_gold.py`).

## dw.dim_modalidade

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_modalidade | BIGSERIAL PK | Surrogate key |
| descricao_modalidade | TEXT | Modalidade de licitação (pregão, dispensa, inexigibilidade etc.) |
| descricao_tipo | TEXT | Tipo do instrumento (ex.: `DESPESA.SEM.INSTRUMENTO.CONTRATUAL`) |

Fonte exclusiva: `contratos` (API). **Não** é usada por `fato_empenho` — o campo
`empenhos.modalidade` (`ORDINARIO`/`ESTIMATIVO`/`GLOBAL`) descreve o *tipo de empenho*, um
conceito diferente de modalidade de licitação, e é armazenado como texto livre em
`fato_empenho.modalidade` em vez de referenciar esta dimensão.

## dw.fato_contrato (particionada por `ano`, RANGE)

Grão: um contrato do portal Ceará Transparente.

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_contrato | BIGSERIAL | Surrogate key (parte da PK composta com `ano`) |
| id_contrato_origem | BIGINT | `contratos.id` na API de origem |
| num_contrato | TEXT | Número do contrato |
| sk_tempo_assinatura | FK → dim_tempo | Data de assinatura |
| sk_credor | FK → dim_credor | Credor/financiador |
| sk_orgao | FK → dim_orgao | Órgão contratante (`cod_orgao`) |
| sk_modalidade | FK → dim_modalidade | Modalidade de licitação |
| valor_contratado | NUMERIC(18,2) | `valor_contrato` |
| valor_empenhado | NUMERIC(18,2) | `calculated_valor_empenhado` |
| valor_pago | NUMERIC(18,2) | `calculated_valor_pago` |
| status | TEXT | `descricao_situacao` |
| tipo_objeto | TEXT | `tipo_objeto` |
| flag_emergency | BOOLEAN | `emergency` |
| dias_vigencia | INTEGER | `data_termino - data_inicio` em dias |
| historico_credor_infringement | INTEGER | `infringement_status` no momento da carga |
| score_anomalia | NUMERIC(5,4) | Score 0-1 do Modelo 1 (Isolation Forest); NULL até a 1ª execução da DAG de ML |
| ano | SMALLINT | Ano de assinatura — chave de particionamento |

## dw.fato_empenho

Grão: um empenho orçamentário.

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_empenho | BIGSERIAL PK | Surrogate key |
| id_empenho_origem | BIGINT | `empenhos.id` no Postgres de origem |
| codigo_empenho | TEXT | `empenhos.codigo` (ex.: `2022NE007525`) |
| sk_tempo | FK → dim_tempo | Data de emissão |
| sk_orgao | FK → dim_orgao | Órgão (resolvido via unidade gestora) |
| sk_credor | FK → dim_credor | Credor |
| sk_contrato | BIGINT (nullable) | Ligação **best-effort** com `fato_contrato` via `codcontrato`; não é FK íntegra — ver limitação abaixo |
| valor | NUMERIC(18,2) | Valor do empenho |
| modalidade | TEXT | Tipo de empenho (`ORDINARIO`/`ESTIMATIVO`/`GLOBAL`), texto livre |
| ano | SMALLINT | Ano do empenho |

**Limitação conhecida:** `empenhos.codcontrato` frequentemente vem zerado
(`'0000000000'`) ou em formato que não corresponde diretamente a `contratos.num_contrato`.
A ligação empenho→contrato é, portanto, best-effort e pode resultar em `sk_contrato` nulo
para a maior parte dos registros. Isso é uma característica dos dados de origem, não um
defeito do pipeline — recomenda-se validar com a equipe de controle antes de usar esse
vínculo para decisões críticas.

## dw.fato_ordem_bancaria

Grão: uma ordem bancária (pagamento efetivo). Base de treino do **Modelo 2**.

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_ordem_bancaria | BIGSERIAL PK | Surrogate key |
| id_ob_origem | BIGINT | `ordem_bancaria_orcamentaria.id` |
| codigo | TEXT | Código do documento |
| sk_tempo | FK → dim_tempo | Data de emissão |
| sk_orgao | FK → dim_orgao | Órgão pagador |
| sk_credor | FK → dim_credor | Credor beneficiário |
| valor | NUMERIC(18,2) | Valor pago |
| tipo_ob | TEXT | `tipoob` |
| ano | SMALLINT | Ano |

## dw.previsao_pagamento_trimestral

Saída do **Modelo 2**.

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_previsao | BIGSERIAL PK | Surrogate key |
| sk_orgao | FK → dim_orgao | Órgão previsto |
| trimestre_referencia | TEXT | Ex.: `2026-Q3` |
| valor_previsto | NUMERIC(18,2) | Mediana prevista (quantil 0.5) |
| intervalo_inferior | NUMERIC(18,2) | Quantil 0.1 |
| intervalo_superior | NUMERIC(18,2) | Quantil 0.9 |
| modelo | TEXT | `xgboost_quantile` ou `naive_persistence` (fallback quando o histórico é curto) |
| data_geracao | TIMESTAMP | Momento da geração |

## dw.relatorio_insights

Histórico dos relatórios narrativos gerados pelo componente de IA generativa
(`models/report_generator.py`).

| Coluna | Tipo | Descrição |
|---|---|---|
| sk_relatorio | BIGSERIAL PK | Surrogate key |
| titulo | TEXT | Título do relatório |
| conteudo_markdown | TEXT | Conteúdo completo em Markdown |
| data_geracao | TIMESTAMP | Momento da geração |
