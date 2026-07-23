-- =============================================================================
-- Data Warehouse dimensional (camada Gold) — Pipeline de Dados Governamentais do Ceará
-- Modelo estrela: fato_contrato e fato_empenho referenciando dim_tempo, dim_credor,
-- dim_orgao e dim_modalidade.
--
-- Convenções:
--   * Todas as dimensões usam surrogate keys (BIGSERIAL), nunca as chaves de negócio como PK.
--   * dim_credor implementa Slowly Changing Dimension tipo 2 (histórico de razão social).
--   * fato_contrato é particionada por ano (RANGE) para acelerar consultas históricas.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dw;
SET search_path TO dw, public;

-- -----------------------------------------------------------------------------
-- DIM_TEMPO
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    sk_tempo        BIGSERIAL PRIMARY KEY,
    data            DATE NOT NULL UNIQUE,
    ano             SMALLINT NOT NULL,
    trimestre       SMALLINT NOT NULL,
    mes             SMALLINT NOT NULL,
    dia             SMALLINT NOT NULL,
    dia_semana      SMALLINT NOT NULL,          -- 0=domingo ... 6=sábado
    nome_dia_semana TEXT NOT NULL,
    nome_mes        TEXT NOT NULL,
    ano_mes         CHAR(7) NOT NULL             -- 'YYYY-MM', útil para agrupamentos
);

-- -----------------------------------------------------------------------------
-- DIM_CREDOR (SCD2)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_credor (
    sk_credor       BIGSERIAL PRIMARY KEY,
    cpf_cnpj        TEXT NOT NULL,
    nome            TEXT NOT NULL,
    tipo_pessoa     CHAR(2) NOT NULL CHECK (tipo_pessoa IN ('PF', 'PJ')),
    historico_infringement INTEGER NOT NULL DEFAULT 0,
    data_inicio     DATE NOT NULL,
    data_fim        DATE,                        -- NULL = versão vigente
    versao_atual    BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS ix_dim_credor_cpf_cnpj ON dw.dim_credor (cpf_cnpj);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_credor_vigente
    ON dw.dim_credor (cpf_cnpj) WHERE versao_atual;

-- -----------------------------------------------------------------------------
-- DIM_ORGAO
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_orgao (
    sk_orgao            BIGSERIAL PRIMARY KEY,
    codigo_orgao        TEXT NOT NULL UNIQUE,
    nome                TEXT NOT NULL,
    cnpj                TEXT,
    tipo_administracao  TEXT,
    esfera              TEXT
);

-- -----------------------------------------------------------------------------
-- DIM_MODALIDADE
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_modalidade (
    sk_modalidade       BIGSERIAL PRIMARY KEY,
    descricao_modalidade TEXT NOT NULL,
    descricao_tipo      TEXT,
    UNIQUE (descricao_modalidade, descricao_tipo)
);

-- -----------------------------------------------------------------------------
-- FATO_CONTRATO (particionada por ano)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.fato_contrato (
    sk_contrato         BIGSERIAL,
    id_contrato_origem  BIGINT NOT NULL,
    num_contrato        TEXT,
    sk_tempo_assinatura BIGINT REFERENCES dw.dim_tempo (sk_tempo),
    sk_credor           BIGINT REFERENCES dw.dim_credor (sk_credor),
    sk_orgao            BIGINT REFERENCES dw.dim_orgao (sk_orgao),
    sk_modalidade       BIGINT REFERENCES dw.dim_modalidade (sk_modalidade),
    valor_contratado    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    valor_empenhado     NUMERIC(18, 2) NOT NULL DEFAULT 0,
    valor_pago          NUMERIC(18, 2) NOT NULL DEFAULT 0,
    status              TEXT,
    tipo_objeto         TEXT,
    flag_emergency      BOOLEAN NOT NULL DEFAULT FALSE,
    dias_vigencia       INTEGER,
    historico_credor_infringement INTEGER NOT NULL DEFAULT 0,
    score_anomalia      NUMERIC(5, 4),
    ano                 SMALLINT NOT NULL,
    PRIMARY KEY (sk_contrato, ano)
) PARTITION BY RANGE (ano);

CREATE TABLE IF NOT EXISTS dw.fato_contrato_2022 PARTITION OF dw.fato_contrato FOR VALUES FROM (2022) TO (2023);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2023 PARTITION OF dw.fato_contrato FOR VALUES FROM (2023) TO (2024);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2024 PARTITION OF dw.fato_contrato FOR VALUES FROM (2024) TO (2025);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2025 PARTITION OF dw.fato_contrato FOR VALUES FROM (2025) TO (2026);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2026 PARTITION OF dw.fato_contrato FOR VALUES FROM (2026) TO (2027);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2027 PARTITION OF dw.fato_contrato FOR VALUES FROM (2027) TO (2028);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_default PARTITION OF dw.fato_contrato DEFAULT;

CREATE INDEX IF NOT EXISTS ix_fato_contrato_credor ON dw.fato_contrato (sk_credor);
CREATE INDEX IF NOT EXISTS ix_fato_contrato_orgao ON dw.fato_contrato (sk_orgao);
CREATE UNIQUE INDEX IF NOT EXISTS ux_fato_contrato_origem ON dw.fato_contrato (id_contrato_origem, ano);

-- -----------------------------------------------------------------------------
-- FATO_EMPENHO
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.fato_empenho (
    sk_empenho          BIGSERIAL PRIMARY KEY,
    id_empenho_origem   BIGINT NOT NULL,
    codigo_empenho      TEXT,
    sk_tempo            BIGINT REFERENCES dw.dim_tempo (sk_tempo),
    sk_orgao            BIGINT REFERENCES dw.dim_orgao (sk_orgao),
    sk_credor           BIGINT REFERENCES dw.dim_credor (sk_credor),
    sk_contrato         BIGINT,                  -- ligação best-effort via cod_contrato (ver docs/dicionario_dados.md)
    valor               NUMERIC(18, 2) NOT NULL DEFAULT 0,
    modalidade          TEXT,
    ano                 SMALLINT NOT NULL,
    UNIQUE (id_empenho_origem, ano)
);
CREATE INDEX IF NOT EXISTS ix_fato_empenho_orgao ON dw.fato_empenho (sk_orgao);
CREATE INDEX IF NOT EXISTS ix_fato_empenho_contrato ON dw.fato_empenho (sk_contrato);

-- -----------------------------------------------------------------------------
-- FATO_ORDEM_BANCARIA (pagamentos efetivos — base do Modelo 2 de previsão)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.fato_ordem_bancaria (
    sk_ordem_bancaria   BIGSERIAL PRIMARY KEY,
    id_ob_origem        BIGINT NOT NULL,
    codigo              TEXT,
    sk_tempo            BIGINT REFERENCES dw.dim_tempo (sk_tempo),
    sk_orgao            BIGINT REFERENCES dw.dim_orgao (sk_orgao),
    sk_credor           BIGINT REFERENCES dw.dim_credor (sk_credor),
    valor               NUMERIC(18, 2) NOT NULL DEFAULT 0,
    tipo_ob             TEXT,
    ano                 SMALLINT NOT NULL,
    UNIQUE (id_ob_origem, ano)
);
CREATE INDEX IF NOT EXISTS ix_fato_ob_orgao ON dw.fato_ordem_bancaria (sk_orgao);
CREATE INDEX IF NOT EXISTS ix_fato_ob_tempo ON dw.fato_ordem_bancaria (sk_tempo);

-- -----------------------------------------------------------------------------
-- Tabelas de saída dos modelos de ML (Fluxo 2)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.previsao_pagamento_trimestral (
    sk_previsao         BIGSERIAL PRIMARY KEY,
    sk_orgao            BIGINT REFERENCES dw.dim_orgao (sk_orgao),
    trimestre_referencia TEXT NOT NULL,          -- ex.: '2026-Q3'
    valor_previsto       NUMERIC(18, 2) NOT NULL,
    intervalo_inferior   NUMERIC(18, 2),
    intervalo_superior   NUMERIC(18, 2),
    modelo               TEXT NOT NULL,
    data_geracao         TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.relatorio_insights (
    sk_relatorio         BIGSERIAL PRIMARY KEY,
    titulo               TEXT NOT NULL,
    conteudo_markdown    TEXT NOT NULL,
    data_geracao         TIMESTAMP NOT NULL DEFAULT now()
);
