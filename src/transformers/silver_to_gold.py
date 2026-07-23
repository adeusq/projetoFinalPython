"""Transformação Silver -> Gold: monta os DataFrames do modelo dimensional a partir da
camada Silver. Não toca o banco — apenas produz as tabelas de dimensão e fato em memória.
A carga efetiva (upsert/SCD2) fica em `src/loaders/dw_loader.py`.
"""
from __future__ import annotations

import logging

import pandas as pd

from src.utils.config import HDFS_SILVER_PATH
from src.utils.hdfs_client import read_all_partitions, read_dataframe_parquet

logger = logging.getLogger(__name__)

DIAS_SEMANA_PT = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
MESES_PT = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
]


def load_silver() -> dict[str, pd.DataFrame]:
    return {
        "empenhos": read_all_partitions(f"{HDFS_SILVER_PATH}/empenhos"),
        "ordem_bancaria": read_all_partitions(f"{HDFS_SILVER_PATH}/ordem_bancaria_orcamentaria"),
        "contratos": read_all_partitions(f"{HDFS_SILVER_PATH}/contratos"),
        "unidade_gestora": read_dataframe_parquet(f"{HDFS_SILVER_PATH}/unidade_gestora/data.parquet"),
    }


# --------------------------------------------------------------------------- #
# DIM_TEMPO
# --------------------------------------------------------------------------- #
def build_dim_tempo(*date_series: pd.Series) -> pd.DataFrame:
    all_dates = pd.concat(date_series).dropna().dt.normalize().unique()
    dim = pd.DataFrame({"data": pd.to_datetime(sorted(all_dates))})
    dim["ano"] = dim["data"].dt.year
    dim["trimestre"] = dim["data"].dt.quarter
    dim["mes"] = dim["data"].dt.month
    dim["dia"] = dim["data"].dt.day
    dim["dia_semana"] = dim["data"].dt.weekday
    dim["nome_dia_semana"] = dim["dia_semana"].map(lambda i: DIAS_SEMANA_PT[i])
    dim["nome_mes"] = (dim["mes"] - 1).map(lambda i: MESES_PT[i])
    dim["ano_mes"] = dim["data"].dt.strftime("%Y-%m")
    return dim


# --------------------------------------------------------------------------- #
# DIM_ORGAO
#
# Nota de modelagem: `empenhos.codigoug` referencia `unidade_gestora.codigo` (unidade
# gestora, granularidade fina), enquanto `contratos.cod_orgao` referencia
# `unidade_gestora.codigoorgao` (órgão, granularidade mais ampla). dim_orgao é modelada
# na granularidade de órgão (codigoorgao) para permitir que as duas fontes convirjam;
# empenhos são "subidos" de unidade gestora para órgão via o mapa `codigo -> codigoorgao`.
# --------------------------------------------------------------------------- #
def _mode_or_none(series: pd.Series):
    modes = series.mode()
    return modes.iat[0] if not modes.empty else None


def build_dim_orgao(unidade_gestora: pd.DataFrame) -> pd.DataFrame:
    """Para cada `codigoorgao`, prefere os atributos do registro "sede" (a unidade
    gestora cujo próprio `codigo` é igual ao `codigoorgao`); na ausência de sede, usa a
    moda dos atributos entre as unidades gestoras daquele órgão."""
    ug = unidade_gestora.dropna(subset=["codigoorgao"])

    fallback = (
        ug.groupby("codigoorgao")
        .agg(
            nome=("titulo", _mode_or_none),
            cnpj=("cnpj", _mode_or_none),
            tipo_administracao=("tipoadministracao", _mode_or_none),
            esfera=("nomepoder", _mode_or_none),
        )
    )

    sede = (
        ug[ug["codigo"] == ug["codigoorgao"]]
        .drop_duplicates(subset=["codigoorgao"])
        .set_index("codigoorgao")[["titulo", "cnpj", "tipoadministracao", "nomepoder"]]
        .rename(columns={
            "titulo": "nome", "tipoadministracao": "tipo_administracao", "nomepoder": "esfera",
        })
    )

    dim = fallback.copy()
    dim.update(sede)
    return dim.reset_index().rename(columns={"codigoorgao": "codigo_orgao"})


def build_ug_to_orgao_map(unidade_gestora: pd.DataFrame) -> pd.DataFrame:
    """Mapa auxiliar codigo_ug (unidade gestora) -> codigo_orgao, usado para resolver
    dim_orgao a partir de `empenhos.codigoug` / `ordem_bancaria.codigoug`.

    A coluna de junção é renomeada para `codigo_ug` (em vez de `codigo`) porque tanto
    `empenhos` quanto `ordem_bancaria_orcamentaria` já possuem sua própria coluna `codigo`
    (o número do documento) — manter o nome original causaria colisão no merge.

    Algumas unidades gestoras trocaram de órgão superior ao longo dos anos (mesmo
    `codigo`, `codigoorgao` diferente em snapshots de anos distintos). Sem desambiguar,
    o merge posterior (many-to-one esperado) vira many-to-many e duplica linhas de
    fato_empenho/fato_ordem_bancaria — por isso ficamos com a associação do ano mais
    recente por `codigo`.
    """
    ug_ordenada = unidade_gestora.sort_values("ano")
    return (
        ug_ordenada[["codigo", "ano", "codigoorgao"]]
        .drop_duplicates(subset=["codigo"], keep="last")
        .drop(columns=["ano"])
        .rename(columns={"codigo": "codigo_ug", "codigoorgao": "codigo_orgao"})
    )


# --------------------------------------------------------------------------- #
# DIM_MODALIDADE (licitação, a partir de contratos)
# --------------------------------------------------------------------------- #
def build_dim_modalidade(contratos: pd.DataFrame) -> pd.DataFrame:
    dim = (
        contratos[["descricao_modalidade", "descricao_tipo"]]
        .drop_duplicates()
        .dropna(subset=["descricao_modalidade"])
        .reset_index(drop=True)
    )
    return dim


# --------------------------------------------------------------------------- #
# DIM_CREDOR (união de credores de contratos e de empenhos)
# --------------------------------------------------------------------------- #
def build_dim_credor(contratos: pd.DataFrame, empenhos: pd.DataFrame) -> pd.DataFrame:
    from_contratos = contratos[["cpf_cnpj_financiador", "descricao_nome_credor", "infringement_status"]].rename(
        columns={"cpf_cnpj_financiador": "cpf_cnpj", "descricao_nome_credor": "nome",
                 "infringement_status": "historico_infringement"}
    )
    from_empenhos = empenhos[["codigocredor", "nomecredor"]].rename(
        columns={"codigocredor": "cpf_cnpj", "nomecredor": "nome"}
    )
    from_empenhos["historico_infringement"] = 0

    combined = pd.concat([from_contratos, from_empenhos], ignore_index=True)
    combined = combined.dropna(subset=["cpf_cnpj"])
    combined["historico_infringement"] = pd.to_numeric(
        combined["historico_infringement"], errors="coerce"
    ).fillna(0).astype(int)

    dim = (
        combined.sort_values("historico_infringement")
        .groupby("cpf_cnpj", as_index=False)
        .agg(nome=("nome", "last"), historico_infringement=("historico_infringement", "max"))
    )
    dim["tipo_pessoa"] = dim["cpf_cnpj"].map(lambda c: "PJ" if len(str(c)) > 11 else "PF")
    return dim


# --------------------------------------------------------------------------- #
# FATO_CONTRATO
# --------------------------------------------------------------------------- #
def build_fato_contrato(contratos: pd.DataFrame) -> pd.DataFrame:
    df = contratos.copy()
    df["dias_vigencia"] = (df["data_termino"] - df["data_inicio"]).dt.days
    fato = pd.DataFrame({
        "id_contrato_origem": df["id"],
        "num_contrato": df.get("num_contrato"),
        "data_assinatura": df["data_assinatura"].dt.normalize(),
        "cpf_cnpj": df["cpf_cnpj_financiador"],
        "codigo_orgao": df["cod_orgao"],
        "descricao_modalidade": df["descricao_modalidade"],
        "descricao_tipo": df["descricao_tipo"],
        "valor_contratado": df["valor_contrato"],
        "valor_empenhado": df.get("calculated_valor_empenhado", 0.0),
        "valor_pago": df.get("calculated_valor_pago", 0.0),
        "status": df.get("descricao_situacao"),
        "tipo_objeto": df.get("tipo_objeto"),
        "flag_emergency": df.get("emergency", False).fillna(False).astype(bool),
        "dias_vigencia": df["dias_vigencia"],
        "historico_credor_infringement": pd.to_numeric(df.get("infringement_status", 0), errors="coerce").fillna(0).astype(int),
        "ano": df["data_assinatura"].dt.year,
    })
    return fato.dropna(subset=["id_contrato_origem", "ano"])


# --------------------------------------------------------------------------- #
# FATO_EMPENHO
# --------------------------------------------------------------------------- #
def build_fato_empenho(empenhos: pd.DataFrame, ug_to_orgao: pd.DataFrame) -> pd.DataFrame:
    df = empenhos.merge(ug_to_orgao, left_on="codigoug", right_on="codigo_ug", how="left")
    fato = pd.DataFrame({
        "id_empenho_origem": df["id"],
        "codigo_empenho": df["codigo"],
        "data_emissao": df["dataemissao"].dt.normalize(),
        "codigo_orgao": df["codigo_orgao"],
        "cpf_cnpj": df["codigocredor"],
        "codcontrato": df.get("codcontrato"),
        "valor": df["valor"],
        "modalidade": df.get("modalidade"),
        "ano": df["ano"],
    })
    return fato.dropna(subset=["id_empenho_origem", "ano"])


def build_fato_ordem_bancaria(ordem_bancaria: pd.DataFrame, ug_to_orgao: pd.DataFrame) -> pd.DataFrame:
    df = ordem_bancaria.merge(ug_to_orgao, left_on="codigoug", right_on="codigo_ug", how="left")
    fato = pd.DataFrame({
        "id_ob_origem": df["id"],
        "codigo": df["codigo"],
        "data_emissao": df["dataemissao"].dt.normalize(),
        "codigo_orgao": df["codigo_orgao"],
        "cpf_cnpj": df["codigocredor"],
        "valor": df["valor"],
        "tipo_ob": df.get("tipoob"),
        "ano": df["ano"],
    })
    return fato.dropna(subset=["id_ob_origem", "ano"])


def run_silver_to_gold() -> dict:
    silver = load_silver()
    dim_tempo = build_dim_tempo(
        silver["contratos"]["data_assinatura"],
        silver["empenhos"]["dataemissao"],
        silver["ordem_bancaria"]["dataemissao"],
    )
    dim_orgao = build_dim_orgao(silver["unidade_gestora"])
    ug_to_orgao = build_ug_to_orgao_map(silver["unidade_gestora"])
    dim_modalidade = build_dim_modalidade(silver["contratos"])
    dim_credor = build_dim_credor(silver["contratos"], silver["empenhos"])
    fato_contrato = build_fato_contrato(silver["contratos"])
    fato_empenho = build_fato_empenho(silver["empenhos"], ug_to_orgao)
    fato_ordem_bancaria = build_fato_ordem_bancaria(silver["ordem_bancaria"], ug_to_orgao)

    logger.info(
        "Gold montado: dim_tempo=%d dim_orgao=%d dim_modalidade=%d dim_credor=%d "
        "fato_contrato=%d fato_empenho=%d fato_ordem_bancaria=%d",
        len(dim_tempo), len(dim_orgao), len(dim_modalidade), len(dim_credor),
        len(fato_contrato), len(fato_empenho), len(fato_ordem_bancaria),
    )
    return {
        "dim_tempo": dim_tempo,
        "dim_orgao": dim_orgao,
        "dim_modalidade": dim_modalidade,
        "dim_credor": dim_credor,
        "fato_contrato": fato_contrato,
        "fato_empenho": fato_empenho,
        "fato_ordem_bancaria": fato_ordem_bancaria,
    }
