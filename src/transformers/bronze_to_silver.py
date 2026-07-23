"""Transformação Bronze -> Silver.

Aplica conversão de tipos, deduplicação (mantendo a versão mais recente por `updated_at`),
normalização de CPF/CNPJ e enriquecimento (junção de empenhos com unidade_gestora). O
resultado é persistido em Parquet particionado por ano/mês na camada Silver do HDFS.
"""
from __future__ import annotations

import json
import logging
import re

import pandas as pd

from src.utils.config import HDFS_BRONZE_PATH, HDFS_SILVER_PATH
from src.utils.hdfs_client import (
    find_all_run_dirs,
    list_dir,
    read_all_partitions,
    read_bytes,
    write_dataframe_parquet,
)

logger = logging.getLogger(__name__)


def _only_digits(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits or None


def _parse_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Converte para datetime64 naive (sem timezone).

    As datas do Postgres de origem já vêm sem timezone, mas as da API de contratos vêm
    com offset explícito (ex.: '-03:00'). Sem normalizar, `pd.concat` entre uma coluna
    tz-aware e uma tz-naive degrada para dtype `object`, quebrando o `.dt` accessor em
    `build_dim_tempo` (silver_to_gold.py).
    """
    for col in columns:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], format="mixed", errors="coerce")
            if isinstance(parsed.dtype, pd.DatetimeTZDtype):
                parsed = parsed.dt.tz_localize(None)
            df[col] = parsed
    return df


def _dedupe_latest(df: pd.DataFrame, key_columns: list[str], order_column: str = "updated_at") -> pd.DataFrame:
    if order_column not in df.columns:
        return df.drop_duplicates(subset=key_columns, keep="last")
    return (
        df.sort_values(order_column)
        .drop_duplicates(subset=key_columns, keep="last")
        .reset_index(drop=True)
    )


# --------------------------------------------------------------------------- #
# Leitura da Bronze (Postgres)
# --------------------------------------------------------------------------- #
def load_postgres_table_history(table: str) -> pd.DataFrame:
    """Consolida TODAS as extrações incrementais (todo run_id) já gravadas na Bronze para
    `table`. A extração é incremental por janela de data — ler apenas o run_id mais
    recente perderia dados de execuções anteriores; a deduplicação por chave natural +
    `updated_at` (`_dedupe_latest`) resolve sobreposições entre execuções."""
    base_dir = f"{HDFS_BRONZE_PATH}/postgres/{table}"
    return read_all_partitions(base_dir)


def load_contratos_history() -> pd.DataFrame:
    """Consolida os contratos de TODOS os run_id já extraídos da API (mesma razão de
    `load_postgres_table_history`)."""
    base_dir = f"{HDFS_BRONZE_PATH}/api/contratos"
    records = []
    for run_dir in find_all_run_dirs(base_dir):
        for filename in sorted(list_dir(run_dir)):
            payload = json.loads(read_bytes(f"{run_dir}/{filename}"))
            records.extend(payload.get("data", []))
    return pd.DataFrame.from_records(records)


# --------------------------------------------------------------------------- #
# Transformações por entidade
# --------------------------------------------------------------------------- #
def transform_empenhos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _parse_dates(df, ["dataemissao", "datacancelamento", "datacontabilizacao", "created_at", "updated_at"])
    df["codigocredor"] = df["codigocredor"].map(_only_digits)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    df = _dedupe_latest(df, key_columns=["id", "ano"])
    return df


def transform_ordem_bancaria(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _parse_dates(df, ["dataemissao", "datacancelamento", "datacontabilizacao", "created_at", "updated_at"])
    df["codigocredor"] = df["codigocredor"].map(_only_digits)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    df = _dedupe_latest(df, key_columns=["id", "ano"])
    return df


def transform_unidade_gestora(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _parse_dates(df, ["created_at", "updated_at"])
    df["cnpj"] = df["cnpj"].map(_only_digits)
    df = _dedupe_latest(df, key_columns=["codigo", "ano"])
    return df


def transform_contratos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    date_columns = [
        "data_assinatura", "data_inicio", "data_termino", "data_publicacao_portal",
        "data_rescisao", "created_at", "updated_at",
    ]
    df = _parse_dates(df, date_columns)
    df["cpf_cnpj_financiador"] = df["cpf_cnpj_financiador"].map(_only_digits)
    for col in ["valor_contrato", "calculated_valor_pago", "calculated_valor_empenhado"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = _dedupe_latest(df, key_columns=["id"])
    return df


def enrich_empenhos_com_ug(empenhos: pd.DataFrame, unidade_gestora: pd.DataFrame) -> pd.DataFrame:
    ug = unidade_gestora[["codigo", "ano", "titulo", "cnpj"]].rename(
        columns={"titulo": "nome_ug", "cnpj": "cnpj_ug"}
    )
    # empenhos.codigo (código do documento) e ug.codigo (chave de junção) colidem no merge;
    # com suffixes=("", "_ug") o lado esquerdo (empenhos) fica sem sufixo e o da direita
    # (ug, join key duplicada) vira "codigo_ug" — é sempre essa cópia redundante que deve
    # ser descartada, nunca o "codigo" original do empenho.
    return empenhos.merge(
        ug, left_on=["codigoug", "ano"], right_on=["codigo", "ano"], how="left", suffixes=("", "_ug")
    ).drop(columns=["codigo_ug"], errors="ignore")


# --------------------------------------------------------------------------- #
# Persistência particionada por ano/mês
# --------------------------------------------------------------------------- #
def write_silver_partitioned(df: pd.DataFrame, entity: str, date_column: str) -> list[str]:
    written = []
    df = df.dropna(subset=[date_column])
    df["_ano"] = df[date_column].dt.year
    df["_mes"] = df[date_column].dt.month
    for (ano, mes), group in df.groupby(["_ano", "_mes"]):
        path = f"{HDFS_SILVER_PATH}/{entity}/ano={int(ano)}/mes={int(mes):02d}/data.parquet"
        write_dataframe_parquet(path, group.drop(columns=["_ano", "_mes"]))
        written.append(path)
    return written


def run_silver_transform() -> dict:
    empenhos_raw = load_postgres_table_history("empenhos")
    obo_raw = load_postgres_table_history("ordem_bancaria_orcamentaria")
    ug_raw = load_postgres_table_history("unidade_gestora")
    contratos_raw = load_contratos_history()

    empenhos = transform_empenhos(empenhos_raw)
    obo = transform_ordem_bancaria(obo_raw)
    ug = transform_unidade_gestora(ug_raw)
    contratos = transform_contratos(contratos_raw)

    empenhos_enriched = enrich_empenhos_com_ug(empenhos, ug)

    results = {
        "empenhos": write_silver_partitioned(empenhos_enriched, "empenhos", "dataemissao"),
        "ordem_bancaria_orcamentaria": write_silver_partitioned(obo, "ordem_bancaria_orcamentaria", "dataemissao"),
        "contratos": write_silver_partitioned(contratos, "contratos", "data_assinatura"),
    }
    # unidade_gestora é uma dimensão de referência (baixo volume): grava snapshot único.
    write_dataframe_parquet(f"{HDFS_SILVER_PATH}/unidade_gestora/data.parquet", ug)
    results["unidade_gestora"] = [f"{HDFS_SILVER_PATH}/unidade_gestora/data.parquet"]

    logger.info("Silver gravado: %s", {k: len(v) for k, v in results.items()})
    return {k: {"partitions": len(v), "rows": None} for k, v in results.items()}


if __name__ == "__main__":
    import json as _json

    logging.basicConfig(level=logging.INFO)
    print(_json.dumps(run_silver_transform(), indent=2))
