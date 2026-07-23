"""Extração incremental do banco PostgreSQL de origem (banco fornecido pelo curso).

Lê as tabelas `empenhos`, `ordem_bancaria_orcamentaria` e `unidade_gestora`, filtradas
por data quando aplicável, e persiste o resultado bruto (sem transformação) na camada
Bronze do HDFS, em formato Parquet.
"""
from __future__ import annotations

import logging

import pandas as pd
import psycopg2

from src.utils.config import src_pg_dsn, HDFS_BRONZE_PATH
from src.utils.hdfs_client import write_dataframe_parquet

logger = logging.getLogger(__name__)

# Tabelas extraídas de forma incremental por uma coluna de data.
INCREMENTAL_TABLES = {
    "empenhos": "dataemissao",
    "ordem_bancaria_orcamentaria": "dataemissao",
}

# Tabelas de referência, extraídas em snapshot completo (baixo volume de linhas).
SNAPSHOT_TABLES = [
    "unidade_gestora",
    "acao",
    "categoria",
    "elemento",
    "fonte_recurso",
    "funcao",
    "grupo",
    "natureza",
]


def connect_source() -> psycopg2.extensions.connection:
    return psycopg2.connect(src_pg_dsn())


def extract_incremental(table: str, date_column: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Extrai linhas de `table` cuja `date_column` esteja no intervalo [start_date, end_date]."""
    query = (
        f"SELECT * FROM {table} "
        f"WHERE {date_column}::timestamp BETWEEN %(start)s AND %(end)s "
        f"ORDER BY {date_column}::timestamp"
    )
    with connect_source() as conn:
        df = pd.read_sql(query, conn, params={"start": start_date, "end": end_date})
    logger.info("Extraídas %d linhas de %s entre %s e %s", len(df), table, start_date, end_date)
    return df


def extract_snapshot(table: str) -> pd.DataFrame:
    with connect_source() as conn:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
    logger.info("Extraídas %d linhas (snapshot completo) de %s", len(df), table)
    return df


def run_extraction(start_date: str, end_date: str, run_id: str) -> dict:
    """Executa a extração completa (incremental + snapshot) e grava tudo na camada Bronze.

    Retorna um dicionário de metadados leve (caminhos HDFS e contagens) — adequado para
    ser passado via XCom, sem transportar DataFrames inteiros.
    """
    results = {}

    for table, date_column in INCREMENTAL_TABLES.items():
        df = extract_incremental(table, date_column, start_date, end_date)
        hdfs_path = f"{HDFS_BRONZE_PATH}/postgres/{table}/run_id={run_id}/data.parquet"
        write_dataframe_parquet(hdfs_path, df)
        results[table] = {"hdfs_path": hdfs_path, "rows": len(df)}

    for table in SNAPSHOT_TABLES:
        df = extract_snapshot(table)
        hdfs_path = f"{HDFS_BRONZE_PATH}/postgres/{table}/run_id={run_id}/data.parquet"
        write_dataframe_parquet(hdfs_path, df)
        results[table] = {"hdfs_path": hdfs_path, "rows": len(df)}

    return results


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO)
    start, end = sys.argv[1], sys.argv[2]
    print(json.dumps(run_extraction(start, end, run_id="manual"), indent=2))
