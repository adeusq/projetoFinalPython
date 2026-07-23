"""Validações mínimas de qualidade sobre os artefatos gravados na camada Bronze."""
from __future__ import annotations

import json
import logging

from src.utils.hdfs_client import list_dir, read_dataframe_parquet, read_bytes

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "empenhos": {"id", "codigoug", "codigocredor", "valor", "dataemissao", "ano"},
    "ordem_bancaria_orcamentaria": {"id", "codigoug", "codigocredor", "valor", "dataemissao", "ano"},
    "unidade_gestora": {"codigo", "titulo", "cnpj", "ano"},
}


class BronzeValidationError(Exception):
    pass


def validate_postgres_table(hdfs_path: str, table: str, expected_min_rows: int = 0) -> None:
    df = read_dataframe_parquet(hdfs_path)
    if len(df) < expected_min_rows:
        raise BronzeValidationError(
            f"{table}: esperado no mínimo {expected_min_rows} linhas, encontrado {len(df)}"
        )
    required = REQUIRED_COLUMNS.get(table)
    if required and not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise BronzeValidationError(f"{table}: colunas obrigatórias ausentes: {missing}")
    logger.info("Bronze OK: %s (%d linhas, %d colunas)", table, len(df), len(df.columns))


def validate_api_contratos(hdfs_prefix: str, expected_pages: int) -> None:
    files = list_dir(hdfs_prefix)
    if len(files) < expected_pages:
        raise BronzeValidationError(
            f"contratos: esperado {expected_pages} páginas, encontrado {len(files)} em {hdfs_prefix}"
        )
    sample_path = f"{hdfs_prefix}/{sorted(files)[0]}"
    payload = json.loads(read_bytes(sample_path))
    if "data" not in payload or "sumary" not in payload:
        raise BronzeValidationError(f"contratos: estrutura inesperada em {sample_path}")
    logger.info("Bronze OK: contratos (%d páginas)", len(files))
