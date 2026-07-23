"""Configuração central do pipeline, carregada de variáveis de ambiente (.env)."""
import os

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


# --- Banco de origem (PostgreSQL fornecido pelo curso, remoto) ---
SRC_PG_HOST = _env("SRC_PG_HOST")
SRC_PG_PORT = int(_env("SRC_PG_PORT", "5432"))
SRC_PG_DB = _env("SRC_PG_DB")
SRC_PG_USER = _env("SRC_PG_USER")
SRC_PG_PASSWORD = _env("SRC_PG_PASSWORD")

# --- Data Warehouse (camada Gold) ---
DW_PG_HOST = _env("DW_PG_HOST", "postgres-dw")
DW_PG_PORT = int(_env("DW_PG_PORT", "5432"))
DW_PG_DB = _env("DW_PG_DB", "dw_ceara")
DW_PG_USER = _env("DW_PG_USER", "dw_user")
DW_PG_PASSWORD = _env("DW_PG_PASSWORD", "dw_pass")

# --- API REST Ceará Transparente ---
API_BASE_URL = _env("API_BASE_URL", "https://api-dados-abertos.cearatransparente.ce.gov.br")
API_CONTRATOS_ENDPOINT = _env("API_CONTRATOS_ENDPOINT", "/transparencia/contratos/contratos")

# --- HDFS ---
HDFS_WEBHDFS_URL = _env("HDFS_WEBHDFS_URL", "http://namenode:9870")
HDFS_USER = _env("HDFS_USER", "root")
HDFS_BRONZE_PATH = _env("HDFS_BRONZE_PATH", "/data/bronze")
HDFS_SILVER_PATH = _env("HDFS_SILVER_PATH", "/data/silver")

# --- Janela de extração (backfill inicial) ---
EXTRACT_START_DATE = _env("EXTRACT_START_DATE", "2026-01-01")
EXTRACT_END_DATE = _env("EXTRACT_END_DATE", "2026-06-30")

# --- IA generativa (API OpenAI) ---
# Chave ausente/vazia é permitida: o gerador de relatório detecta isso e cai para o
# template determinístico em vez de falhar (ver models/report_generator.py).
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = _env("OPENAI_MODEL", "gpt-4o-mini")


def src_pg_dsn() -> str:
    return (
        f"host={SRC_PG_HOST} port={SRC_PG_PORT} dbname={SRC_PG_DB} "
        f"user={SRC_PG_USER} password={SRC_PG_PASSWORD} client_encoding=utf8"
    )


def dw_pg_dsn() -> str:
    return (
        f"host={DW_PG_HOST} port={DW_PG_PORT} dbname={DW_PG_DB} "
        f"user={DW_PG_USER} password={DW_PG_PASSWORD} client_encoding=utf8"
    )
