"""Wrapper fino sobre o cliente WebHDFS usado pelas camadas Bronze e Silver."""
from __future__ import annotations

import io

from hdfs import InsecureClient

from src.utils.config import HDFS_USER, HDFS_WEBHDFS_URL


def get_client() -> InsecureClient:
    return InsecureClient(HDFS_WEBHDFS_URL, user=HDFS_USER)


def write_bytes(hdfs_path: str, data: bytes, overwrite: bool = True) -> str:
    client = get_client()
    with client.write(hdfs_path, overwrite=overwrite) as writer:
        writer.write(data)
    return hdfs_path


def write_text(hdfs_path: str, text: str, overwrite: bool = True, encoding: str = "utf-8") -> str:
    return write_bytes(hdfs_path, text.encode(encoding), overwrite=overwrite)


def read_bytes(hdfs_path: str) -> bytes:
    client = get_client()
    with client.read(hdfs_path) as reader:
        return reader.read()


def list_dir(hdfs_path: str) -> list[str]:
    client = get_client()
    if not client.status(hdfs_path, strict=False):
        return []
    return client.list(hdfs_path)


def ensure_dir(hdfs_path: str) -> None:
    client = get_client()
    client.makedirs(hdfs_path)


def find_all_files(base_dir: str, filename: str = "data.parquet") -> list[str]:
    """Percorre recursivamente `base_dir` no HDFS e retorna os caminhos completos de todos
    os arquivos com nome `filename` encontrados (usado para ler partições ano=/mes=)."""
    client = get_client()
    if not client.status(base_dir, strict=False):
        return []
    found = []
    for path, _dirs, files in client.walk(base_dir):
        if filename in files:
            found.append(f"{path}/{filename}")
    return found


def find_all_run_dirs(base_dir: str) -> list[str]:
    """Localiza, sob `base_dir`, todos os subdiretórios 'run_id=...' (ordem cronológica).

    Cada execução da extração incremental (DAG Bronze) grava apenas o delta daquela
    janela em seu próprio run_id — a Silver precisa consolidar TODOS os run_id
    históricos, não só o mais recente, para não perder dados de execuções anteriores.
    """
    entries = [e for e in list_dir(base_dir) if e.startswith("run_id=")]
    return [f"{base_dir}/{e}" for e in sorted(entries)]


def write_dataframe_parquet(hdfs_path: str, df) -> str:
    """Serializa um DataFrame pandas como Parquet e grava no HDFS."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    return write_bytes(hdfs_path, buffer.getvalue())


def read_dataframe_parquet(hdfs_path: str):
    import pandas as pd

    data = read_bytes(hdfs_path)
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


def read_all_partitions(base_dir: str, filename: str = "data.parquet"):
    """Lê e concatena todos os arquivos `filename` sob `base_dir` (ex.: partições ano=/mes=)."""
    import pandas as pd

    paths = find_all_files(base_dir, filename=filename)
    if not paths:
        return pd.DataFrame()
    return pd.concat([read_dataframe_parquet(p) for p in paths], ignore_index=True)
