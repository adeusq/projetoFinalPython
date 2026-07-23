"""Extração paginada da API REST do portal Ceará Transparente (contratos públicos).

A API espera as datas no formato DD/MM/YYYY e retorna, em `sumary.total_pages`, o total
de páginas a percorrer. Cada página é persistida como um arquivo JSON individual na
camada Bronze do HDFS — sem transformação — preservando a resposta original da fonte.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

import requests

from src.utils.config import API_BASE_URL, API_CONTRATOS_ENDPOINT, HDFS_BRONZE_PATH
from src.utils.hdfs_client import write_text

logger = logging.getLogger(__name__)

RATE_LIMIT_SLEEP_SECONDS = 0.3
REQUEST_TIMEOUT_SECONDS = 30


def _to_api_date(iso_date: str) -> str:
    """Converte 'YYYY-MM-DD' para o formato DD/MM/YYYY exigido pela API."""
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")


def fetch_page(start_date: str, end_date: str, page: int) -> dict:
    url = f"{API_BASE_URL}{API_CONTRATOS_ENDPOINT}"
    params = {
        "data_assinatura_inicio": _to_api_date(start_date),
        "data_assinatura_fim": _to_api_date(end_date),
        "page": page,
    }
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def run_extraction(start_date: str, end_date: str, run_id: str, max_pages: int | None = None) -> dict:
    """Pagina todos os contratos assinados no intervalo informado e grava cada página no HDFS.

    `max_pages` permite limitar o número de páginas (útil em testes); em produção deixe None
    para percorrer `summary.total_pages` por completo.
    """
    first_page = fetch_page(start_date, end_date, page=1)
    total_pages = first_page["sumary"]["total_pages"]
    total_records = first_page["sumary"]["total_records"]
    logger.info("API contratos: %d páginas / %d registros no intervalo %s a %s",
                total_pages, total_records, start_date, end_date)

    pages_to_fetch = min(total_pages, max_pages) if max_pages else total_pages
    hdfs_prefix = f"{HDFS_BRONZE_PATH}/api/contratos/run_id={run_id}"

    rows_written = 0
    for page in range(1, pages_to_fetch + 1):
        payload = first_page if page == 1 else fetch_page(start_date, end_date, page=page)
        hdfs_path = f"{hdfs_prefix}/page_{page:05d}.json"
        write_text(hdfs_path, __import__("json").dumps(payload, ensure_ascii=False))
        rows_written += len(payload.get("data", []))
        if page < pages_to_fetch:
            time.sleep(RATE_LIMIT_SLEEP_SECONDS)

    return {
        "hdfs_path": hdfs_prefix,
        "total_pages_available": total_pages,
        "pages_fetched": pages_to_fetch,
        "rows": rows_written,
    }


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO)
    start, end = sys.argv[1], sys.argv[2]
    print(json.dumps(run_extraction(start, end, run_id="manual"), indent=2))
