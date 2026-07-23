"""DAG 1 — Extração e Carga Bronze.

Extrai dados incrementalmente do PostgreSQL de origem e da API REST do Ceará
Transparente, valida a completude mínima e publica o Dataset consumido pela DAG de
transformação Silver.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from src.utils.datasets import BRONZE_DATASET

DEFAULT_ARGS = {
    "owner": "trabalho-final",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="dag_bronze_extract",
    description="Extração incremental do Postgres de origem e da API de contratos para a camada Bronze",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "extracao"],
)
def dag_bronze_extract():
    @task
    def extract_postgres(**context) -> dict:
        from src.extractors.postgres_extractor import run_extraction

        start_date = Variable.get("last_extracted_date", default_var=None) or _default_start()
        end_date = context["ds"]
        run_id = context["ds_nodash"]
        return run_extraction(start_date, end_date, run_id=run_id)

    @task
    def extract_api(**context) -> dict:
        from src.extractors.api_extractor import run_extraction

        start_date = Variable.get("last_extracted_date", default_var=None) or _default_start()
        end_date = context["ds"]
        run_id = context["ds_nodash"]
        return run_extraction(start_date, end_date, run_id=run_id)

    @task(outlets=[BRONZE_DATASET])
    def validate_bronze(postgres_meta: dict, api_meta: dict, **context) -> None:
        from src.utils.validation import validate_api_contratos, validate_postgres_table

        for table, meta in postgres_meta.items():
            validate_postgres_table(meta["hdfs_path"], table)
        validate_api_contratos(api_meta["hdfs_path"], expected_pages=api_meta["pages_fetched"])

        # Extração incremental: só avança o marcador após validar com sucesso.
        Variable.set("last_extracted_date", context["ds"])

    validate_bronze(extract_postgres(), extract_api())


def _default_start() -> str:
    from src.utils.config import EXTRACT_START_DATE

    return EXTRACT_START_DATE


dag_bronze_extract()
