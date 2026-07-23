"""DAG 2 — Transformação Silver.

Disparada automaticamente quando a DAG Bronze publica o Dataset correspondente
(data-aware scheduling do Airflow 2.x). Aplica tipagem, deduplicação, normalização de
CPF/CNPJ e enriquecimento, persistindo Parquet particionado por ano/mês no HDFS.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from src.utils.datasets import BRONZE_DATASET, SILVER_DATASET

DEFAULT_ARGS = {
    "owner": "trabalho-final",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="dag_silver_transform",
    description="Transformação Bronze -> Silver (qualidade, deduplicação e enriquecimento)",
    schedule=[BRONZE_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["silver", "transformacao"],
)
def dag_silver_transform():
    @task(outlets=[SILVER_DATASET])
    def transform_silver() -> dict:
        from src.transformers.bronze_to_silver import run_silver_transform

        return run_silver_transform()

    transform_silver()


dag_silver_transform()
