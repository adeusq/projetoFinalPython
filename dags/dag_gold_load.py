"""DAG 3 — Carga Gold (Data Warehouse).

Disparada quando a DAG Silver publica seu Dataset. Constrói as dimensões (com SCD2 em
dim_credor) e as tabelas fato a partir da camada Silver e realiza o upsert no PostgreSQL
do Data Warehouse.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from src.utils.datasets import GOLD_DATASET, SILVER_DATASET

DEFAULT_ARGS = {
    "owner": "trabalho-final",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="dag_gold_load",
    description="Carga do modelo dimensional (dimensões + fatos) no Data Warehouse PostgreSQL",
    schedule=[SILVER_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gold", "dw"],
)
def dag_gold_load():
    @task(outlets=[GOLD_DATASET])
    def load_gold() -> dict:
        from src.loaders.dw_loader import run_gold_load

        return run_gold_load()

    load_gold()


dag_gold_load()
