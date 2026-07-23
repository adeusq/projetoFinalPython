"""DAG 4 — Inferência de Machine Learning e IA Generativa.

Disparada quando a DAG Gold publica seu Dataset. Lê o Data Warehouse, treina/aplica os
modelos de detecção de anomalias e previsão de pagamentos, grava os resultados de volta
no DW e gera o relatório narrativo final via LLM (API OpenAI).
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from src.utils.datasets import GOLD_DATASET

DEFAULT_ARGS = {
    "owner": "trabalho-final",
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id="dag_ml_inference",
    description="Treinamento/inferência dos modelos de ML e geração do relatório de insights",
    schedule=[GOLD_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ml", "ia"],
)
def dag_ml_inference():
    @task
    def anomaly_detection() -> dict:
        from models.anomaly_detection import run_anomaly_detection

        return run_anomaly_detection()

    @task
    def payment_forecast() -> dict:
        from models.payment_forecast import run_payment_forecast

        return run_payment_forecast()

    @task
    def generate_report(anomaly_result: dict, forecast_result: dict) -> dict:
        from models.report_generator import run_report_generation

        return run_report_generation()

    generate_report(anomaly_detection(), payment_forecast())


dag_ml_inference()
