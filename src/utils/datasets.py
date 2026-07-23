"""Datasets do Airflow compartilhados entre as DAGs, usados para encadear a execução via
data-aware scheduling (DAG N dispara DAG N+1 ao concluir com sucesso)."""
from airflow.datasets import Dataset

BRONZE_DATASET = Dataset("hdfs://bronze/ceara-transparente")
SILVER_DATASET = Dataset("hdfs://silver/ceara-transparente")
GOLD_DATASET = Dataset("postgres://postgres-dw/dw_ceara/dw")
