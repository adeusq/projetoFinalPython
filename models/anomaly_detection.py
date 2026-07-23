"""Modelo 1 — Detecção de anomalias contratuais (Isolation Forest, não supervisionado).

Lê `dw.fato_contrato`, treina um Isolation Forest sobre as features de valor, modalidade,
tipo de objeto, vigência, emergência e histórico de infrações do credor, e grava um score
de anomalia (0 a 1, onde 1 = mais anômalo) de volta na própria fato_contrato.
"""
from __future__ import annotations

import logging
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from src.loaders.dw_loader import connect_dw

logger = logging.getLogger(__name__)

ARTIFACT_PATH = Path(__file__).parent / "artifacts" / "isolation_forest.joblib"

NUMERIC_FEATURES = ["valor_contratado", "dias_vigencia", "historico_credor_infringement"]
CATEGORICAL_FEATURES = ["descricao_modalidade", "tipo_objeto"]
BOOLEAN_FEATURES = ["flag_emergency"]


def load_features(conn) -> pd.DataFrame:
    query = """
        SELECT
            fc.sk_contrato, fc.ano, fc.valor_contratado, fc.dias_vigencia,
            fc.historico_credor_infringement, fc.flag_emergency, fc.tipo_objeto,
            dm.descricao_modalidade
        FROM dw.fato_contrato fc
        LEFT JOIN dw.dim_modalidade dm ON dm.sk_modalidade = fc.sk_modalidade
    """
    df = pd.read_sql(query, conn)
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["flag_emergency"] = df["flag_emergency"].fillna(False).astype(int)
    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].fillna("DESCONHECIDO")
    return df


def build_pipeline(contamination: float = 0.05) -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ],
        remainder="passthrough",
    )
    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    return Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])


def score_anomalies(pipeline: Pipeline, df: pd.DataFrame) -> pd.Series:
    """Converte o score bruto do Isolation Forest (quanto menor, mais anômalo) em um
    score normalizado 0-1, onde 1 representa o contrato mais anômalo do lote."""
    raw = -pipeline.named_steps["model"].score_samples(
        pipeline.named_steps["preprocessor"].transform(df)
    )
    raw_min, raw_max = raw.min(), raw.max()
    if raw_max == raw_min:
        return pd.Series(0.0, index=df.index)
    return pd.Series((raw - raw_min) / (raw_max - raw_min), index=df.index)


def write_scores(conn, df: pd.DataFrame) -> int:
    with conn.cursor() as cur:
        for row in df.itertuples(index=False):
            cur.execute(
                "UPDATE dw.fato_contrato SET score_anomalia = %s WHERE sk_contrato = %s AND ano = %s",
                (float(row.score_anomalia), int(row.sk_contrato), int(row.ano)),
            )
    conn.commit()
    return len(df)


def run_anomaly_detection(contamination: float = 0.05) -> dict:
    conn = connect_dw()
    try:
        df = load_features(conn)
        if df.empty:
            logger.warning("fato_contrato vazia — nada para treinar/pontuar.")
            return {"rows_scored": 0}

        features = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES + BOOLEAN_FEATURES]
        pipeline = build_pipeline(contamination=contamination)
        pipeline.fit(features)

        df["score_anomalia"] = score_anomalies(pipeline, features)

        ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(pipeline, ARTIFACT_PATH)

        rows = write_scores(conn, df[["sk_contrato", "ano", "score_anomalia"]])
        top_anomalos = df.nlargest(10, "score_anomalia")[
            ["sk_contrato", "valor_contratado", "descricao_modalidade", "score_anomalia"]
        ]
        logger.info("Anomalias pontuadas: %d contratos. Top 10:\n%s", rows, top_anomalos.to_string())
        return {"rows_scored": rows, "top_anomalos": top_anomalos.to_dict(orient="records")}
    finally:
        conn.close()


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO)
    print(json.dumps(run_anomaly_detection(), indent=2, default=str))
