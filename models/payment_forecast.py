"""Modelo 2 — Previsão de pagamentos trimestrais por órgão (XGBoost, regressão quantílica).

Lê o histórico de `dw.fato_ordem_bancaria`, agrega por órgão/trimestre e treina três
regressores XGBoost (quantis 0.1 / 0.5 / 0.9) para produzir uma previsão pontual com
intervalo de confiança para o próximo trimestre de cada órgão.

Observação importante: a qualidade da previsão depende diretamente da profundidade do
histórico carregado no DW. Com uma janela de extração curta (poucos trimestres), o
modelo tem pouquíssimo sinal para aprender sazonalidade — nesse caso o pipeline recorre
a uma previsão ingênua (persistência do último valor) com intervalo mais largo. Para um
histórico robusto, aumente EXTRACT_START_DATE (ver .env) e reprocesse Bronze/Silver/Gold.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

from src.loaders.dw_loader import connect_dw

logger = logging.getLogger(__name__)

ARTIFACT_DIR = Path(__file__).parent / "artifacts"
MIN_QUARTERS_FOR_ML = 5  # mínimo de trimestres históricos por órgão para treinar o XGBoost

FEATURE_COLUMNS = [
    "trimestre", "ano_eleitoral", "valor_lag1", "valor_mesmo_trimestre_ano_anterior",
    "valor_contratado_acumulado", "sequencial_trimestre",
]


def load_quarterly_history(conn) -> pd.DataFrame:
    query = """
        SELECT
            fo.sk_orgao, dorg.codigo_orgao, dorg.nome AS nome_orgao,
            dt.ano, dt.trimestre, SUM(fo.valor) AS valor_total
        FROM dw.fato_ordem_bancaria fo
        JOIN dw.dim_tempo dt ON dt.sk_tempo = fo.sk_tempo
        JOIN dw.dim_orgao dorg ON dorg.sk_orgao = fo.sk_orgao
        GROUP BY fo.sk_orgao, dorg.codigo_orgao, dorg.nome, dt.ano, dt.trimestre
        ORDER BY fo.sk_orgao, dt.ano, dt.trimestre
    """
    return pd.read_sql(query, conn)


def load_valor_contratado_acumulado(conn) -> pd.DataFrame:
    """Valor contratado acumulado por órgão até o fim de cada trimestre (proxy de
    'valor contratado ativo' — não modelamos vigência exata contrato-a-contrato aqui)."""
    query = """
        SELECT fc.sk_orgao, fc.ano, EXTRACT(QUARTER FROM dt.data)::int AS trimestre,
               SUM(fc.valor_contratado) AS valor_contratado_trimestre
        FROM dw.fato_contrato fc
        JOIN dw.dim_tempo dt ON dt.sk_tempo = fc.sk_tempo_assinatura
        GROUP BY fc.sk_orgao, fc.ano, EXTRACT(QUARTER FROM dt.data)
    """
    df = pd.read_sql(query, conn)
    df = df.sort_values(["sk_orgao", "ano", "trimestre"])
    df["valor_contratado_acumulado"] = df.groupby("sk_orgao")["valor_contratado_trimestre"].cumsum()
    return df[["sk_orgao", "ano", "trimestre", "valor_contratado_acumulado"]]


def build_features(history: pd.DataFrame, contratado_acumulado: pd.DataFrame) -> pd.DataFrame:
    df = history.merge(contratado_acumulado, on=["sk_orgao", "ano", "trimestre"], how="left")
    df["valor_contratado_acumulado"] = df["valor_contratado_acumulado"].fillna(0.0)
    df = df.sort_values(["sk_orgao", "ano", "trimestre"]).reset_index(drop=True)

    df["sequencial_trimestre"] = df["ano"] * 4 + df["trimestre"]
    df["ano_eleitoral"] = (df["ano"] % 2 == 0).astype(int)
    df["valor_lag1"] = df.groupby("sk_orgao")["valor_total"].shift(1)
    df["valor_mesmo_trimestre_ano_anterior"] = df.groupby(["sk_orgao", "trimestre"])["valor_total"].shift(1)
    return df


def _naive_forecast(org_history: pd.DataFrame) -> tuple[float, float, float]:
    ultimo_valor = float(org_history["valor_total"].iloc[-1])
    return ultimo_valor, ultimo_valor * 0.7, ultimo_valor * 1.3


def _train_quantile_model(X: pd.DataFrame, y: pd.Series, alpha: float) -> xgb.XGBRegressor:
    model = xgb.XGBRegressor(
        objective="reg:quantileerror",
        quantile_alpha=alpha,
        n_estimators=150,
        max_depth=3,
        learning_rate=0.1,
        random_state=42,
    )
    model.fit(X, y)
    return model


def run_payment_forecast() -> dict:
    conn = connect_dw()
    try:
        history = load_quarterly_history(conn)
        if history.empty:
            logger.warning("fato_ordem_bancaria vazia — nada para prever.")
            return {"forecasts": 0}

        contratado_acumulado = load_valor_contratado_acumulado(conn)
        df = build_features(history, contratado_acumulado)

        quarters_per_org = df.groupby("sk_orgao").size()
        orgaos_com_historico_suficiente = quarters_per_org[quarters_per_org >= MIN_QUARTERS_FOR_ML].index

        train_df = df[df["sk_orgao"].isin(orgaos_com_historico_suficiente)].dropna(subset=FEATURE_COLUMNS)

        modelos = {}
        if len(train_df) >= 10:
            X_train = train_df[FEATURE_COLUMNS]
            y_train = train_df["valor_total"]
            modelos = {
                alpha: _train_quantile_model(X_train, y_train, alpha)
                for alpha in (0.1, 0.5, 0.9)
            }
            ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
            joblib.dump(modelos, ARTIFACT_DIR / "payment_forecast_xgb.joblib")
            logger.info("Modelo XGBoost treinado com %d observações de %d órgãos.",
                        len(train_df), len(orgaos_com_historico_suficiente))
        else:
            logger.warning(
                "Histórico insuficiente para treinar XGBoost (%d linhas de treino) — "
                "usando previsão ingênua (persistência) para todos os órgãos.", len(train_df)
            )

        proximo_ano, proximo_trimestre = _proximo_trimestre()
        resultados = []
        for sk_orgao, org_df in df.groupby("sk_orgao"):
            org_df = org_df.sort_values(["ano", "trimestre"])
            codigo_orgao = org_df["codigo_orgao"].iloc[-1]

            if modelos and sk_orgao in orgaos_com_historico_suficiente.values:
                ultima_linha = org_df.iloc[[-1]].copy()
                features_prox = pd.DataFrame({
                    "trimestre": [proximo_trimestre],
                    "ano_eleitoral": [int(proximo_ano % 2 == 0)],
                    "valor_lag1": [ultima_linha["valor_total"].iat[0]],
                    "valor_mesmo_trimestre_ano_anterior": [
                        org_df[(org_df["ano"] == proximo_ano - 1) & (org_df["trimestre"] == proximo_trimestre)]
                        ["valor_total"].pipe(lambda s: s.iat[0] if len(s) else ultima_linha["valor_total"].iat[0])
                    ],
                    "valor_contratado_acumulado": [ultima_linha["valor_contratado_acumulado"].iat[0]],
                    "sequencial_trimestre": [proximo_ano * 4 + proximo_trimestre],
                })
                p10 = float(modelos[0.1].predict(features_prox)[0])
                p50 = float(modelos[0.5].predict(features_prox)[0])
                p90 = float(modelos[0.9].predict(features_prox)[0])
                modelo_usado = "xgboost_quantile"
            else:
                p50, p10, p90 = _naive_forecast(org_df)
                modelo_usado = "naive_persistence"

            resultados.append({
                "sk_orgao": int(sk_orgao),
                "codigo_orgao": codigo_orgao,
                "trimestre_referencia": f"{proximo_ano}-Q{proximo_trimestre}",
                "valor_previsto": max(p50, 0.0),
                "intervalo_inferior": max(min(p10, p50), 0.0),
                "intervalo_superior": max(p90, p50),
                "modelo": modelo_usado,
            })

        n = write_forecasts(conn, resultados)
        logger.info("Previsões gravadas para %d órgãos (trimestre %s-Q%s).", n, proximo_ano, proximo_trimestre)
        return {"forecasts": n, "trimestre_referencia": f"{proximo_ano}-Q{proximo_trimestre}"}
    finally:
        conn.close()


def _proximo_trimestre(hoje: date | None = None) -> tuple[int, int]:
    hoje = hoje or date.today()
    trimestre_atual = (hoje.month - 1) // 3 + 1
    if trimestre_atual == 4:
        return hoje.year + 1, 1
    return hoje.year, trimestre_atual + 1


def write_forecasts(conn, resultados: list[dict]) -> int:
    with conn.cursor() as cur:
        for r in resultados:
            cur.execute(
                """
                INSERT INTO dw.previsao_pagamento_trimestral
                    (sk_orgao, trimestre_referencia, valor_previsto, intervalo_inferior,
                     intervalo_superior, modelo)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    r["sk_orgao"], r["trimestre_referencia"], r["valor_previsto"],
                    r["intervalo_inferior"], r["intervalo_superior"], r["modelo"],
                ),
            )
    conn.commit()
    return len(resultados)


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO)
    print(json.dumps(run_payment_forecast(), indent=2, default=str))
