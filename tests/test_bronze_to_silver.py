"""Testes unitários das transformações Bronze -> Silver (puramente em memória, sem HDFS/DB)."""
from __future__ import annotations

import pandas as pd

from src.transformers.bronze_to_silver import (
    _dedupe_latest,
    _only_digits,
    _parse_dates,
    enrich_empenhos_com_ug,
    transform_empenhos,
)


def test_only_digits_strips_non_numeric_characters():
    assert _only_digits("123.456.789-00") == "12345678900"
    assert _only_digits("12.345.678/0001-90") == "12345678000190"
    assert _only_digits(None) is None
    assert _only_digits(float("nan")) is None


def test_dedupe_latest_keeps_most_recently_updated_row():
    df = pd.DataFrame({
        "id": [1, 1, 2],
        "ano": [2026, 2026, 2026],
        "updated_at": pd.to_datetime(["2026-01-01", "2026-06-01", "2026-01-01"]),
        "valor": [100, 999, 50],
    })
    result = _dedupe_latest(df, key_columns=["id", "ano"])
    assert len(result) == 2
    assert result.set_index("id").loc[1, "valor"] == 999


def test_transform_empenhos_normalizes_credor_and_dedupes(monkeypatch=None):
    df = pd.DataFrame({
        "id": [1, 1],
        "ano": [2026, 2026],
        "codigocredor": ["123.456.789-00", "123.456.789-00"],
        "valor": ["1000.50", "1000.50"],
        "dataemissao": ["2026-01-10 00:00:00.000", "2026-01-10 00:00:00.000"],
        "datacancelamento": [None, None],
        "datacontabilizacao": ["2026-01-10 00:00:00.000", "2026-01-10 00:00:00.000"],
        "created_at": ["2026-01-10 00:00:00.000", "2026-01-10 00:00:00.000"],
        "updated_at": ["2026-01-10 00:00:00.000", "2026-01-11 00:00:00.000"],
    })
    result = transform_empenhos(df)
    assert len(result) == 1
    assert result.iloc[0]["codigocredor"] == "12345678900"
    assert result.iloc[0]["valor"] == 1000.50


def test_parse_dates_strips_timezone_so_concat_with_naive_dates_stays_datetime64():
    # A API de contratos retorna datas com offset explícito (ex.: '-03:00'), enquanto o
    # Postgres de origem retorna datas sem timezone. Sem normalizar para naive, o
    # pd.concat entre as duas séries degrada para dtype 'object' e quebra o .dt accessor
    # em build_dim_tempo (bug real encontrado ao rodar a DAG dag_gold_load).
    df_com_tz = pd.DataFrame({"data_assinatura": ["2026-01-10T00:00:00.000-03:00"]})
    df_sem_tz = pd.DataFrame({"dataemissao": ["2026-01-10 00:00:00.000"]})

    resultado_tz = _parse_dates(df_com_tz, ["data_assinatura"])["data_assinatura"]
    resultado_naive = _parse_dates(df_sem_tz, ["dataemissao"])["dataemissao"]

    assert resultado_tz.dt.tz is None
    combinado = pd.concat([resultado_tz, resultado_naive])
    assert pd.api.types.is_datetime64_any_dtype(combinado)


def test_enrich_empenhos_com_ug_joins_on_codigo_and_ano():
    empenhos = pd.DataFrame({"codigoug": ["240401"], "ano": [2026], "valor": [100.0]})
    unidade_gestora = pd.DataFrame({
        "codigo": ["240401"], "ano": [2026], "titulo": ["FUNDO ESTADUAL DE SAUDE"], "cnpj": ["12345678000190"],
    })
    result = enrich_empenhos_com_ug(empenhos, unidade_gestora)
    assert result.iloc[0]["nome_ug"] == "FUNDO ESTADUAL DE SAUDE"
    assert result.iloc[0]["cnpj_ug"] == "12345678000190"


def test_enrich_empenhos_com_ug_preserves_original_empenho_codigo_column():
    # empenhos.codigo (código do documento, ex.: '2022NE007525') e unidade_gestora.codigo
    # (chave de junção) colidem no merge — o original deve sobreviver intacto, e não a
    # cópia redundante da chave de junção (bug real: a lógica de drop estava invertida).
    empenhos = pd.DataFrame({
        "codigo": ["2022NE007525"], "codigoug": ["240401"], "ano": [2026], "valor": [100.0],
    })
    unidade_gestora = pd.DataFrame({
        "codigo": ["240401"], "ano": [2026], "titulo": ["FUNDO ESTADUAL DE SAUDE"], "cnpj": ["12345678000190"],
    })
    result = enrich_empenhos_com_ug(empenhos, unidade_gestora)
    assert "codigo" in result.columns
    assert result.iloc[0]["codigo"] == "2022NE007525"
    assert "codigo_ug" not in result.columns
