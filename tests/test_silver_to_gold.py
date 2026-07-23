"""Testes unitários da montagem do modelo dimensional (Silver -> Gold), sem tocar o banco."""
from __future__ import annotations

import pandas as pd

from src.transformers.silver_to_gold import (
    build_dim_credor,
    build_dim_orgao,
    build_dim_tempo,
    build_ug_to_orgao_map,
)


def test_build_dim_tempo_generates_expected_calendar_attributes():
    dates = pd.Series(pd.to_datetime(["2026-01-05", "2026-01-05", "2026-03-20"]))
    dim = build_dim_tempo(dates)

    assert len(dim) == 2  # datas duplicadas removidas
    row = dim[dim["data"] == pd.Timestamp("2026-01-05")].iloc[0]
    assert row["ano"] == 2026
    assert row["trimestre"] == 1
    assert row["mes"] == 1
    assert row["ano_mes"] == "2026-01"


def test_build_ug_to_orgao_map_renames_join_column_to_avoid_collision():
    ug = pd.DataFrame({"codigo": ["240401"], "ano": [2026], "codigoorgao": ["24000000"]})
    mapa = build_ug_to_orgao_map(ug)
    assert list(mapa.columns) == ["codigo_ug", "codigo_orgao"]


def test_build_ug_to_orgao_map_keeps_one_row_per_codigo_using_latest_ano():
    # Algumas unidades gestoras trocam de órgão superior entre anos (bug real: sem
    # desambiguar por ano, o merge many-to-one virava many-to-many e duplicava linhas
    # de fato_empenho, causando CardinalityViolation no upsert em lote).
    ug = pd.DataFrame({
        "codigo": ["100606", "100606"],
        "ano": [2022, 2026],
        "codigoorgao": ["10000000", "10200006"],
    })
    mapa = build_ug_to_orgao_map(ug)
    assert len(mapa) == 1
    assert mapa.iloc[0]["codigo_orgao"] == "10200006"


def test_build_dim_orgao_prefers_sede_record_when_present():
    ug = pd.DataFrame({
        "codigo": ["010001", "010101"],
        "codigoorgao": ["01000000", "01000000"],
        "titulo": ["ASSEMBLEIA LEGISLATIVA", "FUNDO DE PREVIDENCIA"],
        "cnpj": ["06750525000120", "05483909000161"],
        "tipoadministracao": ["DIRETA", "DIRETA"],
        "nomepoder": ["LEGISLATIVO", "LEGISLATIVO"],
    })
    # Nenhuma linha tem codigo == codigoorgao aqui, então o fallback (moda) deve ser usado
    # sem lançar exceção.
    dim = build_dim_orgao(ug)
    assert len(dim) == 1
    assert dim.iloc[0]["codigo_orgao"] == "01000000"


def test_build_dim_credor_unions_contratos_and_empenhos_by_cpf_cnpj():
    contratos = pd.DataFrame({
        "cpf_cnpj_financiador": ["12345678000190"],
        "descricao_nome_credor": ["EMPRESA X LTDA"],
        "infringement_status": [1],
    })
    empenhos = pd.DataFrame({
        "codigocredor": ["12345678000190", "98765432100"],
        "nomecredor": ["EMPRESA X LTDA", "FULANO DE TAL"],
    })
    dim = build_dim_credor(contratos, empenhos)

    assert len(dim) == 2
    pj = dim[dim["cpf_cnpj"] == "12345678000190"].iloc[0]
    assert pj["tipo_pessoa"] == "PJ"
    assert pj["historico_infringement"] == 1

    pf = dim[dim["cpf_cnpj"] == "98765432100"].iloc[0]
    assert pf["tipo_pessoa"] == "PF"
    assert pf["historico_infringement"] == 0
