"""Carga da camada Gold (Data Warehouse dimensional) no PostgreSQL.

Realiza o upsert das dimensões (com SCD2 em dim_credor) e das tabelas fato, resolvendo
surrogate keys a partir das chaves de negócio produzidas por `src/transformers/silver_to_gold.py`.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import psycopg2
import psycopg2.extras

from src.utils.config import dw_pg_dsn

logger = logging.getLogger(__name__)


def connect_dw() -> psycopg2.extensions.connection:
    return psycopg2.connect(dw_pg_dsn())


# --------------------------------------------------------------------------- #
# DIM_TEMPO — insere as datas ausentes e retorna o mapa data -> sk_tempo
# --------------------------------------------------------------------------- #
def upsert_dim_tempo(conn, dim_tempo: pd.DataFrame) -> pd.DataFrame:
    rows = list(dim_tempo.itertuples(index=False))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.dim_tempo
                (data, ano, trimestre, mes, dia, dia_semana, nome_dia_semana, nome_mes, ano_mes)
            VALUES %s
            ON CONFLICT (data) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    # psycopg2 (conexão DBAPI pura, sem SQLAlchemy) devolve a coluna DATE como
    # datetime.date em dtype object — sem essa conversão, o merge posterior com colunas
    # datetime64 do Parquet falha com "trying to merge on datetime64[ns] and object".
    result = pd.read_sql("SELECT sk_tempo, data FROM dw.dim_tempo", conn)
    result["data"] = pd.to_datetime(result["data"])
    return result


# --------------------------------------------------------------------------- #
# DIM_ORGAO
# --------------------------------------------------------------------------- #
def upsert_dim_orgao(conn, dim_orgao: pd.DataFrame) -> pd.DataFrame:
    rows = list(
        dim_orgao[["codigo_orgao", "nome", "cnpj", "tipo_administracao", "esfera"]]
        .itertuples(index=False)
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.dim_orgao (codigo_orgao, nome, cnpj, tipo_administracao, esfera)
            VALUES %s
            ON CONFLICT (codigo_orgao) DO UPDATE SET
                nome = EXCLUDED.nome,
                cnpj = EXCLUDED.cnpj,
                tipo_administracao = EXCLUDED.tipo_administracao,
                esfera = EXCLUDED.esfera
            """,
            rows,
        )
    conn.commit()
    return pd.read_sql("SELECT sk_orgao, codigo_orgao FROM dw.dim_orgao", conn)


# --------------------------------------------------------------------------- #
# DIM_MODALIDADE
# --------------------------------------------------------------------------- #
def upsert_dim_modalidade(conn, dim_modalidade: pd.DataFrame) -> pd.DataFrame:
    rows = list(dim_modalidade[["descricao_modalidade", "descricao_tipo"]].itertuples(index=False))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.dim_modalidade (descricao_modalidade, descricao_tipo)
            VALUES %s
            ON CONFLICT (descricao_modalidade, descricao_tipo) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    return pd.read_sql(
        "SELECT sk_modalidade, descricao_modalidade, descricao_tipo FROM dw.dim_modalidade", conn
    )


# --------------------------------------------------------------------------- #
# DIM_CREDOR (SCD2) — versiona a razão social quando ela muda para o mesmo CPF/CNPJ
# --------------------------------------------------------------------------- #
def upsert_dim_credor_scd2(conn, dim_credor: pd.DataFrame, hoje: date | None = None) -> pd.DataFrame:
    hoje = hoje or date.today()
    current = pd.read_sql(
        "SELECT sk_credor, cpf_cnpj, nome, historico_infringement FROM dw.dim_credor WHERE versao_atual",
        conn,
    )
    merged = dim_credor.merge(current, on="cpf_cnpj", how="left", suffixes=("_novo", "_atual"))

    novos = merged[merged["sk_credor"].isna()]
    mudou_nome = merged[merged["sk_credor"].notna() & (merged["nome_novo"] != merged["nome_atual"])]
    sem_mudanca_nome = merged[merged["sk_credor"].notna() & (merged["nome_novo"] == merged["nome_atual"])]

    with conn.cursor() as cur:
        if len(mudou_nome):
            # Volume de mudanças de razão social por execução é tipicamente baixo;
            # atualização linha a linha é suficiente e mais simples que um UPDATE...FROM(VALUES).
            for cpf in mudou_nome["cpf_cnpj"]:
                cur.execute(
                    "UPDATE dw.dim_credor SET data_fim = %s, versao_atual = FALSE "
                    "WHERE cpf_cnpj = %s AND versao_atual",
                    (hoje, cpf),
                )

        a_inserir = pd.concat([novos, mudou_nome])
        if len(a_inserir):
            rows = [
                (row.cpf_cnpj, row.nome_novo, row.tipo_pessoa, int(row.historico_infringement_novo), hoje)
                for row in a_inserir.itertuples(index=False)
            ]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO dw.dim_credor (cpf_cnpj, nome, tipo_pessoa, historico_infringement, data_inicio) "
                "VALUES %s",
                rows,
            )

        for row in sem_mudanca_nome.itertuples(index=False):
            if int(row.historico_infringement_novo) != int(row.historico_infringement_atual):
                cur.execute(
                    "UPDATE dw.dim_credor SET historico_infringement = %s "
                    "WHERE cpf_cnpj = %s AND versao_atual",
                    (int(row.historico_infringement_novo), row.cpf_cnpj),
                )
    conn.commit()
    return pd.read_sql(
        "SELECT sk_credor, cpf_cnpj FROM dw.dim_credor WHERE versao_atual", conn
    )


# --------------------------------------------------------------------------- #
# FATO_CONTRATO
# --------------------------------------------------------------------------- #
def load_fato_contrato(conn, fato: pd.DataFrame, dim_tempo, dim_orgao, dim_modalidade, dim_credor) -> int:
    df = fato.merge(dim_tempo, left_on="data_assinatura", right_on="data", how="left")
    df = df.merge(dim_orgao, on="codigo_orgao", how="left")
    df = df.merge(dim_modalidade, on=["descricao_modalidade", "descricao_tipo"], how="left")
    df = df.merge(dim_credor, on="cpf_cnpj", how="left")

    rows = [
        (
            r.id_contrato_origem, r.num_contrato,
            (None if pd.isna(r.sk_tempo) else int(r.sk_tempo)),
            (None if pd.isna(r.sk_credor) else int(r.sk_credor)),
            (None if pd.isna(r.sk_orgao) else int(r.sk_orgao)),
            (None if pd.isna(r.sk_modalidade) else int(r.sk_modalidade)),
            float(r.valor_contratado), float(r.valor_empenhado), float(r.valor_pago),
            r.status, r.tipo_objeto, bool(r.flag_emergency),
            (None if pd.isna(r.dias_vigencia) else int(r.dias_vigencia)),
            int(r.historico_credor_infringement), int(r.ano),
        )
        for r in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.fato_contrato (
                id_contrato_origem, num_contrato, sk_tempo_assinatura, sk_credor, sk_orgao,
                sk_modalidade, valor_contratado, valor_empenhado, valor_pago, status,
                tipo_objeto, flag_emergency, dias_vigencia, historico_credor_infringement, ano
            ) VALUES %s
            ON CONFLICT (id_contrato_origem, ano) DO UPDATE SET
                sk_tempo_assinatura = EXCLUDED.sk_tempo_assinatura,
                sk_credor = EXCLUDED.sk_credor,
                sk_orgao = EXCLUDED.sk_orgao,
                sk_modalidade = EXCLUDED.sk_modalidade,
                valor_contratado = EXCLUDED.valor_contratado,
                valor_empenhado = EXCLUDED.valor_empenhado,
                valor_pago = EXCLUDED.valor_pago,
                status = EXCLUDED.status,
                tipo_objeto = EXCLUDED.tipo_objeto,
                flag_emergency = EXCLUDED.flag_emergency,
                dias_vigencia = EXCLUDED.dias_vigencia,
                historico_credor_infringement = EXCLUDED.historico_credor_infringement
            """,
            rows,
        )
    conn.commit()
    return len(rows)


# --------------------------------------------------------------------------- #
# FATO_EMPENHO
# --------------------------------------------------------------------------- #
def load_fato_empenho(conn, fato: pd.DataFrame, dim_tempo, dim_orgao, dim_credor) -> int:
    df = fato.merge(dim_tempo, left_on="data_emissao", right_on="data", how="left")
    df = df.merge(dim_orgao, on="codigo_orgao", how="left")
    df = df.merge(dim_credor, on="cpf_cnpj", how="left")

    rows = [
        (
            r.id_empenho_origem, r.codigo_empenho,
            (None if pd.isna(r.sk_tempo) else int(r.sk_tempo)),
            (None if pd.isna(r.sk_orgao) else int(r.sk_orgao)),
            (None if pd.isna(r.sk_credor) else int(r.sk_credor)),
            float(r.valor), r.modalidade, int(r.ano),
        )
        for r in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.fato_empenho (
                id_empenho_origem, codigo_empenho, sk_tempo, sk_orgao, sk_credor, valor, modalidade, ano
            ) VALUES %s
            ON CONFLICT (id_empenho_origem, ano) DO UPDATE SET
                sk_tempo = EXCLUDED.sk_tempo,
                sk_orgao = EXCLUDED.sk_orgao,
                sk_credor = EXCLUDED.sk_credor,
                valor = EXCLUDED.valor,
                modalidade = EXCLUDED.modalidade
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def load_fato_ordem_bancaria(conn, fato: pd.DataFrame, dim_tempo, dim_orgao, dim_credor) -> int:
    df = fato.merge(dim_tempo, left_on="data_emissao", right_on="data", how="left")
    df = df.merge(dim_orgao, on="codigo_orgao", how="left")
    df = df.merge(dim_credor, on="cpf_cnpj", how="left")

    rows = [
        (
            r.id_ob_origem, r.codigo,
            (None if pd.isna(r.sk_tempo) else int(r.sk_tempo)),
            (None if pd.isna(r.sk_orgao) else int(r.sk_orgao)),
            (None if pd.isna(r.sk_credor) else int(r.sk_credor)),
            float(r.valor), r.tipo_ob, int(r.ano),
        )
        for r in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.fato_ordem_bancaria (
                id_ob_origem, codigo, sk_tempo, sk_orgao, sk_credor, valor, tipo_ob, ano
            ) VALUES %s
            ON CONFLICT (id_ob_origem, ano) DO UPDATE SET
                sk_tempo = EXCLUDED.sk_tempo,
                sk_orgao = EXCLUDED.sk_orgao,
                sk_credor = EXCLUDED.sk_credor,
                valor = EXCLUDED.valor,
                tipo_ob = EXCLUDED.tipo_ob
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def run_gold_load() -> dict:
    from src.transformers.silver_to_gold import run_silver_to_gold

    gold = run_silver_to_gold()
    conn = connect_dw()
    try:
        dim_tempo = upsert_dim_tempo(conn, gold["dim_tempo"])
        dim_orgao = upsert_dim_orgao(conn, gold["dim_orgao"])
        dim_modalidade = upsert_dim_modalidade(conn, gold["dim_modalidade"])
        dim_credor = upsert_dim_credor_scd2(conn, gold["dim_credor"])

        n_contrato = load_fato_contrato(
            conn, gold["fato_contrato"], dim_tempo, dim_orgao, dim_modalidade, dim_credor
        )
        n_empenho = load_fato_empenho(conn, gold["fato_empenho"], dim_tempo, dim_orgao, dim_credor)
        n_ob = load_fato_ordem_bancaria(
            conn, gold["fato_ordem_bancaria"], dim_tempo, dim_orgao, dim_credor
        )

        logger.info(
            "Gold carregado: fato_contrato=%d fato_empenho=%d fato_ordem_bancaria=%d",
            n_contrato, n_empenho, n_ob,
        )
        return {"fato_contrato": n_contrato, "fato_empenho": n_empenho, "fato_ordem_bancaria": n_ob}
    finally:
        conn.close()


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO)
    print(json.dumps(run_gold_load(), indent=2))
