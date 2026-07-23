"""Componente de IA Generativa — gera um relatório narrativo em linguagem acessível para
gestores públicos sem formação técnica, a partir dos insights dos modelos de ML.

Usa a API da OpenAI (requer OPENAI_API_KEY no .env). Caso a chave não esteja configurada
ou a chamada falhe (rede, cota, etc.), o pipeline não falha: recorre a um relatório gerado
por template determinístico, garantindo que a DAG sempre produza uma saída utilizável.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd
from openai import OpenAI

from src.loaders.dw_loader import connect_dw
from src.utils.config import OPENAI_API_KEY, OPENAI_MODEL

logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).parent.parent / "reports"


def gather_insights(conn) -> dict:
    resumo_financeiro = pd.read_sql(
        """
        SELECT
            SUM(valor_contratado) AS total_contratado,
            SUM(valor_pago) AS total_pago,
            SUM(valor_empenhado) AS total_empenhado,
            COUNT(*) FILTER (WHERE valor_pago > valor_contratado) AS contratos_pagamento_acima_do_valor
        FROM dw.fato_contrato
        """,
        conn,
    ).iloc[0].to_dict()

    top_anomalos = pd.read_sql(
        """
        SELECT fc.sk_contrato, fc.num_contrato, fc.valor_contratado, fc.score_anomalia,
               dm.descricao_modalidade, dc.nome AS nome_credor, dorg.nome AS nome_orgao
        FROM dw.fato_contrato fc
        LEFT JOIN dw.dim_modalidade dm ON dm.sk_modalidade = fc.sk_modalidade
        LEFT JOIN dw.dim_credor dc ON dc.sk_credor = fc.sk_credor
        LEFT JOIN dw.dim_orgao dorg ON dorg.sk_orgao = fc.sk_orgao
        WHERE fc.score_anomalia IS NOT NULL
        ORDER BY fc.score_anomalia DESC
        LIMIT 10
        """,
        conn,
    )

    credores_com_infracao = pd.read_sql(
        """
        SELECT dc.nome, dc.cpf_cnpj, dc.historico_infringement, COUNT(fc.sk_contrato) AS qtd_contratos,
               SUM(fc.valor_contratado) AS valor_total
        FROM dw.dim_credor dc
        JOIN dw.fato_contrato fc ON fc.sk_credor = dc.sk_credor
        WHERE dc.historico_infringement > 0 AND dc.versao_atual
        GROUP BY dc.nome, dc.cpf_cnpj, dc.historico_infringement
        ORDER BY valor_total DESC
        LIMIT 10
        """,
        conn,
    )

    previsoes = pd.read_sql(
        """
        SELECT p.trimestre_referencia, dorg.nome AS nome_orgao, p.valor_previsto,
               p.intervalo_inferior, p.intervalo_superior, p.modelo
        FROM dw.previsao_pagamento_trimestral p
        JOIN dw.dim_orgao dorg ON dorg.sk_orgao = p.sk_orgao
        WHERE p.data_geracao = (SELECT MAX(data_geracao) FROM dw.previsao_pagamento_trimestral)
        ORDER BY p.valor_previsto DESC
        LIMIT 15
        """,
        conn,
    )

    return {
        "resumo_financeiro": resumo_financeiro,
        "top_anomalos": top_anomalos,
        "credores_com_infracao": credores_com_infracao,
        "previsoes": previsoes,
    }


def build_prompt(insights: dict) -> str:
    resumo = insights["resumo_financeiro"]
    anomalos_md = insights["top_anomalos"].to_markdown(index=False) if len(insights["top_anomalos"]) else "Nenhum contrato pontuado."
    infratores_md = insights["credores_com_infracao"].to_markdown(index=False) if len(insights["credores_com_infracao"]) else "Nenhum credor com histórico de infração encontrado."
    previsoes_md = insights["previsoes"].to_markdown(index=False) if len(insights["previsoes"]) else "Sem previsões disponíveis."

    return f"""
Você é um analista de dados do governo do Estado do Ceará. Escreva um relatório executivo,
em português claro e acessível para gestores públicos SEM formação técnica (evite jargão de
estatística/ML), com base exclusivamente nos dados abaixo. Estruture em 4 seções com títulos
markdown (##): "Rastreabilidade Financeira", "Contratos com Padrões Anômalos",
"Credores com Histórico de Infrações", "Previsão de Pagamentos do Próximo Trimestre".
Termine com uma seção "Recomendações" com 3 a 5 ações práticas e priorizadas.

### Resumo financeiro agregado
Total contratado: R$ {resumo['total_contratado']:,.2f}
Total pago: R$ {resumo['total_pago']:,.2f}
Total empenhado: R$ {resumo['total_empenhado']:,.2f}
Contratos com pagamento acima do valor contratado: {resumo['contratos_pagamento_acima_do_valor']}

### Top 10 contratos com maior score de anomalia (modelo Isolation Forest, 0 a 1)
{anomalos_md}

### Credores com histórico de infrações recebendo contratos
{infratores_md}

### Previsão de pagamentos do próximo trimestre por órgão (modelo XGBoost)
{previsoes_md}
"""


def call_llm(prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY não configurada")
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content


def fallback_template_report(insights: dict) -> str:
    resumo = insights["resumo_financeiro"]
    pct_pago = (resumo["total_pago"] / resumo["total_contratado"] * 100) if resumo["total_contratado"] else 0
    linhas = [
        "## Rastreabilidade Financeira",
        f"Do total contratado de R$ {resumo['total_contratado']:,.2f}, "
        f"R$ {resumo['total_pago']:,.2f} já foram efetivamente pagos ({pct_pago:.1f}%). "
        f"Foram identificados {resumo['contratos_pagamento_acima_do_valor']} contratos com "
        f"pagamentos acima do valor originalmente contratado — recomenda-se auditoria pontual.",
        "",
        "## Contratos com Padrões Anômalos",
        insights["top_anomalos"].to_markdown(index=False) if len(insights["top_anomalos"]) else "Nenhum contrato pontuado.",
        "",
        "## Credores com Histórico de Infrações",
        insights["credores_com_infracao"].to_markdown(index=False) if len(insights["credores_com_infracao"]) else "Nenhum credor com histórico de infração encontrado.",
        "",
        "## Previsão de Pagamentos do Próximo Trimestre",
        insights["previsoes"].to_markdown(index=False) if len(insights["previsoes"]) else "Sem previsões disponíveis.",
        "",
        "## Recomendações",
        "1. Auditar prioritariamente os contratos no topo do ranking de anomalias.",
        "2. Revisar processos de contratação de credores com histórico de infração.",
        "3. Reservar orçamento conforme as previsões trimestrais por órgão.",
        "4. Investigar contratos com pagamento acima do valor contratado.",
        "",
        "_Relatório gerado por template determinístico (API da OpenAI indisponível nesta execução)._",
    ]
    return "\n".join(linhas)


def run_report_generation() -> dict:
    conn = connect_dw()
    try:
        insights = gather_insights(conn)
        prompt = build_prompt(insights)
        try:
            conteudo = call_llm(prompt)
            gerado_por = OPENAI_MODEL
        except Exception:
            logger.exception("Falha ao chamar a API da OpenAI — usando relatório por template.")
            conteudo = fallback_template_report(insights)
            gerado_por = "template_fallback"

        titulo = f"Relatório de Insights — Pipeline Ceará Transparente ({date.today().isoformat()})"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_path = REPORTS_DIR / f"relatorio_{date.today().isoformat()}.md"
        report_path.write_text(f"# {titulo}\n\n{conteudo}\n", encoding="utf-8")

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dw.relatorio_insights (titulo, conteudo_markdown) VALUES (%s, %s)",
                (titulo, conteudo),
            )
        conn.commit()

        logger.info("Relatório gerado por %s em %s", gerado_por, report_path)
        return {"report_path": str(report_path), "gerado_por": gerado_por}
    finally:
        conn.close()


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO)
    print(json.dumps(run_report_generation(), indent=2))
