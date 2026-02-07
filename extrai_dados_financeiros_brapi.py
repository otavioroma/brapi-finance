import os
import csv
from dotenv import load_dotenv
from brapi import Brapi
from brapi import (
    APIError,
    APIConnectionError,
    BadRequestError,
    AuthenticationError,
    PermissionDeniedError,
    NotFoundError,
    UnprocessableEntityError,
    RateLimitError,
    InternalServerError,
)

# 1. CARREGAMENTO DE CONFIGURAÇÕES
load_dotenv()

# 2. INSTÂNCIA ÚNICA E REUTILIZÁVEL
client = Brapi(
    api_key=os.getenv("BRAPI_TOKEN"), 
    timeout=10.0,
    max_retries=3
)

def format_periodo(end_date):
    # end_date geralmente e date; aceita str como fallback
    if end_date is None:
        return "N/A"
    if hasattr(end_date, "year") and hasattr(end_date, "month"):
        year = end_date.year
        month = end_date.month
    else:
        # tentativa simples para strings tipo YYYY-MM-DD
        try:
            parts = str(end_date).split("-")
            year = int(parts[0])
            month = int(parts[1])
        except Exception:
            return str(end_date)

    trimestre_map = {3: "1T", 6: "2T", 9: "3T", 12: "4T"}
    tri = trimestre_map.get(month)
    if not tri:
        return f"{year}-{month:02d}"
    return f"{tri}{year}"

def get_financial_report(ticker):
    """
    Busca e processa dados financeiros trimestrais de forma robusta.
    """
    try:
        response = client.quote.retrieve(
            tickers=ticker,
            modules=[
                "balanceSheetHistoryQuarterly",
                "incomeStatementHistoryQuarterly",
                "cashflowHistoryQuarterly",
            ],
            timeout=5.0,
        )

        res = response.results[0]

        balancos = getattr(res, "balance_sheet_history_quarterly", None) or []
        dres = getattr(res, "income_statement_history_quarterly", None) or []
        fluxos = getattr(res, "cashflow_history_quarterly", None) or []

        resultados_trimestrais = []
        periodo_tipo = "trimestral"

        if not balancos:
            # Fallback anual quando nao ha dados trimestrais
            periodo_tipo = "anual"
            response_annual = client.quote.retrieve(
                tickers=ticker,
                modules=[
                    "balanceSheetHistory",
                    "incomeStatementHistory",
                    "cashflowHistory",
                ],
                timeout=5.0,
            )
            res_annual = response_annual.results[0]
            balancos = getattr(res_annual, "balance_sheet_history", None) or []
            dres = getattr(res_annual, "income_statement_history", None) or []
            fluxos = getattr(res_annual, "cashflow_history", None) or []

        for b in balancos:
            data_alvo = b.end_date
            d = next((item for item in dres if item.end_date == data_alvo), None)
            f = next((item for item in fluxos if item.end_date == data_alvo), None)

            # Balan?o sempre existe aqui (b). DRE e Fluxo podem nao existir.
            pl = getattr(b, "total_stockholder_equity", None)
            divida_cp = getattr(b, "short_long_term_debt", None)
            divida_lp = getattr(b, "long_term_debt", None)
            caixa = getattr(b, "cash", None)

            # Se faltarem dados do balanco, trate como None
            if pl is None or divida_cp is None or divida_lp is None or caixa is None:
                divida_bruta = None
                divida_liquida = None
            else:
                divida_bruta = divida_cp + divida_lp
                divida_liquida = divida_bruta - caixa

            if d is None:
                lucro = None
                receita_liquida = None
                ebitda = None
            else:
                lucro = getattr(d, "net_income", None)
                receita_liquida = getattr(d, "total_revenue", None)
                ebitda = getattr(d, "ebitda", None)
                if ebitda is None:
                    ebitda = getattr(d, "ebit", None)

            if f is None:
                capex = None
            else:
                capex = getattr(f, "capital_expenditures", None)
                if capex is None:
                    capex = getattr(f, "investment_cash_flow", None)
                    if capex is None:
                        capex = getattr(f, "investments", None)
                if capex is not None:
                    capex = abs(capex)

            # Calculos com NULL quando nao houver dados
            if lucro is None or divida_bruta is None:
                roic = None
            else:
                cap_investido = pl + divida_bruta
                roic = (lucro / cap_investido) * 100 if cap_investido > 0 else 0

            if ebitda is None or receita_liquida in (None, 0):
                margem_ebitda = None
            else:
                margem_ebitda = (ebitda / receita_liquida) * 100

            if capex is None or receita_liquida in (None, 0):
                capex_receita = None
            else:
                capex_receita = (capex / receita_liquida) * 100

            if divida_liquida is None or ebitda in (None, 0):
                alavancagem = None
            else:
                alavancagem = divida_liquida / (ebitda * 4)

            resultados_trimestrais.append({
                "periodo": format_periodo(data_alvo),
                "receita": receita_liquida,
                "lucro": lucro,
                "ebitda": ebitda,
                "margem_ebitda": margem_ebitda,
                "roic": roic,
                "capex": capex,
                "capex_receita": capex_receita,
                "alavancagem": alavancagem,
            })

        return {"ticker": ticker, "periodo_tipo": periodo_tipo, "historico": resultados_trimestrais}

    except BadRequestError as e:
        print(f"[400] Requisicao invalida para {ticker}: {e}")
    except AuthenticationError:
        print("[401] Token invalido ou nao encontrado no .env.")
    except PermissionDeniedError:
        print(f"[403] Sem permissao para acessar os dados de {ticker}.")
    except NotFoundError:
        print(f"[404] Ticker '{ticker}' nao encontrado na B3.")
    except UnprocessableEntityError:
        print(f"[422] Erro de processamento nos dados de {ticker}.")
    except RateLimitError:
        print("[429] Limite de requisicoes atingido.")
    except InternalServerError:
        print("[5xx] Erro interno nos servidores da Brapi.")
    except APIConnectionError:
        print("Erro de Conexao: Verifique sua internet.")
    except Exception as e:
        print(f"Erro inesperado ao processar {ticker}: {e}")

    return None

if __name__ == "__main__":
    empresas = ["VIVT3", "TIMS3", "DESK3", "FIQE3"]
    def fmt_val(v, kind="num"):
        if v is None:
            return "NULL"
        if kind == "money":
            return f"R$ {v:>18,.2f}"
        if kind == "pct":
            return f"{v:>6.2f}%"
        if kind == "ratio":
            return f"{v:>11.2f}x"
        return str(v)

    rows = []
    for t in empresas:
        dados = get_financial_report(t)
        if dados:
            print(f"\n{'='*70}")
            print(f"ATIVO: {dados['ticker']}")
            print(f"Tipo de dados: {dados.get('periodo_tipo', "trimestral")}")
            print(f"{'='*70}")
            historico = dados['historico']
            # Ordena por ano/trimestre e filtra de 1T2024 em diante
            def key_tri(item):
                p = item.get('periodo', '')
                # p formato: '1T2024'
                if len(p) >= 6 and 'T' in p:
                    t, y = p.split('T')
                    try:
                        return (int(y), int(t))
                    except Exception:
                        return (0, 0)
                return (0, 0)

            historico = sorted(historico, key=key_tri)
            historico = [h for h in historico if key_tri(h) >= (2024, 1)]
            for tri in historico:
                rows.append({
                    'ticker': dados['ticker'],
                    'tipo_dados': dados.get('periodo_tipo', 'trimestral'),
                    'periodo': tri.get('periodo'),
                    'receita': tri.get('receita'),
                    'lucro': tri.get('lucro'),
                    'ebitda': tri.get('ebitda'),
                    'margem_ebitda': tri.get('margem_ebitda'),
                    'roic': tri.get('roic'),
                    'capex': tri.get('capex'),
                    'capex_receita': tri.get('capex_receita'),
                    'alavancagem': tri.get('alavancagem'),
                })
                print(f"   Período: {tri['periodo']}")
                print(f"   Receita Liq: {fmt_val(tri['receita'], 'money')} | ROIC: {fmt_val(tri['roic'], 'pct')}")
                print(f"   EBITDA:      {fmt_val(tri['ebitda'], 'money')} | Margem: {fmt_val(tri['margem_ebitda'], 'pct')}")
                print(f"   Capex:       {fmt_val(tri['capex'], 'money')} | Capex/Rec: {fmt_val(tri['capex_receita'], 'pct')}")
                print(f"   Div. Liq/EBITDA: {fmt_val(tri['alavancagem'], 'ratio')}")
                print(f"{'-'*70}")
    if rows:
        output_csv = 'saida_financeira.csv'
        campos = [
            'ticker', 'tipo_dados', 'periodo', 'receita', 'lucro', 'ebitda',
            'margem_ebitda', 'roic', 'capex', 'capex_receita', 'alavancagem'
        ]
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            writer.writerows(rows)
        print(f"CSV gerado: {output_csv}")
    print(f"\nProcessamento Finalizado.")
