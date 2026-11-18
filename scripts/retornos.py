import csv
import hashlib
import json
import re
import time
import urllib.request
from pathlib import Path

import duckdb

# Fase 5 (rodada 3) — retornos diários e mensais por série e CDI. Regras (rodada3_final.md e respostas do
# responsável de 18/11/2026):
#   - P = preco_por_acao (fechamento / fatcot), sempre;
#   - r_t = (P_t·F_t + D_t)/P_s − 1, (s, t] entre pregões do ticker; F_t = produto dos fatores de quantidade e
#     D_t = soma dos proventos com data ex em (s, t], por ação antiga (D28 B), todos os tipos (D24, D27);
#     retorno de preço sem D_t; sensibilidades: JCP × 0,85 (D30) e sem as parcelas repetidas além da primeira (D23);
#   - dia NULL: início de marca G/B sem evento ou mudança do fator de cotação sem evento (`pendencias_eventos`);
#     |r| >= 40% sem nenhuma marca: calculado, flag `salto_sem_marca`; mês com dia NULL → NULL com as flags;
#   - saídas só verificadas (D18, D21, D31); não verificada ou indeterminado → NULL, flag `saida_nao_verificada`;
#   - troca de ticker na série: r = P_sucessor × relacao_troca / P_antigo − 1;
#   - corte D32: nada depois de 2026-10-31 (checagem que interrompe).

DATA_CORTE = "2026-10-31"
JCP_LIQUIDO = 0.85
SAIDA_ZERO = {"incorporacao", "migracao_exterior", "desmembramento"}
FALENCIA, FALENCIA_SENS = -0.30, (-1.0, 0.0)
NULO_DIA = ("marca_gb_sem_evento", "mudou_fatcot_sem_evento", "acoes_entregues_pendente")
FLAG_DIA = ("salto_sem_marca", "quantidade_pendente")  # só marcam; retorno calculado
EXTREMO_ALTA, EXTREMO_BAIXA = 2.0, -0.8  # lista de retornos mensais extremos (só lista)
SAIDAS = Path("dados/manuais/saidas.csv")
PASTA_BCB = Path("dados/brutos/bcb")
PASTA_YF = Path("dados/brutos/yfinance")
CDI_JANELAS = [("01/01/2010", "31/12/2019"), ("01/01/2020", "31/10/2026")]
N_VALIDACAO = 15
MAIORES = 30
AGENTE = "Mozilla/5.0 (X11; Linux x86_64) pesquisa-academica-PIBIC-UnB"
PAUSA_S = 3


def guarda(destino, conteudo):
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_bytes(conteudo)
    with open("dados/hashes.txt", "a") as hashes:
        hashes.write(f"{hashlib.sha256(conteudo).hexdigest()}  {destino}\n")


# CDI: SGS série 12 (taxa diária em %), em janelas de até 10 anos
cdi = []
for ini, fim in CDI_JANELAS:
    destino = PASTA_BCB / f"sgs12_{ini[6:]}{ini[3:5]}{ini[:2]}_{fim[6:]}{fim[3:5]}{fim[:2]}.json"
    if not destino.exists():
        url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json"
               f"&dataInicial={ini}&dataFinal={fim}")
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": AGENTE}), timeout=120) as r:
            guarda(destino, r.read())
        time.sleep(PAUSA_S)
    cdi += [(f"{x['data'][6:]}-{x['data'][3:5]}-{x['data'][:2]}", float(x["valor"])) for x in json.loads(destino.read_text())]

with open(SAIDAS, newline="") as arq:
    saidas = [(r["ticker"], r["tipo"], r["verificado"].strip().lower() == "verdadeiro",
               float(r["valor_dinheiro"].replace(",", ".")) if r["valor_dinheiro"].strip() else None,
               float(r["ultimo_preco"])) for r in csv.DictReader(arq)]

con = duckdb.connect("dados/pibic.duckdb")
con.begin()
con.sql("CREATE TEMP TABLE cdi_bruto (data DATE, taxa_pct DOUBLE)")
con.executemany("INSERT INTO cdi_bruto VALUES (?, ?)", cdi)
con.sql("CREATE TEMP TABLE saidas (ticker VARCHAR, tipo VARCHAR, verificado BOOLEAN, valor_dinheiro DOUBLE, ultimo_preco DOUBLE)")
con.executemany("INSERT INTO saidas VALUES (?, ?, ?, ?, ?)", saidas)

# 1. Séries do universo por série e seus tickers; todos devem estar no escopo dos eventos
con.sql("""
    CREATE TEMP TABLE serie_ticker AS
    SELECT s.serie_id, s.ticker, s.data_inicio, s.data_fim FROM series s
    WHERE s.serie_id IN (SELECT serie_id FROM universo_mensal_serie)
""")
fora = con.sql("""SELECT string_agg(ticker, ' ' ORDER BY ticker) FROM serie_ticker
                  WHERE ticker NOT IN (SELECT ticker FROM universo_mensal)
                    AND ticker NOT IN (SELECT ticker_novo FROM ligacoes WHERE usada)""").fetchall()[0][0]
if fora:
    raise ValueError(f"tickers de séries do universo sem eventos coletados (fora do escopo): {fora}")

# 2. Eventos até o corte; parcela repetida além da primeira de cada grupo (sensibilidade D23)
con.sql(f"""
    CREATE TEMP TABLE ev AS
    SELECT *, parcela_repetida AND seq > min(seq) OVER (PARTITION BY ticker, data_ex, tipo_provento, valor_dinheiro)
              AS repetida_extra
    FROM eventos WHERE data_ex <= DATE '{DATA_CORTE}' AND ticker IN (SELECT ticker FROM serie_ticker)
""")

# 3. Retorno diário por ticker no intervalo (s, t]
con.sql(f"""
    CREATE TEMP TABLE diario_ticker AS
    WITH d AS (
        SELECT a.ticker, a.data, a.preco_por_acao AS p, a.marca,
               lag(a.data) OVER w AS data_ant, lag(a.preco_por_acao) OVER w AS p_ant
        FROM acoes a WHERE a.ticker IN (SELECT ticker FROM serie_ticker) AND a.data <= DATE '{DATA_CORTE}'
        WINDOW w AS (PARTITION BY a.ticker ORDER BY a.data)),
    e AS (
        SELECT d.ticker, d.data,
               coalesce(product(v.fator_quantidade) FILTER (WHERE v.tipo_evento <> 'provento'), 1) AS fator,
               coalesce(sum(v.valor_dinheiro) FILTER (WHERE v.tipo_evento = 'provento'), 0) AS dinheiro,
               coalesce(sum(v.valor_dinheiro * CASE WHEN v.tipo_provento = 'jcp' THEN {JCP_LIQUIDO} ELSE 1 END)
                        FILTER (WHERE v.tipo_evento = 'provento'), 0) AS dinheiro_jcp_liquido,
               coalesce(sum(v.valor_dinheiro) FILTER (WHERE v.tipo_evento = 'provento' AND NOT v.repetida_extra), 0)
                   AS dinheiro_sem_repetidos,
               max(v.data_ex) AS maior_data_ex
        FROM d JOIN ev v ON v.ticker = d.ticker AND v.data_ex > d.data_ant AND v.data_ex <= d.data
        GROUP BY d.ticker, d.data)
    SELECT d.ticker, d.data, d.data_ant, d.p, d.p_ant, coalesce(e.fator, 1) AS fator, coalesce(e.dinheiro, 0) AS dinheiro,
           coalesce(e.dinheiro_jcp_liquido, 0) AS dinheiro_jcp_liquido,
           coalesce(e.dinheiro_sem_repetidos, 0) AS dinheiro_sem_repetidos, e.maior_data_ex,
           (SELECT string_agg(motivo, ',' ORDER BY motivo) FROM pendencias_eventos x
             WHERE x.ticker = d.ticker AND x.data > coalesce(d.data_ant, d.data - 1) AND x.data <= d.data
               AND list_contains({list(NULO_DIA + FLAG_DIA)}, x.motivo)) AS flags
    FROM d LEFT JOIN e USING (ticker, data)
""")

# 4. Série: tickers em sequência (o antigo até o último pregão; o sucessor depois); troca com a relação de troca
con.sql("""
    CREATE TEMP TABLE diario_serie AS
    WITH seg AS (
        SELECT t.*, l.ticker_antigo, l.relacao_troca,
               (SELECT max(data_fim) FROM serie_ticker o WHERE o.serie_id = t.serie_id AND o.ticker = l.ticker_antigo)
                   AS fim_antigo
        FROM serie_ticker t LEFT JOIN ligacoes l ON l.usada AND l.ticker_novo = t.ticker)
    SELECT g.serie_id, d.*, g.ticker_antigo, g.relacao_troca,
           g.ticker_antigo IS NOT NULL AND d.data = min(d.data) OVER (PARTITION BY g.serie_id, d.ticker) AS troca_ticker,
           (SELECT a.preco_por_acao FROM acoes a WHERE a.ticker = g.ticker_antigo AND a.data = g.fim_antigo) AS p_antigo
    FROM diario_ticker d JOIN seg g USING (ticker)
    WHERE g.fim_antigo IS NULL OR d.data > g.fim_antigo
""")
con.sql(f"""
    CREATE OR REPLACE TABLE retornos_diarios AS
    WITH r AS (
        SELECT serie_id, ticker, data, data_ant, p, p_ant, fator, dinheiro, troca_ticker,
               p_ant IS NULL AND NOT troca_ticker AS inicio,
               CASE WHEN troca_ticker THEN concat_ws(',', 'troca_ticker', flags) ELSE flags END AS flags,
               CASE WHEN troca_ticker THEN p * relacao_troca / p_antigo - 1 ELSE (p * fator + dinheiro) / p_ant - 1 END AS rt,
               CASE WHEN troca_ticker THEN p * relacao_troca / p_antigo - 1 ELSE p * fator / p_ant - 1 END AS rp,
               CASE WHEN troca_ticker THEN p * relacao_troca / p_antigo - 1
                    ELSE (p * fator + dinheiro_jcp_liquido) / p_ant - 1 END AS rj,
               CASE WHEN troca_ticker THEN p * relacao_troca / p_antigo - 1
                    ELSE (p * fator + dinheiro_sem_repetidos) / p_ant - 1 END AS rs,
               maior_data_ex
        FROM diario_serie)
    SELECT serie_id, data, ticker, data_ant AS data_anterior, p AS preco, p_ant AS preco_anterior, fator, dinheiro,
           inicio,
           CASE WHEN NOT nulo THEN rt END AS retorno_total,
           CASE WHEN NOT nulo THEN rp END AS retorno_preco,
           CASE WHEN NOT nulo THEN rj END AS retorno_total_jcp_liquido,
           CASE WHEN NOT nulo THEN rs END AS retorno_total_sem_repetidos,
           flags, maior_data_ex
    FROM (SELECT *, coalesce(list_has_any(string_split(flags, ','), {list(NULO_DIA)}), FALSE) AS nulo FROM r)
    ORDER BY serie_id, data
""")

# 5. Retorno de saída (só linhas verificadas); série que termina antes do fim da base
con.sql(f"""
    CREATE TEMP TABLE fim_serie AS
    WITH ult AS (SELECT serie_id, arg_max(ticker, data) AS ticker, max(data) AS ultimo FROM retornos_diarios GROUP BY serie_id)
    SELECT u.*, s.tipo, s.verificado, s.valor_dinheiro, s.ultimo_preco,
           CASE WHEN s.ticker IS NULL THEN NULL
                WHEN NOT s.verificado OR s.tipo = 'indeterminado' THEN NULL
                WHEN list_contains({list(SAIDA_ZERO)}, s.tipo) THEN 0.0
                WHEN s.tipo = 'opa_dinheiro' THEN coalesce(s.valor_dinheiro / s.ultimo_preco - 1, 0.0)
                WHEN s.tipo = 'falencia' THEN {FALENCIA} END AS retorno_saida,
           CASE WHEN s.ticker IS NULL THEN 'saida_sem_registro'
                WHEN NOT s.verificado OR s.tipo = 'indeterminado' THEN 'saida_nao_verificada'
                WHEN s.tipo NOT IN ('incorporacao', 'migracao_exterior', 'desmembramento', 'opa_dinheiro', 'falencia')
                     THEN 'saida_ligacao_nao_usada' END AS flag_saida
    FROM ult u LEFT JOIN saidas s USING (ticker)
    WHERE u.ultimo < (SELECT max(data) FROM acoes WHERE data <= DATE '{DATA_CORTE}')
      -- série suspensa que volta a negociar depois do corte (ex.: RSID3) não é saída
      AND NOT EXISTS (SELECT 1 FROM acoes a JOIN serie_ticker t USING (ticker)
                      WHERE t.serie_id = u.serie_id AND a.data > DATE '{DATA_CORTE}')
""")

# 6. Retorno mensal: composição dos diários do mês (o primeiro pregão da série não tem retorno);
#    dia NULL → mês NULL com as flags; mês de saída composto com o retorno de saída
con.sql(f"""
    CREATE OR REPLACE TABLE retornos_mensais AS
    WITH meses AS (
        SELECT s.serie_id, unnest(generate_series(date_trunc('month', min(r.data)), date_trunc('month', max(r.data)),
                                                  INTERVAL 1 MONTH))::DATE AS mes
        FROM retornos_diarios r JOIN (SELECT DISTINCT serie_id FROM retornos_diarios) s USING (serie_id)
        GROUP BY s.serie_id),
    m AS (
        SELECT serie_id, date_trunc('month', data)::DATE AS mes, arg_max(ticker, data) AS ticker_vigente,
               count(*) AS pregoes, bool_or(inicio) AS mes_inicio,
               bool_or(retorno_total IS NULL AND NOT inicio) AS tem_nulo,
               exp(sum(ln(1 + retorno_total)) FILTER (WHERE NOT inicio)) AS ft,
               exp(sum(ln(1 + retorno_preco)) FILTER (WHERE NOT inicio)) AS fp,
               exp(sum(ln(1 + retorno_total_jcp_liquido)) FILTER (WHERE NOT inicio)) AS fj,
               exp(sum(ln(1 + retorno_total_sem_repetidos)) FILTER (WHERE NOT inicio)) AS fs,
               string_agg(DISTINCT x.flag, ',' ORDER BY x.flag) AS flags_dias
        FROM retornos_diarios LEFT JOIN LATERAL (SELECT unnest(string_split(flags, ',')) AS flag) x ON TRUE
        GROUP BY serie_id, mes),
    -- pendência que anula o dia, com data depois do último pregão do ticker (ex.: TMAR5 2012-04-09): anula o mês
    p AS (
        SELECT t.serie_id, date_trunc('month', x.data)::DATE AS mes, string_agg(DISTINCT x.motivo, ',' ORDER BY x.motivo) AS flags_pend
        FROM pendencias_eventos x JOIN serie_ticker t USING (ticker)
        WHERE list_contains({list(NULO_DIA)}, x.motivo) AND x.data > t.data_fim AND x.data <= DATE '{DATA_CORTE}'
        GROUP BY ALL)
    SELECT k.mes, k.serie_id, m.ticker_vigente, coalesce(m.pregoes, 0) AS pregoes,
           f.serie_id IS NOT NULL AND k.mes = date_trunc('month', f.ultimo) AS mes_saida,
           CASE WHEN f.serie_id IS NOT NULL AND k.mes = date_trunc('month', f.ultimo) THEN f.retorno_saida END AS retorno_saida,
           CASE WHEN nulo THEN NULL ELSE coalesce(m.ft, 1) * coalesce(1 + saida, 1) - 1 END AS retorno_total,
           CASE WHEN nulo THEN NULL ELSE coalesce(m.fp, 1) * coalesce(1 + saida, 1) - 1 END AS retorno_preco,
           CASE WHEN nulo THEN NULL ELSE coalesce(m.fj, 1) * coalesce(1 + saida, 1) - 1 END AS retorno_total_jcp_liquido,
           CASE WHEN nulo THEN NULL ELSE coalesce(m.fs, 1) * coalesce(1 + saida, 1) - 1 END AS retorno_total_sem_repetidos,
           CASE WHEN nulo THEN NULL ELSE coalesce(m.ft, 1) * coalesce(1 + saida_100, 1) - 1 END AS retorno_total_falencia_100,
           CASE WHEN nulo THEN NULL ELSE coalesce(m.ft, 1) * coalesce(1 + saida_0, 1) - 1 END AS retorno_total_falencia_0,
           nullif(concat_ws(',', CASE WHEN m.pregoes IS NULL THEN 'sem_negocio' END,
                            CASE WHEN m.mes_inicio THEN 'mes_inicio' END, m.flags_dias, p.flags_pend, saida_flag), '') AS flags
    FROM meses k LEFT JOIN m USING (serie_id, mes) LEFT JOIN p USING (serie_id, mes) LEFT JOIN fim_serie f USING (serie_id),
         LATERAL (SELECT f.serie_id IS NOT NULL AND k.mes = date_trunc('month', f.ultimo) AS e_saida) z,
         LATERAL (SELECT CASE WHEN z.e_saida THEN f.retorno_saida END AS saida,
                         CASE WHEN z.e_saida THEN CASE WHEN f.tipo = 'falencia' AND f.verificado THEN {FALENCIA_SENS[0]}
                                                       ELSE f.retorno_saida END END AS saida_100,
                         CASE WHEN z.e_saida THEN CASE WHEN f.tipo = 'falencia' AND f.verificado THEN {FALENCIA_SENS[1]}
                                                       ELSE f.retorno_saida END END AS saida_0,
                         CASE WHEN z.e_saida THEN f.flag_saida END AS saida_flag) w,
         LATERAL (SELECT m.pregoes IS NULL OR m.tem_nulo OR p.flags_pend IS NOT NULL
                         OR (z.e_saida AND f.retorno_saida IS NULL) AS nulo) n
    ORDER BY k.serie_id, k.mes
""")

# 7. CDI diário e mensal composto, até o corte
con.sql(f"""
    CREATE OR REPLACE TABLE cdi_diario AS
    SELECT data, taxa_pct, taxa_pct / 100 AS retorno FROM cdi_bruto WHERE data <= DATE '{DATA_CORTE}' ORDER BY data
""")
con.sql("""
    CREATE OR REPLACE TABLE cdi_mensal AS
    SELECT date_trunc('month', data)::DATE AS mes, exp(sum(ln(1 + retorno))) - 1 AS retorno, count(*) AS dias
    FROM cdi_diario GROUP BY mes ORDER BY mes
""")

# ============================ Checagens ============================
print("Fase 5 — retornos e CDI (corte D32: até", DATA_CORTE, ")")
print("26/10/2026: queda de mercado pós-eleição (Ibovespa −13,88%); retornos calculados normalmente.\n")
datas = con.sql("""
    SELECT 'retornos_diarios', max(data)::VARCHAR FROM retornos_diarios
    UNION ALL SELECT 'retornos_mensais', max(mes)::VARCHAR FROM retornos_mensais
    UNION ALL SELECT 'cdi_diario', max(data)::VARCHAR FROM cdi_diario
    UNION ALL SELECT 'cdi_mensal', max(mes)::VARCHAR FROM cdi_mensal
    UNION ALL SELECT 'eventos usados (maior data ex)', max(maior_data_ex)::VARCHAR FROM retornos_diarios
""").fetchall()
for tabela, maior in datas:
    print(f"Maior data usada — {tabela}: {maior}")
if any(maior and maior > DATA_CORTE for _, maior in datas):
    raise ValueError("D32: data posterior ao corte em tabela da Fase 5")

for tabela, chave in (("retornos_diarios", "serie_id, data"), ("retornos_mensais", "serie_id, mes"),
                      ("cdi_diario", "data"), ("cdi_mensal", "mes")):
    n, k = con.sql(f"SELECT count(*), count(DISTINCT ({chave})) FROM {tabela}").fetchall()[0]
    print(f"{tabela}: {n} linhas; chaves distintas {k}")
    if n != k:
        raise ValueError(f"{tabela}: chave repetida")
abaixo = con.sql("""
    SELECT (SELECT count(*) FROM retornos_diarios WHERE retorno_total <= -1 OR retorno_preco <= -1
                OR retorno_total_jcp_liquido <= -1 OR retorno_total_sem_repetidos <= -1),
           (SELECT count(*) FROM retornos_mensais WHERE retorno_total < -1 OR retorno_preco < -1
                OR retorno_total_falencia_100 < -1)
""").fetchall()[0]
print(f"Retornos < −100%: diários {abaixo[0]}; mensais {abaixo[1]}")
if any(abaixo):
    raise ValueError("retorno abaixo de −100%")

print("\nDistribuição dos retornos mensais totais (não nulos)")
con.sql("""
    SELECT count(retorno_total) AS meses, count(*) - count(retorno_total) AS nulos,
           round(quantile_cont(retorno_total, 0.001), 4) AS p0_1, round(quantile_cont(retorno_total, 0.01), 4) AS p1,
           round(median(retorno_total), 4) AS p50, round(quantile_cont(retorno_total, 0.99), 4) AS p99,
           round(quantile_cont(retorno_total, 0.999), 4) AS p99_9
    FROM retornos_mensais
""").show()
print(f"{MAIORES} maiores |retorno mensal total| com eventos e flags do mês")
con.sql(f"""
    SELECT m.serie_id, m.mes, m.ticker_vigente, round(m.retorno_total, 4) AS retorno_total, m.flags,
           (SELECT string_agg(e.tipo_evento || coalesce(' ' || e.tipo_provento, '') || ' '
                              || coalesce(round(e.fator_quantidade, 4)::VARCHAR, round(e.valor_dinheiro, 4)::VARCHAR)
                              || ' ' || e.data_ex, '; ' ORDER BY e.data_ex, e.tipo_evento, e.seq)
              FROM ev e JOIN serie_ticker t ON t.ticker = e.ticker
             WHERE t.serie_id = m.serie_id AND date_trunc('month', e.data_ex) = m.mes) AS eventos_do_mes,
           (SELECT count(*) FROM pendencias_eventos p JOIN serie_ticker t ON t.ticker = p.ticker
             WHERE t.serie_id = m.serie_id AND date_trunc('month', p.data) = m.mes
               AND p.motivo = 'salto_com_marca') AS saltos_com_marca
    FROM retornos_mensais m WHERE m.retorno_total IS NOT NULL
    ORDER BY abs(m.retorno_total) DESC, m.serie_id, m.mes LIMIT {MAIORES}
""").show(max_rows=MAIORES, max_width=300)

print(f"Retornos mensais totais > {EXTREMO_ALTA:+.0%} ou < {EXTREMO_BAIXA:+.0%} (com ou sem flag): retornos diários do mês "
      "(data, ticker, retorno total, preço anterior → preço, marca do ESPECI), eventos usados e pendências. Só lista.")
con.sql(f"""
    SELECT m.serie_id, m.mes, round(m.retorno_total, 4) AS retorno_total, m.flags,
           (SELECT string_agg(strftime(r.data, '%d') || ' ' || r.ticker || ' ' || coalesce(round(r.retorno_total, 4)::VARCHAR, 'NULL')
                              || ' (' || round(r.preco_anterior, 4) || '→' || round(r.preco, 4)
                              || coalesce(' ' || nullif(a.marca, ''), '') || ')', '; ' ORDER BY r.data)
              FROM retornos_diarios r JOIN acoes a ON a.ticker = r.ticker AND a.data = r.data
             WHERE r.serie_id = m.serie_id AND date_trunc('month', r.data) = m.mes) AS diarios,
           (SELECT string_agg(e.tipo_evento || coalesce(' ' || e.tipo_provento, '') || ' '
                              || coalesce(round(e.fator_quantidade, 6)::VARCHAR, round(e.valor_dinheiro, 6)::VARCHAR)
                              || ' ' || e.data_ex || ' (' || e.fonte || ', ' || e.confirmacao || ')', '; '
                              ORDER BY e.data_ex, e.tipo_evento, e.seq)
              FROM ev e JOIN serie_ticker t ON t.ticker = e.ticker
             WHERE t.serie_id = m.serie_id AND date_trunc('month', e.data_ex) = m.mes) AS eventos,
           (SELECT string_agg(x.motivo || ' ' || x.data, '; ' ORDER BY x.data, x.motivo)
              FROM pendencias_eventos x JOIN serie_ticker t ON t.ticker = x.ticker
             WHERE t.serie_id = m.serie_id AND date_trunc('month', x.data) = m.mes) AS pendencias
    FROM retornos_mensais m
    WHERE m.retorno_total > {EXTREMO_ALTA} OR m.retorno_total < {EXTREMO_BAIXA}
    ORDER BY m.serie_id, m.mes
""").show(max_rows=200, max_width=100000, max_col_width=100000)

print("Meses-série com retorno total NULL, por motivo (flag), com quantos estavam no universo por série do mês")
con.sql("""
    CREATE TEMP TABLE nulos AS
    SELECT m.serie_id, m.mes, m.ticker_vigente, coalesce(nullif(f.flag, ''), '(sem flag)') AS motivo,
           EXISTS (SELECT 1 FROM universo_mensal_serie u WHERE u.serie_id = m.serie_id AND u.mes = m.mes) AS no_universo
    FROM retornos_mensais m, LATERAL (SELECT unnest(string_split(coalesce(m.flags, ''), ',')) AS flag) f
    WHERE m.retorno_total IS NULL
      AND (f.flag IN ('marca_gb_sem_evento', 'mudou_fatcot_sem_evento', 'acoes_entregues_pendente', 'sem_negocio',
                      'saida_nao_verificada',
                      'saida_sem_registro', 'saida_ligacao_nao_usada') OR m.flags IS NULL)
""")
con.sql("""SELECT motivo, count(*) AS meses_serie, count(DISTINCT serie_id) AS series,
                  count(*) FILTER (WHERE no_universo) AS no_universo
           FROM nulos GROUP BY motivo ORDER BY motivo""").show(max_width=200)
print("Por ano (meses-série NULL, distintos)")
con.sql("""PIVOT (SELECT DISTINCT serie_id, mes, year(mes) AS ano, motivo FROM nulos) ON motivo USING count(*)
           GROUP BY ano ORDER BY ano""").show(max_rows=30, max_width=250)
print("Por série (meses NULL; no universo)")
con.sql("""SELECT serie_id, string_agg(DISTINCT motivo, ',' ORDER BY motivo) AS motivos,
                  count(DISTINCT mes) AS meses, count(DISTINCT mes) FILTER (WHERE no_universo) AS no_universo
           FROM nulos GROUP BY serie_id ORDER BY meses DESC, serie_id""").show(max_rows=400, max_width=250)
print("Flags de dia (calculado com flag) e mudanças de ticker")
con.sql("""SELECT flag, count(*) AS dias FROM retornos_diarios, LATERAL (SELECT unnest(string_split(flags, ',')) AS flag)
           WHERE flags IS NOT NULL GROUP BY flag ORDER BY flag""").show()
print("Saídas (séries que terminam antes do fim da base)")
con.sql("""SELECT coalesce(flag_saida, 'retorno de saída definido') AS situacao, count(*) AS series
           FROM fim_serie GROUP BY ALL ORDER BY ALL""").show()

print("CDI: conferência do mensal composto contra a composição dos diários em Python")
diarios = con.sql("SELECT date_trunc('month', data)::DATE, retorno FROM cdi_diario ORDER BY data").fetchall()
composto = {}
for mes, r in diarios:
    composto[mes] = composto.get(mes, 1.0) * (1 + r)
dif = max(abs(composto[mes] - 1 - r) for mes, r in con.sql("SELECT mes, retorno FROM cdi_mensal").fetchall())
print(f"  meses {len(composto)}; maior diferença {dif:.2e}")
if dif > 1e-12:
    raise ValueError("CDI mensal composto não confere com os diários")
con.sql("SELECT min(mes), max(mes), round(min(retorno), 6) AS menor, round(max(retorno), 6) AS maior FROM cdi_mensal").show()

con.commit()

# ============================ Validação com o yfinance (D13) ============================
# Regra fixada antes de ver o resultado: séries de um ticker no universo por série em todos os meses de jan/2011 a
# out/2026, sem mudança do fator de cotação, por volume mediano médio; se faltarem, completar pelas de mais meses
# no universo (mesmos filtros). Nada é corrigido com base no yfinance; divergências viram lista.
escolhidos = [t for (t,) in con.sql(f"""
    WITH u AS (SELECT serie_id, count(*) AS meses, avg(volume_mediano) AS volume FROM universo_mensal_serie
               WHERE mes BETWEEN DATE '2011-01-01' AND DATE '{DATA_CORTE}' GROUP BY serie_id),
         um AS (SELECT serie_id FROM series GROUP BY serie_id HAVING count(*) = 1)
    SELECT u.serie_id FROM u JOIN um USING (serie_id)
    WHERE u.serie_id NOT IN (SELECT ticker FROM acoes WHERE mudou_fatcot)
    ORDER BY u.meses = (SELECT count(DISTINCT mes) FROM universo_mensal_serie WHERE mes <= DATE '{DATA_CORTE}') DESC,
             u.meses DESC, u.volume DESC, u.serie_id
    LIMIT {N_VALIDACAO}
""").fetchall()]
print(f"\nValidação com o yfinance — {len(escolhidos)} tickers fixados antes da comparação: {' '.join(escolhidos)}")
historicos = []
for t in escolhidos:
    destino = PASTA_YF / f"historico_{t}.json"
    if not destino.exists():
        import yfinance
        h = yfinance.Ticker(f"{t}.SA").history(start="2010-12-01", end="2026-11-01", auto_adjust=True)
        guarda(destino, json.dumps({str(k.date()): float(v) for k, v in h["Close"].items()}, indent=0).encode())
        time.sleep(PAUSA_S)
    historicos += [(t, d, v) for d, v in json.loads(destino.read_text()).items()]
con.sql("CREATE TEMP TABLE yf (ticker VARCHAR, data DATE, fechamento_ajustado DOUBLE)")
con.executemany("INSERT INTO yf VALUES (?, ?, ?)", historicos)
con.sql(f"""
    CREATE TEMP TABLE comparacao AS
    WITH y AS (SELECT ticker, date_trunc('month', data)::DATE AS mes, arg_max(fechamento_ajustado, data) AS f
               FROM yf WHERE data <= DATE '{DATA_CORTE}' GROUP BY ALL),
         ym AS (SELECT ticker, mes, f / lag(f) OVER (PARTITION BY ticker ORDER BY mes) - 1 AS r_yf FROM y)
    SELECT m.serie_id AS ticker, m.mes, m.retorno_total, ym.r_yf, ym.r_yf - m.retorno_total AS diferenca
    FROM retornos_mensais m JOIN ym ON ym.ticker = m.serie_id AND ym.mes = m.mes
    WHERE m.retorno_total IS NOT NULL AND ym.r_yf IS NOT NULL AND m.mes >= DATE '2011-01-01'
""")
con.sql("""
    SELECT ticker, count(*) AS meses, round(corr(retorno_total, r_yf), 4) AS correlacao,
           round(avg(diferenca), 5) AS diferenca_media, round(max(abs(diferenca)), 4) AS maior_diferenca,
           arg_max(mes, abs(diferenca)) AS mes_maior
    FROM comparacao GROUP BY ticker ORDER BY ticker
""").show(max_rows=20)
print("Meses com |diferença| > 2 p.p. (lista para investigação; nada corrigido)")
con.sql("""SELECT ticker, mes, round(retorno_total, 4) AS retorno_total, round(r_yf, 4) AS retorno_yfinance,
                  round(diferenca, 4) AS diferenca
           FROM comparacao WHERE abs(diferenca) > 0.02 ORDER BY ticker, mes""").show(max_rows=500, max_width=200)
con.close()
