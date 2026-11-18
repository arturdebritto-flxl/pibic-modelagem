import csv
import re
from pathlib import Path

import duckdb

# Fase 4 (rodada 3) — ligações entre tickers, séries contínuas e universo por série.
# Regras: ligação só de `troca_codigo`, `conversao_classe` e `reestruturacao` de dados/manuais/saidas.csv, e só
# usada com verificado = verdadeiro, fonte e relacao_troca preenchidas e um único sucessor (D18, D22);
# `desmembramento` não liga (D21). Cadeias (A → B → C) viram uma série; ticker sem ligação usada = série própria.
# Universo por série com as regras da D17 (mesma consulta de universo.py, agrupada por série; volume do dia da
# série = soma dos tickers da série). Tabelas por ticker (`liquidez`, `universo_mensal`, `universo_anual`) não mudam.
# Rodar de novo depois da verificação das saídas atualiza tudo sem mudar código.

SAIDAS = Path("dados/manuais/saidas.csv")
TIPOS_LIGACAO = ("troca_codigo", "conversao_classe", "reestruturacao")
N_ACOES = 100
PRESENCA_MIN = 0.80
JANELA_MESES = 12
MESES_ESPERADOS = 191


def numero_ou_none(texto):
    texto = (texto or "").strip().replace(",", ".")
    return float(texto) if texto else None


# 1. Ligações propostas em saidas.csv (uma linha por sucessor)
ligacoes = []
with open(SAIDAS, newline="") as arq:
    for r in csv.DictReader(arq):
        if r["tipo"] not in TIPOS_LIGACAO:
            continue
        sucessores = [t.strip() for t in re.split(r"[+,]", r["sucessor"]) if t.strip()]
        verificado = r["verificado"].strip().lower() == "verdadeiro"
        relacao = numero_ou_none(r["relacao_troca"])
        fonte = r["fonte"].strip() or None
        for sucessor in sucessores or [None]:
            motivo = ("sem sucessor" if sucessor is None else "mais de um sucessor" if len(sucessores) > 1
                      else "não verificada" if not verificado else "sem fonte" if not fonte
                      else "sem relação de troca" if relacao is None else None)
            ligacoes.append((r["ticker"], sucessor, r["data_efeito"] or None, relacao, r["tipo"], fonte, verificado,
                             motivo is None, motivo))

with duckdb.connect("dados/pibic.duckdb") as con:
    con.begin()
    con.sql("""CREATE OR REPLACE TABLE ligacoes (ticker_antigo VARCHAR, ticker_novo VARCHAR, data_efeito DATE,
               relacao_troca DOUBLE, tipo VARCHAR, fonte VARCHAR, verificado BOOLEAN, usada BOOLEAN, motivo_nao_usada VARCHAR)""")
    con.executemany("INSERT INTO ligacoes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ligacoes)
    print("Fase 4 — ligações propostas em saidas.csv (tipos troca_codigo, conversao_classe, reestruturacao)")
    con.sql("""SELECT tipo, count(*) AS ligacoes, count(*) FILTER (WHERE usada) AS usadas,
                      count(*) FILTER (WHERE NOT usada) AS nao_usadas FROM ligacoes GROUP BY tipo ORDER BY tipo""").show()
    con.sql("SELECT motivo_nao_usada, count(*) AS ligacoes FROM ligacoes WHERE NOT usada GROUP BY ALL ORDER BY ALL").show()
    print("Ligações usadas")
    con.sql("SELECT * FROM ligacoes WHERE usada ORDER BY ticker_antigo").show(max_width=200)
    ruins = con.sql("""SELECT count(*) FROM ligacoes WHERE usada AND (ticker_novo NOT IN (SELECT ticker FROM acoes)
                       OR ticker_antigo NOT IN (SELECT ticker FROM acoes) OR relacao_troca <= 0)""").fetchall()[0][0]
    if ruins:
        raise ValueError("ligação usada com ticker fora de `acoes` ou relação de troca <= 0")

    # 2. Séries: componentes conexos das ligações usadas (fecho transitivo); série = ticker mais antigo da cadeia
    usadas = con.sql("SELECT ticker_antigo, ticker_novo FROM ligacoes WHERE usada").fetchall()
    pai = {}

    def raiz(t):
        while pai.get(t, t) != t:
            t = pai[t]
        return t

    for a, b in usadas:
        pai[raiz(b)] = raiz(a)
    con.sql("CREATE TEMP TABLE grupo (ticker VARCHAR, grupo VARCHAR)")
    tickers = [t for (t,) in con.sql("SELECT DISTINCT ticker FROM acoes ORDER BY ticker").fetchall()]
    con.executemany("INSERT INTO grupo VALUES (?, ?)", [(t, raiz(t)) for t in tickers])
    con.sql("""
        CREATE OR REPLACE TABLE series AS
        WITH periodo AS (SELECT ticker, min(data) AS data_inicio, max(data) AS data_fim FROM acoes GROUP BY ticker)
        SELECT first(p.ticker ORDER BY p.data_inicio, p.ticker) OVER (PARTITION BY g.grupo) AS serie_id,
               p.ticker, p.data_inicio, p.data_fim
        FROM periodo p JOIN grupo g USING (ticker)
        ORDER BY serie_id, data_inicio
    """)
    n, unicos, series_n, multi = con.sql("""
        SELECT count(*), count(DISTINCT ticker), count(DISTINCT serie_id),
               (SELECT count(*) FROM (SELECT serie_id FROM series GROUP BY serie_id HAVING count(*) > 1)) FROM series
    """).fetchall()[0]
    print(f"Séries: {series_n} séries para {unicos} tickers ({n} linhas); séries com mais de um ticker: {multi}")
    if n != unicos or unicos != len(tickers):
        raise ValueError("series: ticker repetido ou ausente")
    sobrepostos = con.sql("""
        SELECT a.serie_id, a.ticker, b.ticker AS outro, greatest(a.data_inicio, b.data_inicio) AS de,
               least(a.data_fim, b.data_fim) AS ate
        FROM series a JOIN series b ON a.serie_id = b.serie_id AND a.ticker < b.ticker
        WHERE a.data_inicio <= b.data_fim AND b.data_inicio <= a.data_fim
    """)
    print("Tickers da mesma série negociados ao mesmo tempo (volume somado nesses dias):", len(sobrepostos.fetchall()))
    sobrepostos.show(max_width=200)

    # 3. Liquidez e universo por série (D17): volume do dia da série = soma dos volumes dos seus tickers
    con.sql(f"""
        CREATE OR REPLACE TABLE liquidez_serie AS
        WITH vol AS (SELECT s.serie_id, a.data, sum(a.volume) AS volume FROM acoes a JOIN series s USING (ticker)
                     GROUP BY s.serie_id, a.data),
        meses AS (SELECT DISTINCT date_trunc('month', data)::DATE AS mes FROM acoes),
        pregoes AS (SELECT DISTINCT data FROM acoes),
        dias_mercado AS (
            SELECT m.mes, count(*) AS dias_mercado FROM meses m
            JOIN pregoes p ON p.data >= m.mes - INTERVAL {JANELA_MESES} MONTH AND p.data < m.mes
            GROUP BY m.mes),
        por_serie AS (
            SELECT m.mes, v.serie_id, median(v.volume) AS volume_mediano, count(*) AS dias_negociados
            FROM meses m JOIN vol v ON v.data >= m.mes - INTERVAL {JANELA_MESES} MONTH AND v.data < m.mes
            GROUP BY m.mes, v.serie_id)
        SELECT p.mes, p.serie_id, p.volume_mediano, p.dias_negociados, d.dias_mercado,
               p.dias_negociados / d.dias_mercado AS presenca
        FROM por_serie p JOIN dias_mercado d USING (mes)
    """)
    # Ticker vigente: o da série com o último pregão antes do início do mês
    con.sql(f"""
        CREATE OR REPLACE TABLE universo_mensal_serie AS
        SELECT l.mes, l.serie_id,
               (SELECT a.ticker FROM acoes a JOIN series s USING (ticker)
                 WHERE s.serie_id = l.serie_id AND a.data < l.mes ORDER BY a.data DESC, a.ticker LIMIT 1) AS ticker_vigente,
               l.volume_mediano, l.presenca,
               row_number() OVER (PARTITION BY l.mes ORDER BY l.volume_mediano DESC, l.serie_id) AS posicao
        FROM liquidez_serie l
        WHERE l.presenca >= {PRESENCA_MIN}
          AND l.mes >= date_trunc('month', (SELECT min(data) FROM acoes)) + INTERVAL {JANELA_MESES} MONTH
        QUALIFY posicao <= {N_ACOES}
    """)
    con.sql("""
        CREATE OR REPLACE TABLE universo_anual_serie AS
        SELECT m.mes, u.serie_id, u.ticker_vigente, u.posicao
        FROM (SELECT DISTINCT mes FROM universo_mensal_serie) m
        JOIN universo_mensal_serie u ON u.mes = date_trunc('year', m.mes)
    """)

    # Checagens
    meses, menor, maior = con.sql("""SELECT count(*), min(n), max(n)
        FROM (SELECT mes, count(*) AS n FROM universo_mensal_serie GROUP BY mes)""").fetchall()[0]
    chaves = con.sql("SELECT count(*) - count(DISTINCT (mes, serie_id)) FROM universo_mensal_serie").fetchall()[0][0]
    print(f"\nUniverso por série: {meses} meses; séries por mês de {menor} a {maior}; chaves repetidas: {chaves}")
    if meses != MESES_ESPERADOS or menor != N_ACOES or maior != N_ACOES or chaves:
        raise ValueError(f"universo_mensal_serie: esperado {MESES_ESPERADOS} meses × {N_ACOES} séries, chave única")
    con.sql("""
        SELECT 'mensal' AS versao, count(DISTINCT serie_id) AS series, count(*) AS linhas FROM universo_mensal_serie
        UNION ALL SELECT 'anual', count(DISTINCT serie_id), count(*) FROM universo_anual_serie
    """).show()
    print("Diferença entre o universo por ticker e por série (série do ticker no universo por ticker)")
    con.sql("""
        WITH t AS (SELECT u.mes, s.serie_id FROM universo_mensal u JOIN series s USING (ticker)),
             d AS (SELECT mes, serie_id, 'só por ticker' AS lado FROM (SELECT mes, serie_id FROM t
                       EXCEPT SELECT mes, serie_id FROM universo_mensal_serie)
                   UNION ALL
                   SELECT mes, serie_id, 'só por série' FROM (SELECT mes, serie_id FROM universo_mensal_serie
                       EXCEPT SELECT mes, serie_id FROM t))
        SELECT lado, count(*) AS meses_serie, count(DISTINCT mes) AS meses, count(DISTINCT serie_id) AS series
        FROM d GROUP BY lado ORDER BY lado
    """).show()
    b3 = con.sql("""SELECT count(*) FROM ligacoes WHERE usada AND ticker_antigo = 'BVMF3' AND ticker_novo = 'B3SA3'""").fetchall()[0][0]
    if b3:
        meses_b3 = con.sql("""
            SELECT count(DISTINCT u.mes) FROM universo_mensal_serie u JOIN series s ON s.serie_id = u.serie_id
            WHERE s.ticker = 'B3SA3' AND u.mes BETWEEN DATE '2018-03-01' AND DATE '2018-12-01'""").fetchall()[0][0]
        print(f"Checagem BVMF3/B3SA3: mesma série; meses no universo entre mar e dez/2018: {meses_b3} de 10")
        if meses_b3 != 10:
            raise ValueError("BVMF3/B3SA3: série ausente do universo em 2018")
    else:
        print("Checagem BVMF3/B3SA3: pendente (ligação não verificada; a série B3SA3 fica separada de BVMF3)")
    con.commit()
