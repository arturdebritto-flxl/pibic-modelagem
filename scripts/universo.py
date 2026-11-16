import duckdb

con = duckdb.connect("dados/pibic.duckdb")

N_ACOES = 100
PRESENCA_MIN = 0.80
JANELA_MESES = 12

# 1. Liquidez de cada ação em cada mês, olhando só os 12 meses anteriores
con.sql(f"""
    CREATE OR REPLACE TABLE liquidez AS
    WITH meses AS (
        SELECT DISTINCT date_trunc('month', data) AS mes FROM acoes
    ),
    pregoes AS (
        SELECT DISTINCT data FROM acoes
    ),
    dias_mercado AS (
        SELECT m.mes, count(*) AS dias_mercado
        FROM meses m
        JOIN pregoes p
          ON p.data >= m.mes - INTERVAL {JANELA_MESES} MONTH
         AND p.data <  m.mes
        GROUP BY m.mes
    ),
    por_acao AS (
        SELECT m.mes, a.ticker,
               median(a.volume) AS volume_mediano,
               count(*)         AS dias_negociados
        FROM meses m
        JOIN acoes a
          ON a.data >= m.mes - INTERVAL {JANELA_MESES} MONTH
         AND a.data <  m.mes
        GROUP BY m.mes, a.ticker
    )
    SELECT p.mes, p.ticker, p.volume_mediano, p.dias_negociados, d.dias_mercado,
           p.dias_negociados / d.dias_mercado AS presenca
    FROM por_acao p
    JOIN dias_mercado d USING (mes)
""")

# 2. Universo mensal: as N mais líquidas com presença mínima
con.sql(f"""
    CREATE OR REPLACE TABLE universo_mensal AS
    SELECT mes, ticker, volume_mediano, presenca,
           row_number() OVER (PARTITION BY mes
                              ORDER BY volume_mediano DESC, ticker) AS posicao
    FROM liquidez
    WHERE presenca >= {PRESENCA_MIN}
      AND mes >= date_trunc('month', (SELECT min(data) FROM acoes))
                 + INTERVAL {JANELA_MESES} MONTH
    QUALIFY posicao <= {N_ACOES}
""")

# 3. Universo anual: o universo de janeiro vale para o ano inteiro
con.sql("""
    CREATE OR REPLACE TABLE universo_anual AS
    SELECT m.mes, u.ticker, u.posicao
    FROM (SELECT DISTINCT mes FROM universo_mensal) m
    JOIN universo_mensal u
      ON u.mes = date_trunc('year', m.mes)
""")

# Checagem 1: tamanho do universo mês a mês
con.sql("""
    SELECT count(*) AS meses, min(mes) AS primeiro, max(mes) AS ultimo,
           min(n) AS menor_n, max(n) AS maior_n
    FROM (SELECT mes, count(*) AS n FROM universo_mensal GROUP BY mes)
""").show()

# Checagem 2: quantas ações diferentes passam pelo universo
con.sql("""
    SELECT 'mensal' AS versao, count(DISTINCT ticker) AS tickers FROM universo_mensal
    UNION ALL
    SELECT 'anual', count(DISTINCT ticker) FROM universo_anual
""").show()

# Checagem 3: ações do universo que pararam de ser negociadas antes do fim dos dados
con.sql("""
    WITH ultimo AS (
        SELECT ticker, max(data) AS ultimo_pregao FROM acoes GROUP BY ticker
    )
    SELECT count(*) AS pararam_de_negociar
    FROM ultimo
    WHERE ticker IN (SELECT ticker FROM universo_mensal)
      AND ultimo_pregao < DATE '2026-11-01'
""").show()

# Checagem 4: rotatividade — quantas ações entram no universo a cada mês
con.sql("""
    WITH entradas AS (
        SELECT u.mes, count(*) AS n
        FROM universo_mensal u
        LEFT JOIN universo_mensal ant
               ON ant.ticker = u.ticker
              AND ant.mes = u.mes - INTERVAL 1 MONTH
        WHERE ant.ticker IS NULL
          AND u.mes > (SELECT min(mes) FROM universo_mensal)
        GROUP BY u.mes
    )
    SELECT round(avg(n), 1) AS media_entradas, max(n) AS maximo_entradas
    FROM entradas
""").show()