import duckdb

con = duckdb.connect("dados/pibic.duckdb")

TIPOS_ACAO = "('ON', 'PN', 'PNA', 'PNB', 'PNC', 'PND', 'PNE', 'PNF', 'UNT')"

# Tabela de ações (D16)
con.sql(f"""
    CREATE OR REPLACE TABLE acoes AS
    WITH base AS (
        SELECT *,
               replace(split_part(especi, ' ', 1), '*', '') AS tipo,
               trim(substr(especi, 5, 3))                  AS marca
        FROM cotacoes
    )
    SELECT *,
           fechamento / fatcot AS preco_por_acao,
           tipo = 'UNT'        AS eh_unit,
           marca = 'P'         AS classe_temporaria,
           coalesce(fatcot <> lag(fatcot) OVER (PARTITION BY ticker ORDER BY data),
                    false)     AS mudou_fatcot
    FROM base
    WHERE tipo IN {TIPOS_ACAO}
      AND especi NOT LIKE '%REC%'
""")

# Checagem 1: funil — quanto cada filtro removeu
con.sql(f"""
    SELECT
        (SELECT count(*) FROM cotacoes) AS mercado_vista,
        (SELECT count(*) FROM cotacoes
          WHERE replace(split_part(especi, ' ', 1), '*', '') IN {TIPOS_ACAO}) AS tipos_acao,
        (SELECT count(*) FROM acoes) AS sem_recibos
""").show()

# Checagem 2: marcações
con.sql("""
    SELECT count(*)                                   AS linhas,
           count(DISTINCT ticker)                     AS tickers,
           count(*) FILTER (WHERE eh_unit)            AS linhas_unit,
           count(*) FILTER (WHERE classe_temporaria)  AS linhas_temporarias,
           count(*) FILTER (WHERE mudou_fatcot)       AS mudancas_fator,
           count(*) FILTER (WHERE fatcot <> 1)        AS fator_dif_1
    FROM acoes
""").show()

# Checagem 3: uma linha por ação por dia, e preços válidos
con.sql("""
    SELECT
        (SELECT count(*) FROM (
            SELECT ticker, data FROM acoes
            GROUP BY ticker, data
            HAVING count(*) > 1)) AS duplicatas,
        count(*) FILTER (WHERE fechamento <= 0) AS fechamento_zero,
        count(*) FILTER (WHERE quantidade = 0)  AS sem_quantidade
    FROM acoes
""").show()