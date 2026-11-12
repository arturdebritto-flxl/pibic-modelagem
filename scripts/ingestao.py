import duckdb

ARQUIVO = 'dados/brutos/COTAHIST_A2010.TXT'

con = duckdb.connect("dados/pibic.duckdb")

con.sql(f"""
    CREATE OR REPLACE TABLE bruto AS
    SELECT linha
    FROM read_csv('{ARQUIVO}',
        columns = {{'linha': 'VARCHAR'}},
        header = false,
        delim = '|',
        quote = '',
        escape = '',
        auto_detect = false)
""")

con.sql("""
    SELECT substr(linha, 1, 2)  AS tipo,
           count(*)             AS n_linhas,
           min(length(linha))   AS menor,
           max(length(linha))   AS maior
        FROM bruto
        GROUP BY tipo
        ORDER BY tipo
""").show()

con.sql("""
    SELECT CAST(substr(linha, 32, 11) AS BIGINT)    AS total_declarado,
            (SELECT count(*) FROM bruto)            AS total_lido
        FROM bruto
        WHERE substr(linha, 1,  2) = '99'
""").show()

con.sql("""
    CREATE OR REPLACE TABLE cotacoes AS
    SELECT
        CAST(strptime(substr(linha, 3, 8), '%Y%m%d') AS DATE) AS data,
        substr(linha, 11, 2)                            AS codbdi,
        trim(substr(linha, 13, 12))                     AS ticker,
        substr(linha, 25, 3)                            AS tpmerc,
        trim(substr(linha, 28, 12))                     AS nome,
        trim(substr(linha, 40, 10))                     AS especi,
        CAST(substr(linha, 57, 13) AS BIGINT) / 100.0   AS abertura,
        CAST(substr(linha, 70, 13) AS BIGINT) / 100.0   AS maximo,
        CAST(substr(linha, 83, 13) AS BIGINT) / 100.0   AS minimo,
        CAST(substr(linha, 96, 13) AS BIGINT) / 100.0   AS medio,
        CAST(substr(linha, 109, 13) AS BIGINT) / 100.0 AS fechamento,
        CAST(substr(linha, 122, 13) AS BIGINT) / 100.0 AS oferta_compra,
        CAST(substr(linha, 135, 13) AS BIGINT) / 100.0 AS oferta_venda,
        CAST(substr(linha, 148, 5)  AS INTEGER)        AS negocios,
        CAST(substr(linha, 153, 18) AS BIGINT)         AS quantidade,
        CAST(substr(linha, 171, 18) AS BIGINT) / 100.0 AS volume,
        CAST(substr(linha, 211, 7)  AS INTEGER)        AS fatcot,
        substr(linha, 231, 12)                         AS isin
    FROM bruto
    WHERE substr(linha, 1, 2) = '01'
""")

con.sql("SELECT count(*) AS n FROM cotacoes").show()
con.sql("SELECT * FROM cotacoes WHERE ticker LIKE 'ABCB4%' ORDER BY data, ticker LIMIT 4").show()