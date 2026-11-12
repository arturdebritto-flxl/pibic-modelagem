import duckdb

con = duckdb.connect("dados/pibic.duckdb")

# Visão temporária sobre os 17 arquivos (não fica gravada no banco)
con.sql("""
    CREATE OR REPLACE TEMP VIEW todas AS
    SELECT filename AS arquivo, linha
    FROM read_csv('dados/brutos/ascii/COTAHIST_A*.TXT',
        columns = {'linha': 'VARCHAR'},
        header = false, delim = '|', quote = '', escape = '',
        auto_detect = false, filename = true)
""")

# Checagem 1: total declarado no rodapé vs. linhas lidas
con.sql("""
    SELECT arquivo,
           count(*) AS lido,
           count(*) FILTER (WHERE substr(linha, 1, 2) = '01') AS cotacoes_01,
           max(CASE WHEN substr(linha, 1, 2) = '99'
                    THEN CAST(substr(linha, 32, 11) AS BIGINT) END) AS declarado,
           CASE WHEN declarado = lido        THEN 'inclui cabecalho/rodape'
                WHEN declarado = cotacoes_01 THEN 'so cotacoes'
                ELSE 'DIVERGENTE' END AS convencao
    FROM todas
    GROUP BY arquivo
    ORDER BY arquivo
""").show(max_rows=50)

# Tabela de cotações: só registros 01 do mercado à vista (D15)
con.sql("""
    CREATE OR REPLACE TABLE cotacoes AS
    SELECT
        CAST(strptime(substr(linha, 3, 8), '%Y%m%d') AS DATE) AS data,
        substr(linha, 11, 2)                           AS codbdi,
        trim(substr(linha, 13, 12))                    AS ticker,
        substr(linha, 25, 3)                           AS tpmerc,
        trim(substr(linha, 28, 12))                    AS nome,
        trim(substr(linha, 40, 10))                    AS especi,
        CAST(substr(linha, 57, 13)  AS BIGINT) / 100.0 AS abertura,
        CAST(substr(linha, 70, 13)  AS BIGINT) / 100.0 AS maximo,
        CAST(substr(linha, 83, 13)  AS BIGINT) / 100.0 AS minimo,
        CAST(substr(linha, 96, 13)  AS BIGINT) / 100.0 AS medio,
        CAST(substr(linha, 109, 13) AS BIGINT) / 100.0 AS fechamento,
        CAST(substr(linha, 122, 13) AS BIGINT) / 100.0 AS oferta_compra,
        CAST(substr(linha, 135, 13) AS BIGINT) / 100.0 AS oferta_venda,
        CAST(substr(linha, 148, 5)  AS INTEGER)        AS negocios,
        CAST(substr(linha, 153, 18) AS BIGINT)         AS quantidade,
        CAST(substr(linha, 171, 18) AS BIGINT) / 100.0 AS volume,
        CAST(substr(linha, 211, 7)  AS INTEGER)        AS fatcot,
        substr(linha, 231, 12)                         AS isin
    FROM todas
    WHERE substr(linha, 1, 2) = '01'
      AND substr(linha, 25, 3) = '010'
""")

# A tabela 'bruto' das versões anteriores não é mais necessária
con.sql("DROP TABLE IF EXISTS bruto")

# Checagem 2: linhas por ano e total
con.sql("""
    SELECT year(data) AS ano, count(*) AS n
    FROM cotacoes
    GROUP BY ano
    ORDER BY ano
""").show(max_rows=50)

con.sql("SELECT count(*) AS total FROM cotacoes").show()