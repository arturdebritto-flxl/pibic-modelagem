from pathlib import Path

import duckdb

DATA_CORTE = "2026-11-01"
DIAS_SUCESSOR = 30
IPE_DIAS_ANTES = 180
IPE_DIAS_DEPOIS = 60
DIAS_CANCELAMENTO_MAX = 180  # motivo da CVM só vale se o cancelamento vier até N dias após o último pregão
RAZAO_ALERTA_MIN = 0.8
RAZAO_ALERTA_MAX = 1.25
TIPOS = {'troca_codigo', 'conversao_classe', 'incorporacao', 'opa_dinheiro',
         'migracao_exterior', 'falencia', 'reestruturacao', 'indeterminado', 'desmembramento'}
SAIDA = Path("dados/manuais/saidas.csv")
PROPOSTA = Path("resultados/fase1_saidas_proposta.csv")

# Proposta de correspondência motivo de cancelamento (cadastro CVM) -> tipo de saída.
# PENDENTE DE APROVAÇÃO do responsável (tabela completa na checagem 5).
# Motivo fora desta tabela, ou companhia sem cancelamento, fica `indeterminado`.
MOTIVO_CVM_PARA_TIPO = {
    "ELISÃO POR INCORPORAÇÃO": "incorporacao",
    "INCORPORAÇÃO": "incorporacao",
    "ATENDIMENTO AS NORMAS DA INSTRUÇÃO CVM Nº 361/02": "opa_dinheiro",
}

# Os 15 sucessores de resultados/explora_saidas.txt (checagem 2)
SUCESSORES_ESPERADOS = {
    "AEDU11": "AEDU3", "ELPL6": "ELPL4", "PCAR5": "PCAR4", "TCSL4": "TIMP3",
    "TCSL3": "TIMP3", "MPXE3": "ENEV3", "STBP11": "STBP3", "SMLE3": "SMLS3",
    "BVMF3": "B3SA3", "GOLL4": "GOLL54", "MRFG3": "MBRF3", "EMBR3": "EMBJ3",
    "AZUL4": "AZUL53, AZUL54", "ODPV3": "SAUD3", "RAIZ4": "RAIZ55",
}

con = duckdb.connect("dados/pibic.duckdb", read_only=True)

# 1. Resumo de cada ticker (mesma consulta de explora_saidas.py)
con.sql("""
    CREATE TEMP TABLE por_ticker AS
    SELECT ticker,
           min(data)                        AS primeiro_pregao,
           max(data)                        AS ultimo_pregao,
           arg_max(isin, data)              AS isin_final,
           arg_max(nome, data)              AS nome_final,
           arg_max(fechamento, data)        AS ultimo_preco,
           arg_max(classe_temporaria, data) AS classe_temporaria
    FROM acoes
    GROUP BY ticker
""")

# 2. Entrada: tickers do universo mensal que pararam de ser negociados
con.sql(f"""
    CREATE TEMP TABLE pararam AS
    SELECT * FROM por_ticker
    WHERE ticker IN (SELECT ticker FROM universo_mensal)
      AND ultimo_pregao < DATE '{DATA_CORTE}'
""")

# 3. Camada 1 — candidatos a sucessor no COTAHIST:
#    (a) mesma raiz de 4 letras, negociado depois do último pregão (já existente
#        ou iniciado em até DIAS_SUCESSOR dias);
#    (b) ticker novo em até DIAS_SUCESSOR dias com mesmo nome (ou mesmo ISIN)
con.sql(f"""
    CREATE TEMP TABLE candidatos AS
    SELECT p.ticker, n.ticker AS sucessor, n.classe_temporaria,
           left(n.ticker, 4) = left(p.ticker, 4)                          AS regra_a,
           n.primeiro_pregao > p.ultimo_pregao
             AND (n.isin_final = p.isin_final OR n.nome_final = p.nome_final) AS regra_b,
           CASE WHEN n.isin_final = p.isin_final THEN 'isin' ELSE 'nome' END AS criterio_b
    FROM pararam p
    JOIN por_ticker n
      ON n.ticker <> p.ticker
     AND n.ultimo_pregao > p.ultimo_pregao
     AND n.primeiro_pregao <= p.ultimo_pregao + INTERVAL {DIAS_SUCESSOR} DAY
     AND (left(n.ticker, 4) = left(p.ticker, 4)
          OR (n.primeiro_pregao > p.ultimo_pregao
              AND (n.isin_final = p.isin_final OR n.nome_final = p.nome_final)))
""")

# 4. Camada 1 (c) — razão entre o primeiro preço do sucessor depois do último
#    pregão e o último preço do antigo (só evidência; não é relação de troca)
con.sql("""
    CREATE TEMP TABLE razoes AS
    SELECT c.*, arg_min(a.fechamento / p.ultimo_preco, a.data) AS razao_preco
    FROM candidatos c
    JOIN pararam p USING (ticker)
    JOIN acoes a ON a.ticker = c.sucessor AND a.data > p.ultimo_pregao
    GROUP BY ALL
""")

# 5. Camada 1 resumida por ticker
con.sql("""
    CREATE TEMP TABLE camada1 AS
    SELECT ticker,
           string_agg(sucessor, ', ' ORDER BY sucessor) FILTER (WHERE regra_b) AS sucessor_b,
           string_agg(sucessor, ', ' ORDER BY sucessor) FILTER (WHERE regra_a) AS sucessor_a,
           coalesce(bool_or(classe_temporaria) FILTER (WHERE regra_b), false)  AS b_temporaria,
           string_agg(sucessor || ' ('
                      || CASE WHEN regra_a THEN 'a' ELSE '' END
                      || CASE WHEN regra_b THEN 'b:' || criterio_b ELSE '' END
                      || ', razao_preco=' || round(razao_preco, 4) || ')',
                      '; ' ORDER BY sucessor)                                  AS texto
    FROM razoes
    GROUP BY ticker
""")

# 6. Camada 2 — ticker -> CNPJ pelo FCA (ticker só existe no FCA de 2018 em diante)
con.sql("""
    CREATE OR REPLACE TEMP TABLE fca_ticker_cnpj AS
    SELECT DISTINCT ticker, cnpj FROM cvm_fca_valor_mobiliario
    WHERE ticker IN (SELECT ticker FROM por_ticker) AND cnpj IS NOT NULL
""")
ambiguos = con.sql("""
    SELECT ticker, string_agg(cnpj, '; ' ORDER BY cnpj) AS cnpjs
    FROM fca_ticker_cnpj GROUP BY ticker HAVING count(*) > 1
""").fetchall()
if ambiguos:
    raise ValueError(f"Tickers com mais de um CNPJ no FCA: {ambiguos!r}")
con.sql("""
    CREATE TEMP TABLE ticker_cnpj AS
    SELECT ticker, cnpj, 1 AS n_cnpj, 'FCA' AS origem
    FROM fca_ticker_cnpj
    WHERE ticker IN (SELECT ticker FROM pararam)
""")

# 6b. Para quem o FCA não cobre: raiz do ticker -> código CVM (dados cadastrais
#     da B3, scripts/b3_cadastro.py) -> CNPJ no cadastro da CVM
con.sql("""
    INSERT INTO ticker_cnpj
    SELECT DISTINCT p.ticker, c.cnpj, 1, 'B3'
    FROM pararam p
    JOIN b3_emissores b ON b.raiz = left(p.ticker, 4)
    JOIN cvm_cadastro c ON c.codigo_cvm = b.codigo_cvm
    WHERE p.ticker NOT IN (SELECT ticker FROM ticker_cnpj)
""")
ambiguos = con.sql("""
    SELECT ticker, string_agg(cnpj, '; ' ORDER BY cnpj) AS cnpjs
    FROM ticker_cnpj GROUP BY ticker HAVING count(DISTINCT cnpj) > 1
""").fetchall()
if ambiguos:
    raise ValueError(f"Tickers com mais de um CNPJ: {ambiguos!r}")

# D22: candidatos por CNPJ quando a regra por nome/ISIN não encontrou sucessor.
con.sql(f"""
    CREATE OR REPLACE TEMP TABLE candidatos_cnpj AS
    SELECT p.ticker, n.ticker AS sucessor,
           regexp_extract(p.ticker, '[0-9]+') = regexp_extract(n.ticker, '[0-9]+') AS mesma_classe
    FROM pararam p
    JOIN fca_ticker_cnpj fo ON fo.ticker = p.ticker
    JOIN fca_ticker_cnpj fn ON fn.cnpj = fo.cnpj
    JOIN por_ticker n ON n.ticker = fn.ticker
    LEFT JOIN camada1 c1 ON c1.ticker = p.ticker
    WHERE c1.sucessor_b IS NULL
      AND n.primeiro_pregao > p.ultimo_pregao
      AND n.primeiro_pregao <= p.ultimo_pregao + INTERVAL {DIAS_SUCESSOR} DAY
      AND n.nome_final <> p.nome_final
""")
con.sql("""
    CREATE OR REPLACE TEMP TABLE camada_cnpj AS
    SELECT ticker, count(*) FILTER (WHERE mesma_classe) AS mesma_classe,
           count(*) FILTER (WHERE NOT mesma_classe) AS outra_classe,
           string_agg(sucessor, ', ' ORDER BY sucessor) FILTER (WHERE mesma_classe) AS sucessor,
           string_agg(sucessor || CASE WHEN mesma_classe THEN ' (mesma classe)'
                      ELSE ' (classe diferente)' END, '; ' ORDER BY sucessor) AS texto
    FROM candidatos_cnpj GROUP BY ticker
""")

# 7. Camada 2 — CNPJ -> cadastro. O cadastro repete CNPJ (registros antigos e
#    linhas duplicadas): vale o registro mais recente iniciado até o último pregão
con.sql("""
    CREATE TEMP TABLE camada2 AS
    SELECT p.ticker, t.cnpj, t.origem, c.situacao, c.data_cancelamento, c.motivo_cancelamento,
           count(DISTINCT c.codigo_cvm) OVER (PARTITION BY p.ticker) AS registros_cvm
    FROM pararam p
    JOIN ticker_cnpj t USING (ticker)
    LEFT JOIN (SELECT DISTINCT * FROM cvm_cadastro) c
           ON c.cnpj = t.cnpj AND c.data_registro <= p.ultimo_pregao
    QUALIFY row_number() OVER (PARTITION BY p.ticker
                               ORDER BY c.data_registro DESC) = 1
""")

# 8. Camada 3 — fatos relevantes e avisos aos acionistas em torno do último pregão
con.sql(f"""
    CREATE TEMP TABLE camada3 AS
    SELECT DISTINCT p.ticker, p.ultimo_pregao, i.data_entrega, i.categoria, i.tipo,
           i.especie, i.assunto, i.link
    FROM pararam p
    JOIN camada2 c USING (ticker)
    JOIN cvm_ipe i
      ON i.cnpj = c.cnpj
     AND i.categoria IN ('Fato Relevante', 'Aviso aos Acionistas')
     AND i.data_entrega BETWEEN p.ultimo_pregao - INTERVAL {IPE_DIAS_ANTES} DAY
                            AND p.ultimo_pregao + INTERVAL {IPE_DIAS_DEPOIS} DAY
""")

# 9. Tabela de correspondência motivo -> tipo, como tabela para o JOIN
con.sql("CREATE TEMP TABLE motivo_tipo (motivo VARCHAR, tipo VARCHAR)")
con.executemany("INSERT INTO motivo_tipo VALUES (?, ?)", list(MOTIVO_CVM_PARA_TIPO.items()))

# 9b. D20: cancelamento anterior ao último pregão não classifica a saída.
con.sql(f"""
    CREATE TEMP TABLE motivo_valido AS
    SELECT c2.ticker, m.tipo
    FROM camada2 c2
    JOIN pararam p USING (ticker)
    JOIN motivo_tipo m ON m.motivo = c2.motivo_cancelamento
    WHERE c2.data_cancelamento - p.ultimo_pregao BETWEEN 0 AND {DIAS_CANCELAMENTO_MAX}
""")

# 10. Classificação: reestruturacao (sucessor novo é classe temporária "P") >
#     troca_codigo (b) > conversao_classe (a) > motivo CVM (dentro do prazo) > indeterminado.
#     Campos que dependem de documento ficam vazios; verificado = falso em tudo.
con.sql("""
    CREATE TEMP TABLE saidas_base AS
    SELECT p.ticker, p.ultimo_pregao, p.ultimo_preco,
           CASE WHEN c1.b_temporaria            THEN 'reestruturacao'
                WHEN c1.sucessor_b IS NOT NULL  THEN 'troca_codigo'
                WHEN cc.mesma_classe = 1        THEN 'troca_codigo'
                WHEN c1.sucessor_a IS NOT NULL  THEN 'conversao_classe'
                WHEN m.tipo IS NOT NULL         THEN m.tipo
                ELSE 'indeterminado' END                     AS tipo,
           coalesce(c1.sucessor_b, CASE WHEN cc.mesma_classe = 1 THEN cc.sucessor END,
                    c1.sucessor_a)                          AS sucessor,
           CAST(NULL AS DOUBLE)                              AS relacao_troca,
           CAST(NULL AS DOUBLE)                              AS valor_dinheiro,
           CAST(NULL AS DATE)                                AS data_efeito,
           CAST(NULL AS VARCHAR)                             AS fonte,
           concat_ws(' | ',
               'C1: ' || c1.texto,
               'CNPJ candidato: ' || cc.texto,
               'C2: CNPJ ' || c2.cnpj || ' (via ' || c2.origem || '), ' || coalesce(c2.situacao, 'fora do cadastro')
                   || coalesce(' em ' || c2.data_cancelamento, '')
                   || coalesce(' (dias=' || (c2.data_cancelamento - p.ultimo_pregao) || ')', '')
                   || coalesce(', motivo: ' || c2.motivo_cancelamento, ''),
               'C3: ' || c3.n || ' documentos')              AS evidencia_automatica,
           'falso'                                           AS verificado,
           (c1.ticker IS NOT NULL)::INT + (c2.ticker IS NOT NULL)::INT
               + (c3.ticker IS NOT NULL)::INT                AS camadas
    FROM pararam p
    LEFT JOIN camada1 c1 USING (ticker)
    LEFT JOIN camada2 c2 USING (ticker)
    LEFT JOIN camada_cnpj cc USING (ticker)
    LEFT JOIN motivo_valido m USING (ticker)
    LEFT JOIN (SELECT ticker, count(*) AS n FROM camada3 GROUP BY ticker) c3 USING (ticker)
    ORDER BY p.ultimo_pregao, p.ticker
""")

# A razão serve somente para alertar sobre os sucessores efetivamente propostos.
con.sql(f"""
    CREATE OR REPLACE TEMP TABLE alertas AS
    WITH precos AS (
        SELECT s.ticker, a.ticker AS sucessor,
               arg_min(a.fechamento, a.data) / s.ultimo_preco AS razao
        FROM saidas_base s
        JOIN acoes a ON list_contains(string_split(s.sucessor, ', '), a.ticker)
                     AND a.data > s.ultimo_pregao
        GROUP BY s.ticker, a.ticker, s.ultimo_preco
    )
    SELECT ticker, bool_or(razao < {RAZAO_ALERTA_MIN} OR razao > {RAZAO_ALERTA_MAX}) AS extrema,
           string_agg(sucessor || ': ' || round(razao, 4), '; ' ORDER BY sucessor) AS razoes
    FROM precos GROUP BY ticker
""")
con.sql("""
    CREATE OR REPLACE TEMP TABLE saidas AS
    SELECT s.* EXCLUDE (evidencia_automatica, verificado, camadas),
           concat_ws(' | ', s.evidencia_automatica, 'Razões propostas: ' || a.razoes,
                     CASE WHEN a.extrema THEN 'relação de troca não verificada; não presumir 1:1' END)
               AS evidencia_automatica,
           s.verificado,
           concat_ws(';',
               CASE WHEN c2.data_cancelamento < s.ultimo_pregao THEN 'cancelamento_anterior' END,
               CASE WHEN a.extrema THEN 'razao_extrema' END,
               CASE WHEN (SELECT count(*) FROM candidatos c WHERE c.ticker = s.ticker) > 1
                          OR cc.mesma_classe > 1 THEN 'multiplos_sucessores' END,
               CASE WHEN cc.mesma_classe = 1 THEN 'candidato_cnpj' END,
               CASE WHEN cc.outra_classe > 0 THEN 'classe_diferente' END) AS alerta,
           s.camadas
    FROM saidas_base s
    LEFT JOIN camada2 c2 USING (ticker)
    LEFT JOIN camada_cnpj cc USING (ticker)
    LEFT JOIN alertas a USING (ticker)
    ORDER BY s.ultimo_pregao, s.ticker
""")

# Checagem 1: 115 linhas, ticker único, tipo nunca nulo, nada verificado
linhas, tickers, tipo_nulo, verificados = con.sql("""
    SELECT count(*), count(DISTINCT ticker), count(*) FILTER (WHERE tipo IS NULL),
           count(*) FILTER (WHERE verificado <> 'falso')
    FROM saidas
""").fetchall()[0]
print(f"\nChecagem 1: linhas={linhas} tickers={tickers} tipo_nulo={tipo_nulo} verificados={verificados}")
assert (linhas, tickers, tipo_nulo, verificados) == (115, 115, 0, 0)

# Checagem 2: os 15 sucessores da exploração aparecem iguais
obtidos = dict(con.sql("SELECT ticker, sucessor_b FROM camada1 WHERE sucessor_b IS NOT NULL").fetchall())
print("Checagem 2: sucessores por nome/ISIN iguais aos 15 da exploração:",
      "OK" if obtidos == SUCESSORES_ESPERADOS else f"DIVERGENTE {obtidos}")
assert obtidos == SUCESSORES_ESPERADOS
assert set(r[0] for r in con.sql("SELECT DISTINCT tipo FROM saidas").fetchall()) <= TIPOS
assert con.sql("SELECT count(*) FROM saidas WHERE ultimo_preco IS NULL OR ultimo_preco <= 0 "
               "OR ultimo_pregao IS NULL OR relacao_troca IS NOT NULL OR valor_dinheiro IS NOT NULL "
               "OR data_efeito IS NOT NULL OR fonte IS NOT NULL").fetchall()[0][0] == 0

print("\nChecagem 3: contagem por tipo")
con.sql("SELECT tipo, count(*) AS n FROM saidas GROUP BY tipo ORDER BY n DESC").show()

print("Checagem 4: contagem por número de camadas com evidência")
con.sql("""
    SELECT camadas, count(*) AS n,
           count(*) FILTER (WHERE tipo = 'indeterminado') AS indeterminados
    FROM saidas GROUP BY camadas ORDER BY camadas
""").show()

print("Checagem 5: tabela MOTIVO_CVM_PARA_TIPO — cada (situação, motivo) do cadastro "
      "entre os tickers com CNPJ, e o tipo proposto (PARA APROVAÇÃO)")
con.sql("""
    SELECT c2.situacao, c2.motivo_cancelamento AS motivo, count(*) AS tickers,
           coalesce(m.tipo, 'indeterminado') AS tipo_proposto,
           count(*) FILTER (WHERE s.tipo = m.tipo) AS decididos_pelo_motivo,
           string_agg(c2.ticker, ' ' ORDER BY c2.ticker) AS quais
    FROM camada2 c2
    JOIN saidas s USING (ticker)
    LEFT JOIN motivo_tipo m ON m.motivo = c2.motivo_cancelamento
    GROUP BY ALL ORDER BY tickers DESC
""").show(max_rows=50, max_width=220)

print(f"Checagem 6: distância entre o último pregão e o cancelamento na CVM (dias); "
      f"acima de {DIAS_CANCELAMENTO_MAX} o motivo NÃO é usado; negativos = cancelamento antes do último pregão")
con.sql(f"""
    SELECT s.ticker, s.ultimo_pregao, s.tipo, s.sucessor, c2.data_cancelamento,
           c2.data_cancelamento - s.ultimo_pregao AS dias,
           CASE WHEN c2.data_cancelamento - s.ultimo_pregao > {DIAS_CANCELAMENTO_MAX}
                THEN 'FORA DO PRAZO'
                WHEN c2.data_cancelamento < s.ultimo_pregao THEN 'antes do último pregão'
                ELSE '' END AS observacao,
           c2.motivo_cancelamento
    FROM saidas s JOIN camada2 c2 USING (ticker)
    WHERE c2.data_cancelamento IS NOT NULL
    ORDER BY s.ultimo_pregao
""").show(max_rows=120, max_width=220)

print("Checagem 7: conflitos — Camada 1 aponta sucessor e a CVM registra cancelamento "
      "em até 1 ano do último pregão")
con.sql("""
    SELECT s.ticker, s.ultimo_pregao, s.tipo, s.sucessor, c2.data_cancelamento,
           c2.motivo_cancelamento
    FROM saidas s JOIN camada2 c2 USING (ticker)
    WHERE s.sucessor IS NOT NULL
      AND c2.data_cancelamento BETWEEN s.ultimo_pregao - INTERVAL 1 YEAR
                                   AND s.ultimo_pregao + INTERVAL 1 YEAR
""").show(max_rows=120, max_width=220)

print("Checagem 8: tickers com sucessor pelas duas regras, (a) e (b), ou com mais de um candidato")
con.sql("""
    SELECT ticker, sucessor_a, sucessor_b FROM camada1
    WHERE (sucessor_a IS NOT NULL AND sucessor_b IS NOT NULL)
       OR sucessor_a LIKE '%,%' OR sucessor_b LIKE '%,%'
    ORDER BY ticker
""").show(max_rows=120)

print("Checagem 9: origem do CNPJ")
con.sql("""
    SELECT coalesce(t.origem, 'sem CNPJ') AS origem, count(*) AS tickers
    FROM pararam p LEFT JOIN ticker_cnpj t USING (ticker)
    GROUP BY ALL ORDER BY tickers DESC
""").show()

print("Checagem 9b: tickers sem CNPJ (nem FCA nem B3; sem Camadas 2 e 3), por ano do último pregão")
con.sql("""
    SELECT year(ultimo_pregao) AS ano, count(*) AS tickers,
           count(*) FILTER (WHERE ticker NOT IN (SELECT ticker FROM ticker_cnpj)) AS sem_cnpj,
           string_agg(ticker, ' ' ORDER BY ticker)
               FILTER (WHERE ticker NOT IN (SELECT ticker FROM ticker_cnpj)) AS quais
    FROM pararam GROUP BY ano ORDER BY ano
""").show(max_rows=30, max_width=220)

print("Checagem 10: tickers com mais de um CNPJ no FCA ou mais de um registro CVM candidato")
con.sql("""
    SELECT c2.ticker, t.n_cnpj, c2.registros_cvm
    FROM camada2 c2 JOIN ticker_cnpj t USING (ticker)
    WHERE t.n_cnpj > 1 OR c2.registros_cvm > 1
""").show()

print("Checagem 11: documentos do IPE por ticker (resumo) — lista em resultados/fase1_ipe_documentos.csv")
con.sql("""
    SELECT count(*) AS documentos, count(DISTINCT ticker) AS tickers,
           count(*) FILTER (WHERE categoria = 'Fato Relevante') AS fatos_relevantes,
           count(*) FILTER (WHERE categoria = 'Aviso aos Acionistas') AS avisos
    FROM camada3
""").show()

print("Tabela final")
con.sql("SELECT * EXCLUDE (relacao_troca, valor_dinheiro, data_efeito, fonte) FROM saidas").show(
    max_rows=200, max_width=250)

print("Camada por CNPJ (D22)")
con.sql("SELECT * FROM camada_cnpj ORDER BY ticker").show(max_rows=200)
print("Contagem por alerta (proposta automática)")
con.sql("SELECT codigo, count(*) AS n FROM saidas, "
        "unnest(string_split(alerta, ';')) AS t(codigo) "
        "WHERE alerta <> '' GROUP BY codigo ORDER BY codigo").show()

# Gravar somente depois das checagens; o CSV manual existente permanece protegido.
destino = PROPOSTA if SAIDA.exists() else SAIDA
destino.parent.mkdir(parents=True, exist_ok=True)
con.sql(f"COPY (SELECT * EXCLUDE (camadas) FROM saidas) TO '{destino}' (HEADER, DELIMITER ',')")
print("gravado:", destino)
if destino == PROPOSTA:
    print("CSV manual preservado; diferença BBTG11 esperada após a aplicação de D21:")
    con.sql(f"""
        SELECT ticker, m.tipo AS tipo_manual, s.tipo AS tipo_proposto,
               m.sucessor AS sucessor_manual, s.sucessor AS sucessor_proposto
        FROM read_csv('{SAIDA}', all_varchar = true) m
        FULL JOIN saidas s USING (ticker)
        WHERE m.tipo IS DISTINCT FROM s.tipo OR m.sucessor IS DISTINCT FROM s.sucessor
    """).show(max_rows=200)
con.sql("COPY (SELECT * FROM camada3 ORDER BY ticker, data_entrega, link) "
        "TO 'resultados/fase1_ipe_documentos.csv' (HEADER, DELIMITER ',')")

con.close()
