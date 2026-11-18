import duckdb

# Fase 6 (rodada 3) — painel mensal PROVISÓRIO (D33), jan/2011 a out/2026 (D32), sem variáveis explicativas.
# Uma linha por série no universo de cada mês (mensal: universo_mensal_serie; anual: universo_anual_serie).
# Alvos: retorno do mês seguinte (principal, D17) e composto dos 12 meses seguintes (secundário, D17), com as
# sensibilidades da Fase 5 (JCP líquido, sem repetidos, falência −100% e 0%). Alvo que depende de mês depois do
# corte fica NULL com a flag `alvo_fora_do_corte`; série sem mês seguinte (já encerrada), `serie_encerrada`.
# O composto de 12 meses termina na saída, se houver (meses depois da saída não existem em retornos_mensais).
# Flags com prefixo do mês a que se referem: atual:, seguinte:, 12m:.

INICIO, FIM = "2011-01-01", "2026-10-01"
MESES_ESPERADOS, N_SERIES = 190, 100
VARIANTES = {"": "retorno_total", "_preco": "retorno_preco", "_jcp_liquido": "retorno_total_jcp_liquido",
             "_sem_repetidos": "retorno_total_sem_repetidos", "_falencia_100": "retorno_total_falencia_100",
             "_falencia_0": "retorno_total_falencia_0"}

con = duckdb.connect("dados/pibic.duckdb")
con.begin()
# Composição dos 12 meses seguintes por variante: NULL se algum mês da janela é NULL, se a janela passa do corte
# ou se a série não tem o mês seguinte
compostos = ",\n           ".join(
    f"CASE WHEN NOT b.fora AND count(r.mes) > 0 AND count(r.{c}) = count(r.mes) "
    f"THEN exp(sum(ln(1 + r.{c}))) - 1 END AS r12{s}" for s, c in VARIANTES.items())
con.sql(f"""
    CREATE TEMP TABLE janela12 AS
    SELECT b.mes, b.serie_id, b.fora, count(r.mes) AS n_meses,
           (SELECT string_agg(DISTINCT x, ',' ORDER BY x) FROM retornos_mensais q, unnest(string_split(q.flags, ',')) t(x)
             WHERE q.serie_id = b.serie_id AND q.mes > b.mes AND q.mes <= b.mes + INTERVAL 12 MONTH) AS flags12,
           {compostos}
    FROM (SELECT DISTINCT u.mes, u.serie_id, (u.mes + INTERVAL 12 MONTH)::DATE > DATE '{FIM}' AS fora
          FROM (SELECT mes, serie_id FROM universo_mensal_serie UNION SELECT mes, serie_id FROM universo_anual_serie) u
          WHERE u.mes BETWEEN DATE '{INICIO}' AND DATE '{FIM}') b
    LEFT JOIN retornos_mensais r ON r.serie_id = b.serie_id AND r.mes > b.mes AND r.mes <= b.mes + INTERVAL 12 MONTH
    GROUP BY b.mes, b.serie_id, b.fora
""")
con.sql(f"""
    CREATE TEMP TABLE cdi12 AS
    SELECT c.mes, CASE WHEN (c.mes + INTERVAL 12 MONTH)::DATE <= DATE '{FIM}'
                       THEN (SELECT exp(sum(ln(1 + d.retorno))) - 1 FROM cdi_mensal d
                             WHERE d.mes > c.mes AND d.mes <= c.mes + INTERVAL 12 MONTH) END AS cdi_12m_seguinte,
           (SELECT d.retorno FROM cdi_mensal d WHERE d.mes = (c.mes + INTERVAL 1 MONTH)::DATE) AS cdi_mes_seguinte
    FROM cdi_mensal c
""")

for tabela, universo in (("painel_mensal", "universo_mensal_serie"), ("painel_mensal_anual", "universo_anual_serie")):
    seguinte = ",\n           ".join(f"s.{c} AS retorno_mes_seguinte{v}" for v, c in VARIANTES.items())
    doze = ",\n           ".join(f"j.r12{v} AS retorno_12m_seguinte{v}" for v in VARIANTES)
    con.sql(f"""
        CREATE OR REPLACE TABLE {tabela} AS
        SELECT u.mes, u.serie_id, u.ticker_vigente, u.posicao AS posicao_universo,
               (SELECT bool_or(a.eh_unit) FROM acoes a WHERE a.ticker = u.ticker_vigente) AS eh_unit,
               (SELECT bool_or(a.classe_temporaria) FROM acoes a WHERE a.ticker = u.ticker_vigente) AS classe_temporaria,
               a.retorno_total AS retorno_mes_atual,
               {seguinte},
               {doze},
               c.cdi_mes_seguinte, c.cdi_12m_seguinte, coalesce(s.mes_saida, FALSE) AS mes_saida_seguinte,
               nullif(concat_ws(',',
                   CASE WHEN (u.mes + INTERVAL 1 MONTH)::DATE > DATE '{FIM}' THEN 'alvo_fora_do_corte'
                        WHEN s.mes IS NULL THEN 'serie_encerrada' END,
                   CASE WHEN j.fora THEN '12m:alvo_fora_do_corte' END,
                   (SELECT string_agg('atual:' || x, ',' ORDER BY x) FROM unnest(string_split(a.flags, ',')) t(x)),
                   (SELECT string_agg('seguinte:' || x, ',' ORDER BY x) FROM unnest(string_split(s.flags, ',')) t(x)),
                   (SELECT string_agg('12m:' || x, ',' ORDER BY x) FROM unnest(string_split(j.flags12, ',')) t(x))), '')
                   AS flags
        FROM {universo} u
        LEFT JOIN retornos_mensais a ON a.serie_id = u.serie_id AND a.mes = u.mes
        LEFT JOIN retornos_mensais s ON s.serie_id = u.serie_id AND s.mes = (u.mes + INTERVAL 1 MONTH)::DATE
        LEFT JOIN janela12 j ON j.serie_id = u.serie_id AND j.mes = u.mes
        LEFT JOIN cdi12 c ON c.mes = u.mes
        WHERE u.mes BETWEEN DATE '{INICIO}' AND DATE '{FIM}'
        ORDER BY u.mes, u.posicao
    """)

print("PAINEL PROVISÓRIO — saídas de saidas.csv ainda não verificadas (D33); não usar para conclusões.")
print("26/10/2026: queda de mercado pós-eleição (Ibovespa −13,88%); retornos calculados normalmente.\n")
for tabela in ("painel_mensal", "painel_mensal_anual"):
    meses, menor, maior, chaves, ultimo = con.sql(f"""
        SELECT count(DISTINCT mes), min(n), max(n), (SELECT count(*) - count(DISTINCT (mes, serie_id)) FROM {tabela}),
               (SELECT max(mes) FROM {tabela})
        FROM (SELECT mes, count(*) AS n FROM {tabela} GROUP BY mes)
    """).fetchall()[0]
    print(f"{tabela}: {meses} meses; linhas por mês de {menor} a {maior}; chaves repetidas {chaves}; último mês {ultimo}")
    if meses != MESES_ESPERADOS or menor != N_SERIES or maior != N_SERIES or chaves or str(ultimo) > FIM:
        raise ValueError(f"{tabela}: esperado {MESES_ESPERADOS} meses × {N_SERIES} séries até {FIM}, chave única")
    print("Alvos nulos e flags")
    con.sql(f"""
        SELECT count(*) AS linhas,
               count(*) - count(retorno_mes_seguinte) AS mes_seguinte_nulo,
               count(*) - count(retorno_12m_seguinte) AS doze_meses_nulo,
               count(*) - count(retorno_mes_atual) AS mes_atual_nulo,
               count(*) FILTER (WHERE mes_saida_seguinte) AS mes_saida_seguinte,
               count(*) FILTER (WHERE eh_unit) AS units, count(*) FILTER (WHERE classe_temporaria) AS classe_temporaria
        FROM {tabela}
    """).show()
    con.sql(f"""
        SELECT flag, count(*) AS linhas FROM {tabela}, LATERAL (SELECT unnest(string_split(flags, ',')) AS flag)
        WHERE flags IS NOT NULL GROUP BY flag ORDER BY flag
    """).show(max_rows=60)
    print("Alvo do mês seguinte nulo, por motivo (flag do mês seguinte ou da linha)")
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE motivos AS
        SELECT mes, serie_id, coalesce(
                   (SELECT string_agg(x, ',' ORDER BY x) FROM unnest(string_split(flags, ',')) t(x)
                    WHERE x IN ('alvo_fora_do_corte', 'serie_encerrada') OR x LIKE 'seguinte:%'), '(sem flag)') AS motivo
        FROM {tabela} WHERE retorno_mes_seguinte IS NULL
    """)
    con.sql("SELECT motivo, count(*) AS linhas, count(DISTINCT mes) AS meses FROM motivos GROUP BY motivo ORDER BY linhas DESC").show(
        max_width=250)
    print("Por mês (só meses com alvo nulo)")
    con.sql("SELECT mes, count(*) AS nulos, string_agg(DISTINCT motivo, ' | ' ORDER BY motivo) AS motivos FROM motivos "
            "GROUP BY mes ORDER BY mes").show(max_rows=200, max_width=250)
con.commit()
con.close()
