import base64
import csv
import hashlib
import html
import json
import re
import tempfile
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

import duckdb

# Fase 3 (proventos) — proventos em dinheiro da B3 (D19) para todo o universo,
# tabela `proventos_b3`: uma linha por registro devolvido pela B3, sem remover
# nem somar nada. Condições da aprovação do Portão 3a (16/11/2026):
#   1. coluna `pareamento` com a marca do COTAHIST na data ex inferida;
#   2. listas de pendências (marca sem provento; empresa sem registros);
#   3. nome que devolve zero: tentar sem "/" e sem pontuação, registrando a variante;
#   4. taxa de coincidência por empresa e por nome consultado, com as 20 piores;
#   5. registros idênticos só marcados e contados;
#   6. unit só pareia com o registro UNT.
# Classe: o par é feito só quando a classe da B3 é igual ao `tipo` do COTAHIST.
# Marca: o início é contado por letra (EDJ → ED não é novo início de D).

ANO_MIN, ANO_MAX = 2010, 2026
JANELA_PREGOES = 5
PAUSA_S = 3
PIORES = 20
AGENTE = "Mozilla/5.0 (X11; Linux x86_64) pesquisa-academica-PIBIC-UnB"
B3 = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall"
PASTA = Path("dados/brutos/b3_proventos")
PENDENCIAS = Path("resultados/fase3_pendencias_marcas.csv")
# Consultas já baixadas na Fase 2 e no Portão 3a (arquivos nomeados pela raiz do ticker)
JA_BAIXADOS = {
    "PETROBRAS": "dados/brutos/b3_teste/proventos_PETR", "VALE": "dados/brutos/b3_teste/proventos_VALE",
    "CIELO": "dados/brutos/b3_teste/proventos_CIEL", "ENERGIAS BR": "dados/brutos/b3_teste/proventos_ENBR",
    "SOUZA CRUZ": "dados/brutos/b3_teste/proventos_CRUZ",
    "ITAUUNIBANCO": "dados/brutos/b3_proventos/proventos_ITUB", "BRADESCO": "dados/brutos/b3_proventos/proventos_BBDC",
    "AMBEV S/A": "dados/brutos/b3_proventos/proventos_ABEV", "AMBEV SA": "dados/brutos/b3_proventos/proventos_ABEV_alt2",
    "WEG": "dados/brutos/b3_proventos/proventos_WEGE", "TAESA": "dados/brutos/b3_proventos/proventos_TAEE",
}
# Comparação dos registros repetidos com as fontes de conferência da Fase 2: ticker -> (nome, classe)
COMPARAR = {"PETR4": ("PETROBRAS", "PN"), "VALE3": ("VALE", "ON"), "WEGE3": ("WEG", "ON")}
SUFIXO_CLASSE = {"3": "ON", "4": "PN", "5": "PNA", "6": "PNB", "11": "UNT"}


def baixa(url, destino):
    """Baixa uma vez (bruto nunca é sobrescrito) e registra o hash."""
    if not destino.exists():
        pedido = urllib.request.Request(url, headers={"User-Agent": AGENTE})
        with urllib.request.urlopen(pedido, timeout=60) as resposta:
            conteudo = resposta.read()
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(conteudo)
        with open("dados/hashes.txt", "a") as hashes:
            hashes.write(f"{hashlib.sha256(conteudo).hexdigest()}  {destino}\n")
        time.sleep(PAUSA_S)


def data_iso(texto):
    """DD/MM/AAAA -> AAAA-MM-DD; vazio se a B3 não informou."""
    return f"{texto[6:10]}-{texto[3:5]}-{texto[:2]}" if texto and len(texto) >= 10 else ""


# 1. Nomes de pregão que os tickers do universo tiveram no COTAHIST
with duckdb.connect("dados/pibic.duckdb", read_only=True) as con:
    nomes = [r[0] for r in con.sql("""
        SELECT DISTINCT nome FROM acoes
        WHERE ticker IN (SELECT ticker FROM universo_mensal) ORDER BY nome
    """).fetchall()]
print(len(nomes), "nomes de pregão a consultar")

# 2. Download: todas as páginas de cada nome. Nome que devolve zero: tenta sem "/"
#    e depois sem pontuação. A variante usada fica em cada registro
consultas, registros = [], []
for nome in nomes:
    variantes = list(dict.fromkeys([nome, nome.replace("/", ""), re.sub(r"[^A-Z0-9 ]", "", nome)]))
    for tentativa, consultado in enumerate(variantes, 1):
        prefixo = JA_BAIXADOS.get(consultado) or str(PASTA / f"proventos_{urllib.parse.quote(consultado, safe='')}")
        pagina, paginas, lidos = 1, 1, 0
        while pagina <= paginas:
            arquivo = Path(f"{prefixo}_p{pagina}.json")
            parametros = json.dumps({"language": "pt-br", "pageNumber": pagina, "pageSize": 120,
                                     "tradingName": consultado}, separators=(",", ":"))
            baixa(f"{B3}/GetListedCashDividends/{base64.b64encode(parametros.encode()).decode()}", arquivo)
            # resposta de 0 bytes = nenhum registro
            dados = json.loads(arquivo.read_text() or '{"page": {"totalPages": 0, "totalRecords": 0}, "results": []}')
            paginas = dados["page"]["totalPages"]
            for r in dados["results"]:
                registros.append([nome, consultado, arquivo.name, (r.get("typeStock") or "").strip(),
                                  (r.get("corporateAction") or "").strip(), data_iso(r.get("dateApproval")),
                                  data_iso(r.get("lastDatePriorEx")),
                                  (r.get("valueCash") or "").replace(".", "").replace(",", "."),
                                  json.dumps(r, sort_keys=True)])
                lidos += 1
            pagina += 1
        if lidos != dados["page"]["totalRecords"]:
            raise ValueError(f"{consultado}: {lidos} registros lidos, {dados['page']['totalRecords']} declarados")
        consultas.append((nome, consultado, tentativa, lidos))
        if lidos:
            break

# Conferência anual da WEGE3 (condição 5): baixada antes de abrir a conexão de escrita
baixa("https://www.dadosdemercado.com.br/acoes/wege3/dividendos",
      Path("dados/brutos/dadosdemercado_teste/dividendos_WEGE3.html"))

# Registros idênticos dentro da mesma consulta (todos os campos iguais): marcados, não removidos
vistos, total = Counter(), Counter((r[1], r[8]) for r in registros)
tmp = Path(tempfile.mkdtemp())
with open(tmp / "registros.csv", "w", newline="") as arq:
    escritor = csv.writer(arq)
    for i, r in enumerate(registros, 1):
        vistos[(r[1], r[8])] += 1
        escritor.writerow([i] + r[:8] + [total[(r[1], r[8])] > 1, vistos[(r[1], r[8])]])

con = duckdb.connect("dados/pibic.duckdb")
con.begin()
con.sql("CREATE TEMP TABLE consultas (nome_cotahist VARCHAR, nome_consultado VARCHAR, tentativa INTEGER, registros INTEGER)")
con.executemany("INSERT INTO consultas VALUES (?, ?, ?, ?)", consultas)
con.sql(f"""
    CREATE TEMP TABLE bruto AS
    SELECT * FROM read_csv('{tmp}/registros.csv', header = false, columns = {{
        'id_registro': 'INTEGER', 'nome_cotahist': 'VARCHAR', 'nome_consultado': 'VARCHAR', 'arquivo': 'VARCHAR',
        'classe_b3': 'VARCHAR', 'tipo_b3': 'VARCHAR', 'data_aprovacao': 'DATE', 'data_com': 'DATE',
        'valor': 'DOUBLE', 'repetido': 'BOOLEAN', 'repeticao': 'INTEGER'}})
""")

print("\nConsultas: nomes resolvidos por variante e nomes sem nenhum registro")
con.sql("""
    SELECT count(DISTINCT nome_cotahist) AS nomes,
           count(*) FILTER (WHERE registros > 0 AND tentativa = 1) AS com_o_nome_do_cotahist,
           count(*) FILTER (WHERE registros > 0 AND tentativa > 1) AS com_variante,
           count(DISTINCT nome_cotahist) - count(*) FILTER (WHERE registros > 0) AS sem_registros
    FROM consultas
""").show()
con.sql("""
    SELECT nome_cotahist, string_agg(nome_consultado || ' -> ' || registros, ' | ' ORDER BY tentativa) AS tentativas
    FROM consultas GROUP BY nome_cotahist HAVING count(*) > 1 OR max(registros) = 0 ORDER BY nome_cotahist
""").show(max_rows=300, max_width=200)

# 3. Calendário da bolsa: todos os pregões do mercado à vista, numerados
con.sql("""
    CREATE TEMP TABLE pregoes AS
    SELECT data, row_number() OVER (ORDER BY data) AS n
    FROM (SELECT DISTINCT data FROM cotacoes)
""")

# 4. Data ex inferida = primeiro pregão da bolsa depois da última data "com" (D19).
#    Só para data com no período da base: antes de 2010 o "primeiro pregão depois" seria 04/01/2010
con.sql(f"""
    CREATE TEMP TABLE registros AS
    SELECT b.*,
           CASE b.tipo_b3 WHEN 'DIVIDENDO' THEN 'dividendo' WHEN 'JRS CAP PROPRIO' THEN 'jcp'
                ELSE 'outro' END                                   AS tipo_provento,
           CASE b.tipo_b3 WHEN 'DIVIDENDO' THEN 'D' WHEN 'JRS CAP PROPRIO' THEN 'J' END AS letra,
           CASE WHEN year(b.data_com) BETWEEN {ANO_MIN} AND {ANO_MAX}
                THEN (SELECT min(data) FROM pregoes WHERE data > b.data_com) END AS data_ex
    FROM bruto b
""")

# 5. Tickers do universo: nomes que tiveram, período na base e classe (`tipo`) do COTAHIST
con.sql("""
    CREATE TEMP TABLE tickers_nome AS
    SELECT DISTINCT ticker, nome FROM acoes WHERE ticker IN (SELECT ticker FROM universo_mensal)
""")
con.sql("""
    CREATE TEMP TABLE periodo AS
    SELECT ticker, min(data) AS primeiro, max(data) AS ultimo, count(DISTINCT tipo) AS n_tipos,
           any_value(tipo) AS tipo, bool_or(eh_unit) AS eh_unit
    FROM acoes WHERE ticker IN (SELECT ticker FROM universo_mensal) GROUP BY ticker
""")

# 6. Candidatos: registro do emissor (pelo nome) com data ex dentro do período do
#    ticker. Classe do COTAHIST = `tipo` na data ex ou no pregão anterior mais próximo
con.sql("""
    CREATE TEMP TABLE candidatos AS
    SELECT c.*, a.tipo AS classe_cotahist
    FROM (SELECT r.id_registro, r.classe_b3, r.data_ex, tn.ticker
          FROM registros r
          JOIN tickers_nome tn ON tn.nome = r.nome_cotahist
          JOIN periodo p ON p.ticker = tn.ticker AND r.data_ex BETWEEN p.primeiro AND p.ultimo) c
    ASOF JOIN acoes a ON a.ticker = c.ticker AND a.data <= c.data_ex
""")

# 7. Par: só quando a classe da B3 é igual à classe do COTAHIST (unit só com UNT)
con.sql("CREATE TEMP TABLE pares AS SELECT id_registro, ticker FROM candidatos WHERE classe_b3 = classe_cotahist")

# 8. Marcas do COTAHIST, por letra: o ticker "tem D" (ou J) quando a marca começa
#    com E e tem a letra depois do E. Início de D = tem D e o pregão anterior do
#    mesmo ticker não tinha. O primeiro pregão do ticker não conta como início
con.sql("""
    CREATE TEMP TABLE marcas_dia AS
    SELECT a.ticker, a.data, p.n, coalesce(a.marca, '') AS marca,
           lag(coalesce(a.marca, '')) OVER w AS anterior
    FROM acoes a JOIN pregoes p USING (data)
    WHERE a.ticker IN (SELECT ticker FROM universo_mensal)
    WINDOW w AS (PARTITION BY a.ticker ORDER BY a.data)
""")
con.sql("""
    CREATE TEMP TABLE marcas AS
    SELECT m.*, l.letra,
           m.marca LIKE 'E%' AND contains(substr(m.marca, 2), l.letra) AS tem,
           m.marca LIKE 'E%' AND contains(substr(m.marca, 2), l.letra)
               AND m.anterior IS NOT NULL
               AND NOT (m.anterior LIKE 'E%' AND contains(substr(m.anterior, 2), l.letra)) AS inicio
    FROM marcas_dia m, (VALUES ('D'), ('J')) l(letra)
""")

print("Transições de marca observadas no universo (pregão anterior → pregão do dia, só quando mudam)")
con.sql("""
    SELECT CASE WHEN anterior = '' THEN '(sem marca)' ELSE anterior END AS anterior,
           CASE WHEN marca = '' THEN '(sem marca)' ELSE marca END AS marca, count(*) AS vezes
    FROM marcas_dia WHERE anterior <> marca GROUP BY ALL ORDER BY vezes DESC, anterior, marca
""").show(max_rows=500)
print("Marca D ou J já presente no primeiro pregão do ticker (não contada como início)")
con.sql("""
    SELECT letra, count(*) AS tickers FROM marcas WHERE tem AND anterior IS NULL GROUP BY letra ORDER BY letra
""").show()

# 9. Tabela final: uma linha por registro da B3 (e por ticker pareado, se houver).
#    "Marca do tipo" = marca de evento (começa com E) que contém a letra do tipo (D ou J)
con.sql("""
    CREATE OR REPLACE TABLE proventos_b3 AS
    SELECT r.id_registro, r.nome_cotahist, r.nome_consultado, r.arquivo, r.classe_b3, r.tipo_b3,
           r.tipo_provento, r.data_aprovacao, r.data_com, r.data_ex, r.valor, r.repetido, r.repeticao,
           p.ticker,
           p.ticker IS NOT NULL AND min(r.nome_cotahist) OVER g <> max(r.nome_cotahist) OVER g
                                                                 AS repetido_entre_consultas,
           m.marca AS marca_no_dia,
           CASE WHEN p.ticker IS NULL OR m.data IS NULL THEN 'nao_comparavel'
                WHEN m.marca = ''                       THEN 'sem_marca'
                WHEN r.letra IS NULL OR NOT ml.tem      THEN 'outra_marca'
                WHEN NOT ml.inicio                      THEN 'marca_vigente'
                WHEN m.marca = 'E' || r.letra           THEN 'inicio_exato'
                ELSE 'inicio_composto' END AS pareamento
    FROM registros r
    LEFT JOIN pares p USING (id_registro)
    LEFT JOIN marcas_dia m ON m.ticker = p.ticker AND m.data = r.data_ex
    LEFT JOIN marcas ml ON ml.ticker = p.ticker AND ml.data = r.data_ex AND ml.letra = r.letra
    WINDOW g AS (PARTITION BY p.ticker, r.classe_b3, r.tipo_b3, r.data_com, r.valor)
    ORDER BY r.id_registro, p.ticker
""")

# Checagem 1: contagens, chave, nulos e faixas
linhas, chaves, n_registros, sem_data, sem_valor, negativos = con.sql("""
    SELECT count(*), count(DISTINCT (id_registro, coalesce(ticker, ''))), count(DISTINCT id_registro),
           count(*) FILTER (WHERE data_com IS NULL), count(*) FILTER (WHERE valor IS NULL),
           count(*) FILTER (WHERE valor < 0)
    FROM proventos_b3
""").fetchall()[0]
print(f"Checagem 1: linhas={linhas} chaves(id_registro, ticker)={chaves} registros_b3={n_registros} "
      f"lidos={len(registros)} sem_data_com={sem_data} sem_valor={sem_valor} valor_negativo={negativos}")
if linhas != chaves or n_registros != len(registros) or negativos:
    raise ValueError("proventos_b3: chave repetida, registro perdido ou valor negativo")
print("  registros com mais de um ticker pareado:", linhas - n_registros)
con.sql("""
    SELECT count(*) AS linhas, count(ticker) AS com_ticker, count(DISTINCT ticker) AS tickers,
           count(DISTINCT nome_cotahist) AS nomes, min(data_com) AS com_min, max(data_com) AS com_max,
           min(valor) AS valor_min, max(valor) AS valor_max,
           count(*) FILTER (WHERE repetido) AS repetidos,
           count(*) FILTER (WHERE repetido_entre_consultas) AS repetidos_entre_consultas
    FROM proventos_b3
""").show(max_width=250)

print("Checagem 2: tipo da B3 × classe da B3 (linhas com e sem ticker)")
con.sql("""
    SELECT tipo_b3, tipo_provento, count(*) AS linhas, count(ticker) AS com_ticker,
           string_agg(DISTINCT classe_b3, ' ' ORDER BY classe_b3) AS classes
    FROM proventos_b3 GROUP BY ALL ORDER BY linhas DESC
""").show(max_rows=50, max_width=200)

print("Checagem 3: por que a linha não tem ticker")
con.sql(f"""
    SELECT CASE WHEN p.ticker IS NOT NULL THEN 'com ticker'
                WHEN p.data_com IS NULL THEN 'sem data com'
                WHEN year(p.data_com) < {ANO_MIN} THEN 'data com anterior a {ANO_MIN}'
                WHEN p.data_ex IS NULL THEN 'sem data ex (data com no fim da base)'
                WHEN NOT EXISTS (SELECT 1 FROM candidatos c WHERE c.id_registro = p.id_registro)
                     THEN 'data ex fora do período de todos os tickers do nome'
                ELSE 'classe da B3 sem ticker de mesma classe no universo' END AS situacao,
           count(*) AS linhas
    FROM proventos_b3 p GROUP BY ALL ORDER BY linhas DESC
""").show(max_width=200)

# Condição 1 — pareamento
print("Condição 1 — pareamento com a marca do COTAHIST, por tipo de provento (linhas com ticker)")
con.sql("""
    PIVOT (SELECT tipo_provento, pareamento FROM proventos_b3 WHERE ticker IS NOT NULL)
    ON pareamento USING count(*) GROUP BY tipo_provento ORDER BY tipo_provento
""").show(max_width=250)
print("Marca no dia × tipo de provento (linhas com ticker e pregão no dia)")
con.sql("""
    PIVOT (SELECT tipo_provento, CASE WHEN marca_no_dia = '' THEN '(sem marca)' ELSE marca_no_dia END AS marca
           FROM proventos_b3 WHERE marca_no_dia IS NOT NULL)
    ON tipo_provento USING count(*) GROUP BY marca ORDER BY marca
""").show(max_rows=100)

# Classe — pendências e os dois relatos pedidos
print("Classe (a): tickers cujo `tipo` no COTAHIST difere da regra do final do ticker (3→ON, 4→PN, 5→PNA, 6→PNB, 11→UNT)")
con.sql("CREATE TEMP TABLE sufixo (final VARCHAR, classe VARCHAR)")
con.executemany("INSERT INTO sufixo VALUES (?, ?)", list(SUFIXO_CLASSE.items()))
con.sql("""
    SELECT p.ticker, p.tipo AS tipo_cotahist, s.classe AS pela_regra_do_final, p.n_tipos
    FROM periodo p LEFT JOIN sufixo s ON s.final = regexp_extract(p.ticker, '[0-9]+')
    WHERE p.tipo IS DISTINCT FROM s.classe OR p.n_tipos > 1 ORDER BY p.ticker
""").show(max_rows=100)

con.sql("""
    CREATE TEMP TABLE classe_pendente AS
    SELECT c.ticker, c.classe_cotahist, count(*) AS registros_do_emissor_no_periodo,
           string_agg(DISTINCT c.classe_b3, ' ' ORDER BY c.classe_b3) AS classes_na_b3
    FROM candidatos c GROUP BY c.ticker, c.classe_cotahist
    HAVING count(*) FILTER (WHERE c.classe_b3 = c.classe_cotahist) = 0
""")
print("Pendência de classe: ticker com registros do emissor no período, nenhum com a classe do COTAHIST")
con.sql("SELECT * FROM classe_pendente ORDER BY ticker").show(max_rows=300, max_width=200)
print("Condição 6 — units entre as pendências de classe (sem registro UNT; nada foi composto)")
con.sql("""
    SELECT c.* FROM classe_pendente c JOIN periodo p USING (ticker) WHERE p.eh_unit ORDER BY ticker
""").show(max_rows=100, max_width=200)
print("Classe (b): pendências que um 'PN genérico' resolveria (ticker PNA/PNB, B3 só com PN) — regra NÃO aplicada")
con.sql("""
    SELECT c.ticker, c.classe_cotahist, c.classes_na_b3,
           (SELECT count(*) FROM candidatos k WHERE k.ticker = c.ticker AND k.classe_b3 = 'PN') AS registros_pn
    FROM classe_pendente c
    WHERE c.classe_cotahist IN ('PNA', 'PNB', 'PNC') AND list_contains(string_split(c.classes_na_b3, ' '), 'PN')
    ORDER BY c.ticker
""").show(max_rows=100, max_width=200)

# Condição 2 — detector de lacunas
con.sql("""
    CREATE TEMP TABLE pareados AS
    SELECT DISTINCT p.ticker, r.letra, g.n
    FROM proventos_b3 p JOIN registros r USING (id_registro) JOIN pregoes g ON g.data = p.data_ex
    WHERE p.ticker IS NOT NULL
""")
con.sql("""
    CREATE TEMP TABLE por_ticker AS
    SELECT p.ticker, p.tipo,
           (SELECT string_agg(DISTINCT nome, ' | ') FROM tickers_nome t WHERE t.ticker = p.ticker) AS nomes,
           (SELECT count(*) FROM marcas m WHERE m.ticker = p.ticker AND m.inicio) AS inicios_marca_dj,
           (SELECT coalesce(sum(c.registros), 0) FROM consultas c JOIN tickers_nome t ON t.nome = c.nome_cotahist
             WHERE t.ticker = p.ticker) AS registros_b3_dos_nomes,
           (SELECT count(*) FROM proventos_b3 v WHERE v.ticker = p.ticker) AS linhas_pareadas
    FROM periodo p
""")
print("Condição 2b — empresa com marcas D/J no COTAHIST e zero registros na B3 (todas as variantes do nome)")
con.sql("""
    SELECT ticker, tipo, nomes, inicios_marca_dj FROM por_ticker
    WHERE inicios_marca_dj > 0 AND registros_b3_dos_nomes = 0 ORDER BY ticker
""").show(max_rows=300, max_width=200)
print("Tickers com marcas D/J, registros na B3 para o nome, mas nenhuma linha pareada (classe ou período)")
con.sql("""
    SELECT ticker, tipo, nomes, inicios_marca_dj, registros_b3_dos_nomes FROM por_ticker
    WHERE inicios_marca_dj > 0 AND registros_b3_dos_nomes > 0 AND linhas_pareadas = 0 ORDER BY ticker
""").show(max_rows=300, max_width=200)

con.sql(f"""
    CREATE TEMP TABLE lacunas AS
    SELECT m.ticker, m.data, m.marca, m.anterior AS marca_anterior, m.letra,
           EXISTS (SELECT 1 FROM pareados v WHERE v.ticker = m.ticker
                     AND abs(v.n - m.n) <= {JANELA_PREGOES}) AS outro_tipo_em_ate_5_pregoes,
           t.registros_b3_dos_nomes = 0 AS empresa_sem_registros_na_b3,
           t.linhas_pareadas = 0 AS ticker_sem_linha_pareada
    FROM marcas m JOIN por_ticker t USING (ticker)
    WHERE m.inicio AND year(m.data) BETWEEN {ANO_MIN} AND {ANO_MAX}
      AND NOT EXISTS (SELECT 1 FROM pareados v WHERE v.ticker = m.ticker AND v.letra = m.letra
                        AND abs(v.n - m.n) <= {JANELA_PREGOES})
""")
con.sql(f"COPY (SELECT * FROM lacunas ORDER BY ticker, data, letra) TO '{PENDENCIAS}' (HEADER, DELIMITER ',')")
print(f"Condição 2a — início de marca D ou J sem provento do mesmo tipo em até {JANELA_PREGOES} pregões "
      f"(lista em {PENDENCIAS})")
con.sql(f"""
    SELECT (SELECT count(*) FROM marcas m WHERE m.inicio AND year(m.data) BETWEEN {ANO_MIN} AND {ANO_MAX}) AS inicios_de_marca,
           count(*) AS pendentes, count(DISTINCT ticker) AS tickers,
           count(*) FILTER (WHERE empresa_sem_registros_na_b3) AS de_empresa_sem_registros,
           count(*) FILTER (WHERE ticker_sem_linha_pareada AND NOT empresa_sem_registros_na_b3)
               AS de_ticker_sem_linha_pareada,
           count(*) FILTER (WHERE NOT ticker_sem_linha_pareada) AS de_ticker_com_proventos,
           count(*) FILTER (WHERE NOT ticker_sem_linha_pareada AND outro_tipo_em_ate_5_pregoes)
               AS dessas_com_outro_tipo_perto
    FROM lacunas
""").show(max_width=250)
print("Pendentes por ano (só tickers com proventos pareados)")
con.sql("""
    SELECT year(data) AS ano, count(*) AS pendentes, count(DISTINCT ticker) AS tickers
    FROM lacunas WHERE NOT ticker_sem_linha_pareada GROUP BY ano ORDER BY ano
""").show(max_rows=30)

# Condição 4 — taxa de coincidência por empresa (raiz do ticker) e por nome consultado,
# em grupos ticker / data ex / tipo de dividendos e JCP com pregão no dia
for tabela, chave, outra in (("taxas", "left(ticker, 4) AS empresa", "string_agg(DISTINCT nome_consultado, ' | ') AS nomes_consultados"),
                             ("taxas_nome", "nome_consultado", "string_agg(DISTINCT ticker, ' ' ORDER BY ticker) AS tickers")):
    con.sql(f"""
    CREATE TEMP TABLE {tabela} AS
    SELECT {chave}, {outra},
           count(*) AS grupos,
           count(*) FILTER (WHERE pareamento = 'inicio_exato')    AS inicio_exato,
           count(*) FILTER (WHERE pareamento = 'inicio_composto') AS inicio_composto,
           count(*) FILTER (WHERE pareamento = 'marca_vigente')   AS marca_vigente,
           count(*) FILTER (WHERE pareamento = 'outra_marca')     AS outra_marca,
           count(*) FILTER (WHERE pareamento = 'sem_marca')       AS sem_marca,
           round(count(*) FILTER (WHERE pareamento LIKE 'inicio%') / count(*), 3) AS taxa_inicio,
           round(count(*) FILTER (WHERE pareamento IN ('inicio_exato', 'inicio_composto', 'marca_vigente'))
                 / count(*), 3) AS taxa_com_vigente
    FROM (SELECT DISTINCT ticker, data_ex, tipo_provento, pareamento, nome_consultado FROM proventos_b3
          WHERE ticker IS NOT NULL AND tipo_provento <> 'outro' AND pareamento <> 'nao_comparavel')
    GROUP BY ALL
""")
print(f"Condição 4 — as {PIORES} piores taxas de coincidência por empresa (taxa_inicio = início exato ou composto)")
con.sql(f"SELECT * FROM taxas ORDER BY taxa_inicio, grupos DESC LIMIT {PIORES}").show(max_width=250)
print("Distribuição da taxa_inicio entre as empresas")
con.sql("""
    SELECT count(*) AS empresas, sum(grupos) AS grupos, min(taxa_inicio) AS minimo,
           quantile_cont(taxa_inicio, 0.1) AS p10, median(taxa_inicio) AS mediana, max(taxa_inicio) AS maximo,
           count(*) FILTER (WHERE taxa_inicio < 0.5) AS abaixo_de_50, count(*) FILTER (WHERE taxa_inicio < 0.8) AS abaixo_de_80,
           round(sum(inicio_exato + inicio_composto) / sum(grupos), 3) AS taxa_geral
    FROM taxas
""").show(max_width=250)
print("Todas as empresas")
con.sql("SELECT * FROM taxas ORDER BY empresa").show(max_rows=400, max_width=250)
print(f"Condição 4 — as {PIORES} piores taxas de coincidência por nome consultado")
con.sql(f"SELECT * FROM taxas_nome ORDER BY taxa_inicio, grupos DESC LIMIT {PIORES}").show(max_width=250)
print("Todos os nomes consultados")
con.sql("SELECT * FROM taxas_nome ORDER BY nome_consultado").show(max_rows=400, max_width=250)

# Condição 5 — registros idênticos: contagem por nome e comparação anual para PETR, VALE e WEGE
print("Condição 5 — registros idênticos na mesma consulta (marcados em `repetido`; nada removido nem somado)")
con.sql("""
    SELECT nome_consultado, count(DISTINCT id_registro) FILTER (WHERE repeticao = 1) AS grupos_repetidos,
           count(DISTINCT id_registro) FILTER (WHERE repeticao > 1) AS excedentes,
           min(data_com) AS com_min, max(data_com) AS com_max
    FROM proventos_b3 WHERE repetido GROUP BY nome_consultado ORDER BY excedentes DESC
""").show(max_rows=300)
print("Registros iguais vindos de consultas por nomes diferentes (`repetido_entre_consultas`)")
con.sql("""
    SELECT ticker, count(*) AS linhas, string_agg(DISTINCT nome_consultado, ' | ') AS nomes
    FROM proventos_b3 WHERE repetido_entre_consultas GROUP BY ticker ORDER BY ticker
""").show(max_rows=100, max_width=200)

print("Total anual em R$ por ação (todos os tipos da classe), pelo ano da data com; yfinance pelo ano da data ex")
print(f"{'ticker':6} {'ano':4} | {'B3 com repetidos':>18} | {'B3 sem repetidos':>18} | {'repet.':>6} | "
      f"{'brapi':>14} | {'Dados de Mercado':>16} | {'yfinance':>14}")
for ticker, (nome, classe) in COMPARAR.items():
    b3_ano = {a: (n, s, n1, s1, rep) for a, n, s, n1, s1, rep in con.sql(f"""
        SELECT year(data_com), count(*), sum(valor), count(*) FILTER (WHERE repeticao = 1),
               sum(valor) FILTER (WHERE repeticao = 1), count(*) FILTER (WHERE repeticao > 1)
        FROM bruto WHERE nome_consultado = '{nome}' AND classe_b3 = '{classe}'
          AND year(data_com) BETWEEN {ANO_MIN} AND {ANO_MAX} GROUP BY ALL
    """).fetchall()}
    arq_brapi = Path(f"dados/brutos/brapi_teste/quote_{ticker}.json")
    arq_dm = Path(f"dados/brutos/dadosdemercado_teste/dividendos_{ticker}.html")
    arq_yf = Path(f"dados/brutos/yfinance_teste/dividendos_{ticker}.csv")
    brapi = dm = yf = None
    if arq_brapi.exists():
        brapi = [(r["lastDatePrior"][:4], r["rate"])
                 for r in json.loads(arq_brapi.read_text())["results"][0]["dividendsData"]["cashDividends"]
                 if r.get("lastDatePrior")]
    if arq_dm.exists():
        tabela = re.search(r"(?s)<table.*?</table>", arq_dm.read_text(encoding="utf-8")).group()
        celulas = [[html.unescape(re.sub(r"<[^>]+>", "", c)).strip() for c in re.findall(r"(?s)<td.*?</td>", linha)]
                   for linha in re.findall(r"(?s)<tr.*?</tr>", tabela)]
        dm = [(c[2][-4:], float(c[1].replace("*", "").replace(".", "").replace(",", ".")))
              for c in celulas if len(c) == 5]
    if arq_yf.exists():
        yf = [(linha[:4], float(linha.split(",")[1])) for linha in arq_yf.read_text().splitlines()[1:]]

    def resumo(fonte, ano):
        if fonte is None:
            return "não salvo"
        valores = [v for a, v in fonte if a == str(ano)]
        return f"{len(valores):>2} / {sum(valores):.6f}"

    for ano in range(ANO_MIN, ANO_MAX + 1):
        n, s, n1, s1, rep = b3_ano.get(ano, (0, 0.0, 0, 0.0, 0))
        print(f"{ticker:6} {ano} | {n:>3} / {s or 0:>12.6f} | {n1:>3} / {s1 or 0:>12.6f} | {rep:>6} | "
              f"{resumo(brapi, ano):>14} | {resumo(dm, ano):>16} | {resumo(yf, ano):>14}")

con.commit()
con.close()
