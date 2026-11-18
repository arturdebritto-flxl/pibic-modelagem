import base64
import csv
import hashlib
import html
import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

# Fase 3 (rodada 2) — tabela `eventos`: grupamentos, desdobramentos e bonificações da B3
# (`stockDividends` do GetListedSupplementCompany) e proventos em dinheiro (`proventos_b3`,
# D19, D23, D24), mais as lacunas (Parte 3), para os tickers do universo e os sucessores
# propostos em saidas.csv. Especificação: .agentes/specs/rodada2_eventos.md.
# Regras aprovadas pelo responsável (17/11/2026):
#   - evento de quantidade → ticker pelo ISIN (`isinCode` = `isin` do ticker no pregão anterior à data ex);
#   - validação pelo preço com `preco_por_acao` (fechamento / fatcot) nos dois dias, por DIA: fator do dia =
#     produto dos fatores de todos os eventos do domínio do ticker com a mesma data ex (itens gravados separados);
#   - entrada (18/11/2026), resíduo = |ln(razão de preço × fator do dia)|: fator do dia 1 → fator_liquido_1;
#     resíduo <= TOL_PRECO → preco (marca_e_preco); resíduo < |ln(razão)| e <= ln(2) → preco_aproximado (limite
#     ln(2) acrescentado pelo responsável na rodada 3);
#     senão fica fora, como pendência;
#   - rótulos fora do domínio e bonificação que entrega outra classe: só listados;
#   - D23: repetidos da mesma consulta somam (linhas com `seq` distinto, `parcela_repetida`);
#     repetidos entre consultas: uma linha só;
#   - JBSS3 (Dados de Mercado): marca sem par único vira pendência.

ANO_MIN, ANO_MAX = 2010, 2026
TOL_PRECO = 0.10         # resíduo |ln(preço ex / preço anterior × fator do dia)| máximo para confirmar pelo preço
RESIDUO_MAX = 0.6931     # ln(2): resíduo máximo de `preco_aproximado` (D25, rodada 3); acima, pendência
JANELA_PREGOES = 5
RETORNO_EXTREMO = 0.50   # checagem 4: |retorno bruto de fechamento| acima disso
PIORES = 20
PAUSA_S = 3
AGENTE = "Mozilla/5.0 (X11; Linux x86_64) pesquisa-academica-PIBIC-UnB"
B3 = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall"
PASTA_SUP = Path("dados/brutos/b3_cadastro")
PASTA_SUP_FASE2 = Path("dados/brutos/b3_teste")  # suplementos baixados na Fase 2 (CIEL, CRUZ, ENBR, PETR, VALE)
DM_JBSS3 = Path("dados/brutos/dadosdemercado/dividendos_JBSS3.html")
PASTA_DM = Path("dados/brutos/dadosdemercado")   # página principal do ticker (tabela de eventos de quantidade)
PASTA_YF = Path("dados/brutos/yfinance")          # `splits` do yfinance (D26), em JSON
JANELA_EXTERNO = 1       # D26: evento de outra fonte a até 1 pregão de um início de marca G ou B
DATA_MERCADO = "2026-10-26"  # queda de mercado pós-eleição (Ibovespa −13,88%): fora do detector de saltos
SALTO = 0.40             # |retorno de preco_por_acao| do detector de saltos (rodada 3)
SAIDAS = Path("dados/manuais/saidas.csv")
PENDENCIAS = Path("resultados/fase3_pendencias_marcas.csv")
# Parte 3a: tickers com marcas D/J e zero registros, e tickers sem linha pareada
LACUNAS_3A = "BBTG11 BISA3 DROG3 ESTC3 OHLB3 QGEP3 SUZB5 TBLE3 TLPP4 TMAR5 ALLL11 ALLL3 BRTO4 DTEX3 SMLE3".split()
SO_LISTAR_3A = {"BBTG11"}  # D21: sem ligação; identidade do emissor composta
JBSS3_MARCAS_ESPERADAS = 11
OBS_D23 = "D23 não confirmada nesta fonte"
OBS_MESMO_DIA = "provento e evento de quantidade na mesma data ex; base (ação antiga ou nova) não informada pela fonte"


def baixa(url, destino):
    """Baixa uma vez (bruto nunca é sobrescrito) e registra o hash; False se a página não existe.
    HTTP 404 fica registrado num marcador `<destino>.404` (vazio, com hash) para não repetir a consulta;
    falha de rede é tentada de novo até 3 vezes."""
    marcador = destino.with_name(destino.name + ".404")
    if destino.exists():
        return True
    if marcador.exists():
        return False
    pedido = urllib.request.Request(url, headers={"User-Agent": AGENTE})
    for tentativa in range(3):
        try:
            with urllib.request.urlopen(pedido, timeout=60) as resposta:
                conteudo = resposta.read()
            break
        except urllib.error.HTTPError as erro:
            print(f"{url}: HTTP {erro.code}")
            time.sleep(PAUSA_S)
            if erro.code == 404:
                marcador.parent.mkdir(parents=True, exist_ok=True)
                marcador.write_bytes(b"")
                with open("dados/hashes.txt", "a") as hashes:
                    hashes.write(f"{hashlib.sha256(b'').hexdigest()}  {marcador}\n")
            return False
        except urllib.error.URLError:
            if tentativa == 2:
                raise
            time.sleep(10 * PAUSA_S)
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_bytes(conteudo)
    with open("dados/hashes.txt", "a") as hashes:
        hashes.write(f"{hashlib.sha256(conteudo).hexdigest()}  {destino}\n")
    time.sleep(PAUSA_S)
    return True


def data_iso(texto):
    """DD/MM/AAAA -> AAAA-MM-DD; None se vazio."""
    return f"{texto[6:10]}-{texto[3:5]}-{texto[:2]}" if texto and len(texto) >= 10 else None


def numero(texto):
    return float(texto.replace("*", "").replace(".", "").replace(",", ".").strip())


def eventos_dm(ticker):
    """Tabela "Evento, Aprovação, Registro, Ex, Razão" da página do ticker no Dados de Mercado.
    Desdobramento (também usado para grupamento, ex.: MGLU3 2024 "10:1") e grupamento: a:b = ações antes : depois,
    fator = b/a. Bonificação: a:b = a ações novas para cada b, fator = 1 + a/b (ver notas/eventos_b3.md)."""
    destino = PASTA_DM / f"pagina_{ticker}.html"
    if not baixa(f"https://www.dadosdemercado.com.br/acoes/{ticker.lower()}", destino):
        return None
    saida = []
    for tabela in re.findall(r"(?s)<table.*?</table>", destino.read_text(encoding="utf-8")):
        linhas = [[html.unescape(re.sub(r"<[^>]+>", "", c)).strip() for c in re.findall(r"(?s)<t[hd].*?</t[hd]>", tr)]
                  for tr in re.findall(r"(?s)<tr.*?</tr>", tabela)]
        if not linhas or linhas[0] != ["Evento", "Aprovação", "Registro", "Ex", "Razão"]:
            continue
        for evento, _, _, ex, razao in linhas[1:]:
            a, b = (float(x.replace(",", ".")) for x in razao.split(":"))
            fator = 1 + a / b if evento.startswith("Bonifica") else b / a
            saida.append((ticker, "Dados de Mercado", evento, razao, data_iso(ex), fator))
    return saida


def eventos_yf(ticker):
    """`splits` do yfinance (razão = ações depois / antes), salvo uma vez em JSON; None se o Yahoo não conhece."""
    destino = PASTA_YF / f"splits_{ticker}.json"
    if not destino.exists():
        import yfinance
        serie = yfinance.Ticker(f"{ticker}.SA").splits
        dados = {"indisponivel": True} if serie is None else {str(k.date()): float(v) for k, v in serie.items()}
        conteudo = json.dumps(dados, indent=1).encode()
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(conteudo)
        with open("dados/hashes.txt", "a") as hashes:
            hashes.write(f"{hashlib.sha256(conteudo).hexdigest()}  {destino}\n")
        time.sleep(PAUSA_S)
    dados = json.loads(destino.read_text())
    if dados.get("indisponivel"):
        return None
    return [(ticker, "yfinance", "split", f"{v:g}", d, v) for d, v in dados.items()]


# 1. Escopo: tickers do universo e sucessores propostos em saidas.csv (como em proventos.py)
with open(SAIDAS, newline="") as arq:
    saidas = list(csv.DictReader(arq))
sucessores = sorted({t.strip() for r in saidas for t in re.split(r"[+,]", r["sucessor"]) if t.strip()})
with duckdb.connect("dados/pibic.duckdb", read_only=True) as con:
    universo = {r[0] for r in con.sql("SELECT DISTINCT ticker FROM universo_mensal").fetchall()}
    escopo = sorted(universo) + [t for t in sucessores if t not in universo]
    # Parte 3a: nomes do sucessor (saidas.csv) e de tickers com o mesmo CNPJ no FCA, exceto os do próprio ticker
    nomes_3a, so_proprios = [], {}
    for r in saidas:
        if r["ticker"] not in LACUNAS_3A:
            continue
        ligados = {t.strip() for t in re.split(r"[+,]", r["sucessor"]) if t.strip()}
        cnpj = re.search(r"CNPJ ([0-9./-]+)", r["evidencia_automatica"])
        if cnpj:
            ligados |= {t for (t,) in con.sql("SELECT DISTINCT ticker FROM cvm_fca_valor_mobiliario "
                                               "WHERE cnpj = ? AND ticker IS NOT NULL", params=[cnpj.group(1)]).fetchall()}
        proprios = {n for (n,) in con.sql("SELECT DISTINCT nome FROM acoes WHERE ticker = ?", params=[r["ticker"]]).fetchall()}
        nomes = [n for (n,) in con.sql("SELECT DISTINCT nome FROM acoes WHERE list_contains(?, ticker) ORDER BY nome",
                                       params=[sorted(ligados)]).fetchall()]
        nomes_3a += [(r["ticker"], n) for n in nomes if n not in proprios]
        if nomes and set(nomes) <= proprios:
            so_proprios[r["ticker"]] = nomes
raizes = sorted({t[:4] for t in escopo})
print(f"Escopo: {len(escopo)} tickers ({len(universo)} do universo, {len(escopo) - len(universo)} sucessores fora dele); "
      f"{len(raizes)} raízes consultadas no GetListedSupplementCompany")

# 2. Downloads (antes de abrir a conexão de escrita): suplemento de cada raiz e dividendos da JBSS3
for raiz in raizes:
    if not (PASTA_SUP_FASE2 / f"suplemento_{raiz}.json").exists():
        parametros = json.dumps({"issuingCompany": raiz, "language": "pt-br"}, separators=(",", ":"))
        baixa(f"{B3}/GetListedSupplementCompany/{base64.b64encode(parametros.encode()).decode()}",
              PASTA_SUP / f"suplemento_{raiz}.json")
tem_dm_jbss3 = baixa("https://www.dadosdemercado.com.br/acoes/jbss3/dividendos", DM_JBSS3)

# 3. Leitura dos eventos de quantidade da B3: uma linha por item de `stockDividends`
eventos_b3, sem_emissor = [], []
for raiz in raizes:
    arquivo = PASTA_SUP / f"suplemento_{raiz}.json"
    if not arquivo.exists():
        arquivo = PASTA_SUP_FASE2 / arquivo.name
    dados = json.loads(arquivo.read_text() or "[]")  # raiz desconhecida: resposta vazia ou lista vazia
    if not isinstance(dados, list) or len(dados) > 1 or any(not isinstance(e, dict) for e in dados):
        raise ValueError(f"{raiz}: formato de resposta inesperado ou mais de um emissor")
    if not dados:
        sem_emissor.append(raiz)
    for emissor in dados:
        for e in emissor.get("stockDividends") or []:
            eventos_b3.append((raiz, str(arquivo), (e.get("label") or "").strip(), numero(e.get("factor") or ""),
                               (e.get("isinCode") or "").strip(), (e.get("assetIssued") or "").strip(),
                               data_iso(e.get("approvedOn")), data_iso(e.get("lastDatePrior")),
                               (e.get("remarks") or "").strip()))
print(f"Raízes sem emissor na B3 ({len(sem_emissor)}): {' '.join(sem_emissor)}")
print(f"Itens de stockDividends lidos: {len(eventos_b3)}")

# Dados de Mercado, JBSS3: (tipo, valor, ajustado, data com, data ex)
dm_jbss3 = []
if tem_dm_jbss3:
    tabela = re.search(r"(?s)<table.*?</table>", DM_JBSS3.read_text(encoding="utf-8")).group()
    for linha in re.findall(r"(?s)<tr.*?</tr>", tabela):
        c = [html.unescape(re.sub(r"<[^>]+>", "", x)).strip() for x in re.findall(r"(?s)<td.*?</td>", linha)]
        if len(c) == 5:  # Tipo, Valor, Registro (data com), Ex, Pagamento
            dm_jbss3.append((c[0], numero(c[1]), "*" in c[1], data_iso(c[2]), data_iso(c[3])))

con = duckdb.connect("dados/pibic.duckdb")
con.begin()
con.sql("CREATE TEMP TABLE escopo (ticker VARCHAR, escopo VARCHAR)")
con.executemany("INSERT INTO escopo VALUES (?, ?)", [(t, "universo" if t in universo else "sucessor") for t in escopo])
con.sql("""CREATE TEMP TABLE sup (raiz VARCHAR, arquivo VARCHAR, rotulo VARCHAR, fator_b3 DOUBLE, isin VARCHAR,
           isin_entregue VARCHAR, data_aprovacao DATE, data_com DATE, observacao_b3 VARCHAR)""")
con.executemany("INSERT INTO sup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", eventos_b3)
con.sql("CREATE TEMP TABLE nomes_3a (ticker VARCHAR, nome VARCHAR)")
if nomes_3a:
    con.executemany("INSERT INTO nomes_3a VALUES (?, ?)", nomes_3a)
con.sql("CREATE TEMP TABLE dm_jbss3 (tipo VARCHAR, valor DOUBLE, ajustado BOOLEAN, data_com DATE, data_ex DATE)")
if dm_jbss3:
    con.executemany("INSERT INTO dm_jbss3 VALUES (?, ?, ?, ?, ?)", dm_jbss3)

# 4. Calendário da bolsa e pregões dos tickers do escopo, com o pregão anterior do mesmo ticker
con.sql("""
    CREATE TEMP TABLE pregoes AS
    SELECT data, row_number() OVER (ORDER BY data) AS n FROM (SELECT DISTINCT data FROM cotacoes)
""")
con.sql("""
    CREATE TEMP TABLE dia AS
    SELECT a.ticker, a.data, p.n, a.isin, a.tipo, a.fechamento, a.preco_por_acao, a.mudou_fatcot,
           coalesce(a.marca, '') AS marca,
           lag(a.data) OVER w AS data_ant, lag(a.fechamento) OVER w AS fech_ant, lag(a.preco_por_acao) OVER w AS ppa_ant,
           lag(coalesce(a.marca, '')) OVER w AS marca_ant
    FROM acoes a JOIN pregoes p USING (data)
    WHERE a.ticker IN (SELECT ticker FROM escopo)
    WINDOW w AS (PARTITION BY a.ticker ORDER BY a.data)
""")
# Marcas por letra (como em proventos.py): início = tem a letra e o pregão anterior do ticker não tinha
con.sql("""
    CREATE TEMP TABLE marcas AS
    SELECT d.ticker, d.data, d.n, d.marca, l.letra,
           d.marca LIKE 'E%' AND contains(substr(d.marca, 2), l.letra) AS tem,
           d.marca LIKE 'E%' AND contains(substr(d.marca, 2), l.letra) AND d.marca_ant IS NOT NULL
               AND NOT (d.marca_ant LIKE 'E%' AND contains(substr(d.marca_ant, 2), l.letra)) AS inicio
    FROM dia d, (VALUES ('D'), ('J'), ('G'), ('B')) l(letra)
""")
con.sql("""
    CREATE TEMP TABLE periodo AS
    SELECT ticker, min(data) AS primeiro, max(data) AS ultimo FROM dia GROUP BY ticker
""")

# ============================ Parte 2 — eventos de quantidade ============================
# Data ex = primeiro pregão depois da última data com (mesma regra dos proventos), só para data com 2010–2026
con.sql(f"""
    CREATE TEMP TABLE sup_ex AS
    SELECT s.*, CASE WHEN year(s.data_com) BETWEEN {ANO_MIN} AND {ANO_MAX}
                     THEN (SELECT min(data) FROM pregoes WHERE data > s.data_com) END AS data_ex
    FROM sup s
""")
print("\nParte 2 — itens de stockDividends por rótulo (todas as datas | data ex na base)")
con.sql("""
    SELECT rotulo, count(*) AS itens, count(data_ex) AS com_data_ex_na_base,
           count(*) FILTER (WHERE isin_entregue <> isin) AS ativo_entregue_diferente
    FROM sup_ex GROUP BY rotulo ORDER BY itens DESC
""").show(max_rows=50)

# Par por ISIN: ticker do escopo cujo pregão anterior à data ex tem o ISIN do evento
con.sql("""
    CREATE TEMP TABLE sup_par AS
    SELECT s.*, d.ticker, 'isin' AS par
    FROM sup_ex s
    JOIN (SELECT DISTINCT ticker, isin FROM dia) t ON t.isin = s.isin
    ASOF JOIN dia d ON d.ticker = t.ticker AND d.data < s.data_ex
    WHERE s.data_ex IS NOT NULL AND d.isin = s.isin
""")
mult = con.sql("""SELECT count(*) FROM (SELECT raiz, rotulo, fator_b3, isin, isin_entregue, data_com
                  FROM sup_par GROUP BY ALL HAVING count(DISTINCT ticker) > 1)""").fetchall()[0][0]
if mult:
    raise ValueError("evento de quantidade pareado com mais de um ticker pelo ISIN")
print("Itens com data ex na base sem ticker do escopo com o mesmo ISIN no pregão anterior (classe fora do escopo, "
      "ticker já sem negociação ou ISIN diferente)")
con.sql("""
    SELECT rotulo, count(*) AS itens, count(DISTINCT isin) AS isins,
           string_agg(DISTINCT raiz, ' ' ORDER BY raiz) AS raizes
    FROM sup_ex s WHERE data_ex IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM sup_par p WHERE p.raiz = s.raiz AND p.isin = s.isin AND p.rotulo = s.rotulo
                      AND p.data_com = s.data_com AND p.fator_b3 = s.fator_b3 AND p.isin_entregue = s.isin_entregue)
    GROUP BY rotulo ORDER BY itens DESC
""").show(max_rows=50, max_width=250)
print("  (lista completa, só rótulos do domínio)")
con.sql("""
    SELECT s.raiz, s.rotulo, s.fator_b3, s.isin, s.isin_entregue, s.data_com, s.data_ex,
           (SELECT string_agg(DISTINCT a.ticker, ' ' ORDER BY a.ticker) FROM acoes a WHERE a.isin = s.isin) AS tickers_com_o_isin
    FROM sup_ex s WHERE data_ex IS NOT NULL AND rotulo IN ('GRUPAMENTO', 'DESDOBRAMENTO', 'BONIFICACAO')
      AND NOT EXISTS (SELECT 1 FROM sup_par p WHERE p.raiz = s.raiz AND p.isin = s.isin AND p.rotulo = s.rotulo
                      AND p.data_com = s.data_com AND p.fator_b3 = s.fator_b3 AND p.isin_entregue = s.isin_entregue)
    ORDER BY s.raiz, s.data_ex, s.isin
""").show(max_rows=300, max_width=250)

# Parte 1b (rodada 3, D28 C): item do domínio com ISIN sem ticker do escopo → ticker do escopo de mesma raiz e
# mesma classe. Classe do ISIN pelas posições 10–11 (OR, PR, PA, PB, PC; unit = caracteres 7–10 'CDAM'), conferida
# pelo `tipo` do ticker no último pregão antes da data ex (até 30 dias antes). Unit: só quando todos os
# componentes (classes da raiz com item no dia) têm o mesmo fator. Item igual a outro já pareado ao ticker
# (mesmo rótulo, fator e data ex; ex.: BBAS3 2024, mesmo desdobramento sob três ISINs) não entra duas vezes.
con.sql("""
    CREATE TEMP MACRO classe_isin(i) AS
    CASE WHEN substr(i, 7, 4) = 'CDAM' THEN 'UNT'
         ELSE CASE substr(i, 10, 2) WHEN 'OR' THEN 'ON' WHEN 'PR' THEN 'PN' WHEN 'PA' THEN 'PNA'
                                    WHEN 'PB' THEN 'PNB' WHEN 'PC' THEN 'PNC' END END
""")
con.sql("""
    CREATE TEMP TABLE sem_par AS
    SELECT s.*, classe_isin(s.isin) AS classe FROM sup_ex s
    WHERE s.data_ex IS NOT NULL AND s.rotulo IN ('GRUPAMENTO', 'DESDOBRAMENTO', 'BONIFICACAO')
      AND NOT EXISTS (SELECT 1 FROM sup_par p WHERE p.raiz = s.raiz AND p.isin = s.isin AND p.rotulo = s.rotulo
                      AND p.data_com = s.data_com AND p.fator_b3 = s.fator_b3 AND p.isin_entregue = s.isin_entregue)
""")
con.sql("""
    CREATE TEMP TABLE raiz_candidatos AS
    SELECT s.* EXCLUDE (classe), d.ticker, 'raiz' AS par
    FROM sem_par s
    JOIN (SELECT DISTINCT ticker FROM dia) t ON left(t.ticker, 4) = s.raiz
    ASOF JOIN dia d ON d.ticker = t.ticker AND d.data < s.data_ex
    WHERE s.classe <> 'UNT' AND d.tipo = s.classe AND d.data >= s.data_ex - INTERVAL 30 DAY
    UNION ALL
    SELECT c.raiz, c.arquivo, c.rotulo, c.fator_b3, d.isin, d.isin, c.data_aprovacao, c.data_com,
           'unit: componentes ' || c.classes || ' com o mesmo fator', c.data_ex, d.ticker, 'raiz_unit'
    FROM (SELECT raiz, rotulo, data_com, data_ex, min(arquivo) AS arquivo, min(data_aprovacao) AS data_aprovacao,
                 min(fator_b3) AS fator_b3, count(DISTINCT fator_b3) AS fatores,
                 count(DISTINCT classe_isin(isin)) AS n_classes,
                 string_agg(DISTINCT classe_isin(isin), '+' ORDER BY classe_isin(isin)) AS classes
          FROM sup_ex
          WHERE data_ex IS NOT NULL AND rotulo IN ('GRUPAMENTO', 'DESDOBRAMENTO', 'BONIFICACAO')
            AND isin = isin_entregue AND substr(isin, 7, 4) <> 'CDAM'
          GROUP BY raiz, rotulo, data_com, data_ex) c
    JOIN (SELECT DISTINCT ticker FROM dia) t ON left(t.ticker, 4) = c.raiz
    ASOF JOIN dia d ON d.ticker = t.ticker AND d.data < c.data_ex
    WHERE d.tipo = 'UNT' AND c.n_classes >= 2 AND c.fatores = 1 AND d.data >= c.data_ex - INTERVAL 30 DAY
""")
con.sql("""
    CREATE TEMP TABLE raiz_avaliados AS
    SELECT *,
           CASE WHEN EXISTS (SELECT 1 FROM sup_par p WHERE p.ticker = c.ticker AND p.rotulo = c.rotulo
                             AND p.fator_b3 = c.fator_b3 AND p.data_ex = c.data_ex)
                     THEN 'igual a item já pareado pelo ISIN do ticker'
                WHEN row_number() OVER (PARTITION BY ticker, rotulo, fator_b3, data_ex ORDER BY par, isin) > 1
                     THEN 'igual a outro item atribuído pela raiz'
           END AS descarte
    FROM raiz_candidatos c
""")
print("\nParte 1b (D28 C) — itens sem ticker do escopo pelo ISIN, atribuídos pela raiz e classe")
con.sql("""
    SELECT ticker, par, rotulo, fator_b3, isin, data_ex, coalesce(descarte, 'atribuído') AS resultado, observacao_b3
    FROM raiz_avaliados ORDER BY ticker, data_ex, rotulo, isin
""").show(max_rows=100, max_width=250)
print("Itens sem ticker do escopo (lista de 44 da rodada 2) que continuam sem ticker:",
      con.sql("""SELECT count(*) FROM sem_par s WHERE NOT EXISTS (SELECT 1 FROM raiz_candidatos c
                  WHERE c.raiz = s.raiz AND c.rotulo = s.rotulo AND c.data_com = s.data_com AND c.isin = s.isin)
                  AND NOT EXISTS (SELECT 1 FROM raiz_candidatos c WHERE c.par = 'raiz_unit' AND c.raiz = s.raiz
                  AND c.rotulo = s.rotulo AND c.data_com = s.data_com)""").fetchall()[0][0])
mult = con.sql("""SELECT count(*) FROM (SELECT raiz, rotulo, fator_b3, isin, data_com FROM raiz_avaliados
                  WHERE descarte IS NULL AND par = 'raiz' GROUP BY ALL HAVING count(DISTINCT ticker) > 1)""").fetchall()[0][0]
if mult:
    raise ValueError("Parte 1b: item atribuído pela raiz a mais de um ticker")
bbas = con.sql("""SELECT count(*) FROM sup_par WHERE ticker = 'BBAS3' AND rotulo = 'DESDOBRAMENTO'
                  AND data_ex = DATE '2024-04-16'""").fetchall()[0][0]
print(f"Esperado da especificação: desdobramento da BBAS3 em 16/04/2024 em `eventos`; itens pareados pelo ISIN do "
      f"próprio ticker: {bbas} (os itens dos ISINs A04OR8 e A05OR5 são o mesmo evento)")
if bbas != 1:
    raise SystemExit("BBAS3 16/04/2024: desdobramento ausente ou repetido: parar e relatar")

# Itens idênticos (mesmo ticker, ISIN, rótulo, fator, ativo entregue e data com) vindos da mesma raiz ou de
# raízes diferentes: um só; prefere a raiz do próprio ticker
con.sql("""
    CREATE TEMP TABLE sup_todos AS
    SELECT * FROM sup_par UNION ALL SELECT * EXCLUDE (descarte) FROM raiz_avaliados WHERE descarte IS NULL
""")
con.sql("""
    CREATE TEMP TABLE sup_unico AS
    SELECT * EXCLUDE (ordem, copias) FROM (
        SELECT *, row_number() OVER g AS ordem, count(*) OVER (PARTITION BY ticker, isin, rotulo, fator_b3, isin_entregue, data_com) AS copias
        FROM sup_todos
        WINDOW g AS (PARTITION BY ticker, isin, rotulo, fator_b3, isin_entregue, data_com
                     ORDER BY raiz <> left(ticker, 4), raiz)
    ) WHERE ordem = 1
""")
print("Itens idênticos removidos (mesmo evento devolvido mais de uma vez):",
      con.sql("SELECT (SELECT count(*) FROM sup_todos) - (SELECT count(*) FROM sup_unico)").fetchall()[0][0])

# Itens da B3 (pelo ISIN e pela raiz) como base da Parte 1c
con.sql("""
    CREATE TEMP TABLE itens AS
    SELECT ticker, data_ex, raiz, rotulo, fator_b3, isin, isin_entregue, data_com, observacao_b3, par,
           'B3' AS fonte, NULL::DOUBLE AS fator_ext
    FROM sup_unico
""")

# Parte 1c (D26): inícios de marca G ou B (2010+) sem item de quantidade da B3 do ticker em até 5 pregões
con.sql(f"""
    CREATE TEMP TABLE marcas_gap AS
    SELECT m.ticker, m.data, any_value(m.n) AS n, string_agg(m.letra, '' ORDER BY m.letra) AS letras
    FROM marcas m
    WHERE m.inicio AND m.letra IN ('G', 'B') AND year(m.data) BETWEEN {ANO_MIN} AND {ANO_MAX}
      AND NOT EXISTS (SELECT 1 FROM itens i JOIN pregoes g ON g.data = i.data_ex
                      WHERE i.ticker = m.ticker AND i.rotulo IN ('GRUPAMENTO', 'DESDOBRAMENTO', 'BONIFICACAO')
                        AND abs(g.n - m.n) <= {JANELA_PREGOES})
    GROUP BY m.ticker, m.data
""")
tickers_gap = [t for (t,) in con.sql("SELECT DISTINCT ticker FROM marcas_gap ORDER BY ticker").fetchall()]
externos, sem_dm, sem_yf = [], [], []
for t in tickers_gap:
    dm = eventos_dm(t)
    if dm is None:
        sem_dm.append(t)
    yf = eventos_yf(t)
    if yf is None:
        sem_yf.append(t)
    externos += (dm or []) + (yf or [])
print(f"\nParte 1c (D26) — tickers com início de marca G/B sem item da B3: {len(tickers_gap)}; sem página no Dados de "
      f"Mercado: {len(sem_dm)} ({' '.join(sem_dm)}); desconhecidos do yfinance: {len(sem_yf)} ({' '.join(sem_yf)})")
con.sql("CREATE TEMP TABLE ext_bruto (ticker VARCHAR, fonte VARCHAR, evento VARCHAR, razao VARCHAR, data_ex DATE, fator DOUBLE)")
if externos:
    con.executemany("INSERT INTO ext_bruto VALUES (?, ?, ?, ?, ?, ?)", externos)
# Pregão de referência de cada evento externo (primeiro pregão a partir da data informada)
con.sql("""
    CREATE TEMP TABLE ext_par AS
    SELECT e.*, g.n AS n_ex FROM ext_bruto e
    ASOF JOIN pregoes g ON g.data >= e.data_ex
    WHERE year(e.data_ex) >= 2010
""")
# Conferência das convenções: eventos externos no mesmo dia (±1 pregão) de um item do domínio da B3 do ticker
print("Conferência das convenções de fator contra a B3 (mesmo ticker, ±1 pregão; fator B3 convertido)")
con.sql(f"""
    SELECT e.fonte, count(*) AS pares,
           count(*) FILTER (WHERE abs(ln(e.fator / q.fator_b3_convertido)) < 0.001) AS fator_igual,
           string_agg(CASE WHEN abs(ln(e.fator / q.fator_b3_convertido)) >= 0.001
                           THEN e.ticker || ' ' || e.data_ex || ' ' || e.razao || ' x ' || round(q.fator_b3_convertido, 4) END,
                      '; ' ORDER BY e.ticker, e.data_ex, e.razao, q.fator_b3_convertido) AS diferentes
    FROM ext_par e
    JOIN (SELECT i.ticker, g.n, CASE i.rotulo WHEN 'GRUPAMENTO' THEN i.fator_b3 ELSE 1 + i.fator_b3 / 100 END
                 AS fator_b3_convertido
          FROM itens i JOIN pregoes g ON g.data = i.data_ex
          WHERE i.rotulo IN ('GRUPAMENTO', 'DESDOBRAMENTO', 'BONIFICACAO')) q
      ON q.ticker = e.ticker AND abs(q.n - e.n_ex) <= {JANELA_EXTERNO}
    GROUP BY e.fonte ORDER BY e.fonte
""").show(max_width=300)
# Escolha por início de marca: o Dados de Mercado onde ele cobre; "cobre" = evento a até 1 pregão que não reprova
# na regra de preço (D25, com a razão do intervalo (s, t] do evento); senão o yfinance; sem nenhum que passe, fica
# o da fonte de maior prioridade, que vai para pendência na validação do dia
con.sql(f"""
    CREATE TEMP TABLE ext_cand AS
    SELECT DISTINCT m.ticker, m.data AS data_marca, e.fonte, e.evento, e.razao, e.data_ex, e.fator,
           (abs(ln(t.preco_por_acao / s.preco_por_acao * e.fator)) < abs(ln(t.preco_por_acao / s.preco_por_acao))
                AND abs(ln(t.preco_por_acao / s.preco_por_acao * e.fator)) <= {RESIDUO_MAX})
               OR abs(ln(t.preco_por_acao / s.preco_por_acao * e.fator)) <= {TOL_PRECO} AS passa_preco
    FROM ext_par e JOIN marcas_gap m ON m.ticker = e.ticker AND abs(m.n - e.n_ex) <= {JANELA_EXTERNO}
    ASOF LEFT JOIN dia t ON t.ticker = e.ticker AND t.data >= e.data_ex
    ASOF LEFT JOIN dia s ON s.ticker = e.ticker AND s.data < e.data_ex
""")
con.sql("""
    CREATE TEMP TABLE ext_escolhidos AS
    SELECT DISTINCT ticker, fonte, evento, razao, data_ex, fator FROM (
        SELECT *, min(CASE WHEN passa_preco THEN (fonte <> 'Dados de Mercado')::INT END) OVER m AS fonte_ok,
                  min((fonte <> 'Dados de Mercado')::INT) OVER m AS fonte_qualquer
        FROM ext_cand WINDOW m AS (PARTITION BY ticker, data_marca))
    WHERE (fonte <> 'Dados de Mercado')::INT = coalesce(fonte_ok, fonte_qualquer)
      AND (passa_preco OR fonte_ok IS NULL)
""")
print("Inícios de marca em que o evento do Dados de Mercado reprova no preço e o do yfinance é usado:",
      con.sql("""SELECT count(DISTINCT (ticker, data_marca)) FROM ext_cand c WHERE fonte = 'yfinance' AND passa_preco
                  AND EXISTS (SELECT 1 FROM ext_cand d WHERE d.ticker = c.ticker AND d.data_marca = c.data_marca
                              AND d.fonte = 'Dados de Mercado')
                  AND NOT EXISTS (SELECT 1 FROM ext_cand d WHERE d.ticker = c.ticker AND d.data_marca = c.data_marca
                                  AND d.fonte = 'Dados de Mercado' AND d.passa_preco)""").fetchall()[0][0])
print("Cobertura dos inícios de marca G/B sem item da B3 (por fonte que tem evento a até 1 pregão)")
con.sql(f"""
    SELECT year(m.data) AS ano, count(*) AS marcas,
           count(*) FILTER (WHERE EXISTS (SELECT 1 FROM ext_par e WHERE e.ticker = m.ticker AND e.fonte = 'Dados de Mercado'
                                          AND abs(m.n - e.n_ex) <= {JANELA_EXTERNO})) AS dados_de_mercado,
           count(*) FILTER (WHERE EXISTS (SELECT 1 FROM ext_par e WHERE e.ticker = m.ticker AND e.fonte = 'yfinance'
                                          AND abs(m.n - e.n_ex) <= {JANELA_EXTERNO})) AS yfinance,
           count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM ext_par e WHERE e.ticker = m.ticker
                                              AND abs(m.n - e.n_ex) <= {JANELA_EXTERNO})) AS nenhuma
    FROM marcas_gap m GROUP BY ano ORDER BY ano
""").show(max_rows=30)
# Eventos externos viram itens (rótulo pelo fator e pelo nome da fonte; o yfinance não distingue bonificação)
con.sql("""
    INSERT INTO itens
    SELECT ticker, data_ex, left(ticker, 4),
           CASE WHEN fator < 1 THEN 'GRUPAMENTO' WHEN evento LIKE 'Bonifica%' THEN 'BONIFICACAO' ELSE 'DESDOBRAMENTO' END,
           NULL, NULL, NULL, NULL, fonte || ': ' || evento || ' ' || razao, 'externo', fonte, fator
    FROM ext_escolhidos
""")

# Conversão do fator; preço no intervalo (s, t] (D28 A): t = primeiro pregão do ticker a partir da data ex,
# s = último pregão antes dela; marca no pregão t
con.sql("""
    CREATE TEMP TABLE quantidade AS
    SELECT u.ticker, u.data_ex, u.raiz, u.rotulo, u.fator_b3, u.isin, u.isin_entregue, u.data_com, u.observacao_b3,
           u.par, u.fonte,
           CASE WHEN u.par = 'externo' THEN 'dominio'
                WHEN u.rotulo NOT IN ('GRUPAMENTO', 'DESDOBRAMENTO', 'BONIFICACAO') THEN 'fora_do_dominio'
                WHEN u.rotulo = 'BONIFICACAO' AND substr(u.isin_entregue, 7, 5) <> substr(u.isin, 7, 5)
                     THEN 'bonificacao_outra_classe'
                ELSE 'dominio' END AS situacao,
           CASE u.rotulo WHEN 'GRUPAMENTO' THEN 'grupamento' WHEN 'DESDOBRAMENTO' THEN 'desdobramento'
                         WHEN 'BONIFICACAO' THEN 'bonificacao' END AS tipo_evento,
           coalesce(u.fator_ext, CASE u.rotulo WHEN 'GRUPAMENTO' THEN u.fator_b3
                                               WHEN 'DESDOBRAMENTO' THEN 1 + u.fator_b3 / 100
                                               WHEN 'BONIFICACAO' THEN 1 + u.fator_b3 / 100 END) AS fator_quantidade,
           t.data AS data_preco, s.data AS data_anterior, coalesce(t.data, u.data_ex) AS dia_chave,
           t.preco_por_acao AS ppa_t, s.preco_por_acao AS ppa_s,
           t.preco_por_acao / s.preco_por_acao AS razao_ppa, t.fechamento / s.fechamento AS razao_fech,
           t.mudou_fatcot, CASE WHEN t.data = u.data_ex THEN t.marca END AS marca_no_dia,
           coalesce(t.data = u.data_ex, FALSE) AS pregao_no_dia,
           u.raiz <> left(u.ticker, 4) AS outra_raiz
    FROM itens u
    ASOF LEFT JOIN dia t ON t.ticker = u.ticker AND t.data >= u.data_ex
    ASOF LEFT JOIN dia s ON s.ticker = u.ticker AND s.data < u.data_ex
""")
# Validação por dia (D25): fator do dia = produto dos fatores do domínio do ticker no mesmo intervalo (s, t]
con.sql(f"""
    CREATE TEMP TABLE dia_q AS
    SELECT *,
           CASE WHEN razao_ppa IS NULL THEN 'sem_preco'
                WHEN abs(ln(fator_dia)) < 1e-9 THEN 'fator_liquido_1'
                WHEN residuo <= {TOL_PRECO} THEN 'preco'
                WHEN residuo < abs(ln(razao_ppa)) AND residuo <= {RESIDUO_MAX} THEN 'preco_aproximado'
                ELSE 'pendencia' END AS categoria
    FROM (SELECT ticker, dia_chave, min(data_ex) AS data_ex, product(fator_quantidade) AS fator_dia, count(*) AS itens,
                 string_agg(CASE WHEN par = 'externo' THEN observacao_b3 ELSE rotulo || ' ' || fator_b3 END, ' + '
                            ORDER BY rotulo DESC, fator_quantidade, fonte) AS fatores,
                 any_value(razao_ppa) AS razao_ppa, any_value(razao_fech) AS razao_fech,
                 any_value(data_anterior) AS data_anterior, any_value(data_preco) AS data_preco,
                 abs(ln(any_value(razao_ppa) * product(fator_quantidade))) AS residuo,
                 abs(ln(any_value(razao_fech) * product(fator_quantidade))) AS residuo_fech
          FROM quantidade WHERE situacao = 'dominio' GROUP BY ticker, dia_chave)
""")
con.sql(f"""
    CREATE TEMP TABLE quantidade_v AS
    SELECT q.*, z.fator_dia, z.fatores, z.residuo, z.residuo_fech, z.categoria,
           q.razao_ppa * z.fator_dia AS razao_corrigida,
           EXISTS (SELECT 1 FROM marcas m WHERE m.ticker = q.ticker AND m.data = q.data_preco AND m.inicio
                     AND m.letra IN (CASE q.tipo_evento WHEN 'grupamento' THEN 'G' WHEN 'bonificacao' THEN 'B' END,
                                     CASE q.tipo_evento WHEN 'desdobramento' THEN 'G' END,
                                     CASE q.tipo_evento WHEN 'desdobramento' THEN 'B' END)) AS ok_marca
    FROM quantidade q LEFT JOIN dia_q z USING (ticker, dia_chave)
""")
print("Parte 1c — eventos de outra fonte escolhidos e resultado da regra de preço (D25)")
con.sql("""
    SELECT q.ticker, q.data_ex, q.fonte, q.observacao_b3 AS evento, round(q.fator_quantidade, 6) AS fator,
           z.fatores AS fatores_do_dia, round(z.razao_ppa, 4) AS razao_ppa, round(z.residuo, 4) AS residuo,
           z.categoria
    FROM quantidade q JOIN dia_q z USING (ticker, dia_chave) WHERE q.par = 'externo'
    ORDER BY q.ticker, q.data_ex, q.fonte
""").show(max_rows=300, max_width=250)
con.sql("""SELECT q.fonte, z.categoria, count(*) AS eventos FROM quantidade q JOIN dia_q z USING (ticker, dia_chave)
           WHERE q.par = 'externo' GROUP BY ALL ORDER BY ALL""").show()
print("Parte 1a (D28 A) — itens sem pregão do ticker na data ex (preço entre o último pregão antes e o primeiro depois)")
con.sql("""
    SELECT ticker, data_ex, rotulo, fator_quantidade, data_anterior, data_preco, round(razao_ppa, 4) AS razao_ppa,
           round(residuo, 4) AS residuo, round(abs(ln(razao_ppa)), 4) AS residuo_sem_fator, categoria
    FROM quantidade_v WHERE situacao = 'dominio' AND NOT pregao_no_dia ORDER BY ticker, data_ex
""").show(max_width=250)

print(f"\nValidação pelo preço por dia (preco_por_acao; resíduo = |ln(razão × fator do dia)|; preco: resíduo <= "
      f"{TOL_PRECO}; preco_aproximado: resíduo > {TOL_PRECO} e < |ln(razão)|; pendencia: resíduo >= |ln(razão)|, fica "
      "fora de `eventos`); itens do domínio, data ex 2010+, ticker do escopo")
con.sql("""
    SELECT rotulo, count(*) AS itens, count(*) FILTER (WHERE categoria = 'preco') AS preco,
           count(*) FILTER (WHERE categoria = 'preco_aproximado') AS preco_aproximado,
           count(*) FILTER (WHERE categoria = 'fator_liquido_1') AS fator_liquido_1,
           count(*) FILTER (WHERE categoria = 'pendencia') AS pendencia,
           count(*) FILTER (WHERE categoria = 'sem_preco') AS sem_preco,
           count(*) FILTER (WHERE ok_marca) AS com_inicio_de_marca,
           count(*) FILTER (WHERE outra_raiz) AS achados_por_outra_raiz
    FROM quantidade_v WHERE situacao = 'dominio' GROUP BY rotulo ORDER BY rotulo
""").show(max_width=200)
print("Dias com mais de um item do domínio (fator do dia = produto)")
con.sql("""
    SELECT ticker, data_ex, fatores, round(fator_dia, 6) AS fator_dia, round(razao_ppa, 4) AS razao_ppa,
           round(residuo, 4) AS residuo, categoria
    FROM dia_q WHERE itens > 1 ORDER BY ticker, data_ex
""").show(max_rows=100, max_width=250)
print("preco_aproximado, pendencia e sem_preco (razão = preco_por_acao ex / anterior; resíduo = |ln(razão × fator do dia)|)")
con.sql("""
    SELECT categoria, ticker, data_ex, fatores, round(fator_dia, 6) AS fator_dia, round(razao_ppa, 4) AS razao_ppa,
           round(residuo, 4) AS residuo, round(abs(ln(razao_ppa)), 4) AS residuo_sem_fator
    FROM dia_q WHERE categoria IN ('preco_aproximado', 'pendencia', 'sem_preco') ORDER BY categoria, ticker, data_ex
""").show(max_rows=100, max_width=250)
print("Conversão: exemplos por rótulo (fator da B3 → fator_quantidade; razão de preço; razão corrigida)")
con.sql("""
    SELECT rotulo, ticker, data_ex, fator_b3, fator_quantidade, round(razao_ppa, 4) AS razao_ppa,
           round(razao_corrigida, 4) AS razao_corrigida_dia, categoria, marca_no_dia
    FROM (SELECT *, row_number() OVER (PARTITION BY rotulo ORDER BY ticker, data_ex) AS k
          FROM quantidade_v WHERE situacao = 'dominio' AND razao_ppa IS NOT NULL)
    WHERE k <= 5 ORDER BY rotulo, k
""").show(max_rows=50, max_width=200)
print("Marca do ESPECI no dia ex, por rótulo")
con.sql("""
    PIVOT (SELECT rotulo, CASE WHEN NOT pregao_no_dia THEN '(sem pregão)' WHEN marca_no_dia = '' THEN '(sem marca)'
                               ELSE marca_no_dia END AS marca FROM quantidade_v WHERE situacao = 'dominio')
    ON rotulo USING count(*) GROUP BY marca ORDER BY marca
""").show(max_rows=50)
print("Ajuste 1 do responsável: eventos em dia de mudou_fatcot (validação com preco_por_acao e, para contraste, "
      "com fechamento)")
con.sql("""
    SELECT ticker, data_ex, rotulo, fator_quantidade, round(razao_ppa, 6) AS razao_ppa, round(residuo, 4) AS residuo,
           categoria, round(razao_fech, 6) AS razao_fechamento, round(residuo_fech, 4) AS residuo_fechamento,
           marca_no_dia
    FROM quantidade_v WHERE mudou_fatcot ORDER BY data_ex, ticker
""").show(max_width=200)

print("Rótulos fora do domínio e bonificações que entregam outra classe (não entram em `eventos`)")
con.sql("""
    SELECT ticker, data_ex, rotulo, situacao, fator_b3, isin, isin_entregue, round(razao_ppa, 4) AS razao_ppa,
           marca_no_dia, raiz
    FROM quantidade_v WHERE situacao <> 'dominio' ORDER BY ticker, data_ex, rotulo, isin_entregue, fator_b3
""").show(max_rows=300, max_width=250)

# ============================ Parte 2 (rodada 3) — entregas de ações (D27) ============================
# CIS RED CAP, REST CAP ACOES e bonificação em ação de outra classe: quantidade recebida por ação antiga =
# fator/100 (hipótese validada pelo preço); preço = preco_por_acao da ação entregue (qualquer tipo de `cotacoes`,
# inclusive BDR) no primeiro pregão a partir da data ex. Perna com o ISIN do próprio ticker e fator 100: só anotada.
con.sql("""
    CREATE TEMP TABLE entregue_preco AS
    SELECT isin, data, min(ticker) AS ticker, arg_min(fechamento / fatcot, ticker) AS preco
    FROM cotacoes WHERE isin IN (SELECT isin_entregue FROM quantidade) GROUP BY isin, data
""")
con.sql("""
    CREATE TEMP TABLE entregas AS
    SELECT q.ticker, q.data_ex, q.dia_chave, q.rotulo, q.situacao, q.fator_b3, q.isin, q.isin_entregue,
           q.ppa_t, q.ppa_s, q.razao_ppa,
           q.isin_entregue = q.isin AND q.fator_b3 = 100 AS perna_propria,
           q.fator_b3 / 100 AS quantidade_recebida,
           c.ticker AS ticker_entregue, c.data AS data_entregue, c.preco AS preco_entregue,
           q.fator_b3 / 100 * c.preco AS valor
    FROM quantidade q
    ASOF LEFT JOIN entregue_preco c ON c.isin = q.isin_entregue AND c.data >= q.data_ex
    WHERE q.rotulo IN ('CIS RED CAP', 'REST CAP ACOES') OR q.situacao = 'bonificacao_outra_classe'
""")
# Validação por dia (D25 aplicada a (P_t × F + D) / P_s), F = fator do dia dos eventos de quantidade (1 se não houver)
con.sql(f"""
    CREATE TEMP TABLE entregas_dia AS
    SELECT *,
           CASE WHEN NOT tem_preco OR razao_ppa IS NULL THEN 'pendencia'
                WHEN residuo <= {TOL_PRECO} THEN 'preco'
                WHEN residuo < residuo_sem_d AND residuo <= {RESIDUO_MAX} THEN 'preco_aproximado'
                ELSE 'pendencia' END AS categoria,
           CASE WHEN razao_ppa IS NULL THEN 'sem pregão do ticker no intervalo da data ex'
                WHEN NOT tem_preco THEN 'ação entregue sem pregão no COTAHIST a partir da data ex'
                WHEN residuo > {TOL_PRECO} AND (residuo >= residuo_sem_d OR residuo > {RESIDUO_MAX})
                     THEN 'preço não confirma o valor entregue' END
               AS motivo
    FROM (SELECT e.ticker, e.dia_chave, min(e.data_ex) AS data_ex, count(*) AS itens,
                 bool_and(e.preco_entregue IS NOT NULL) AS tem_preco, sum(e.valor) AS d,
                 any_value(e.razao_ppa) AS razao_ppa, coalesce(any_value(z.fator_dia), 1) AS fator_dia,
                 abs(ln((any_value(e.ppa_t) * coalesce(any_value(z.fator_dia), 1) + sum(e.valor)) / any_value(e.ppa_s)))
                     AS residuo,
                 abs(ln(any_value(e.razao_ppa) * coalesce(any_value(z.fator_dia), 1))) AS residuo_sem_d,
                 string_agg(e.rotulo || ' ' || coalesce(e.ticker_entregue, e.isin_entregue) || ' ' || e.quantidade_recebida,
                            ' + ' ORDER BY e.isin_entregue) AS entregas
          FROM entregas e LEFT JOIN dia_q z USING (ticker, dia_chave)
          WHERE NOT e.perna_propria GROUP BY e.ticker, e.dia_chave)
""")
print("\nParte 2 (D27) — entregas de ações (quantidade = fator/100; resíduo = |ln((P_t·F + D)/P_s)|)")
con.sql("""
    SELECT ticker, data_ex, entregas, round(d, 4) AS d, round(razao_ppa, 4) AS razao_ppa, round(residuo, 4) AS residuo,
           round(residuo_sem_d, 4) AS residuo_sem_d, categoria, motivo
    FROM entregas_dia ORDER BY ticker, data_ex
""").show(max_rows=100, max_width=300)
print("Parte 2 — pernas com o ISIN do próprio ticker e fator 100 (sem mudança de quantidade; sem evento)")
con.sql("""SELECT ticker, data_ex, rotulo, isin FROM entregas WHERE perna_propria ORDER BY ticker, data_ex""").show(max_width=200)

# ============================ Parte 3a — nome do sucessor ============================
# Registros da B3 (inclusive os sem ticker) consultados por nome do sucessor ou de mesmo CNPJ, com data ex no
# período do ticker antigo e classe igual à do COTAHIST no pregão anterior mais próximo
con.sql("""
    CREATE TEMP TABLE achados_3a_bruto AS
    SELECT c.ticker_antigo, c.* EXCLUDE (ticker_antigo, ticker, classe_cotahist), c.ticker AS ticker_atual
    FROM (SELECT n.ticker AS ticker_antigo, b.*, a.tipo AS classe_cotahist
          FROM nomes_3a n
          JOIN proventos_b3 b ON b.nome_cotahist = n.nome
          JOIN periodo p ON p.ticker = n.ticker AND b.data_ex BETWEEN p.primeiro AND p.ultimo
          ASOF JOIN acoes a ON a.ticker = n.ticker AND a.data <= b.data_ex) c
    WHERE c.classe_b3 = c.classe_cotahist
""")
# Registro já pareado com outro ticker seria inserido duas vezes; só os tickers que só são listados podem tê-lo
print("\nParte 3a — registros achados já pareados com outro ticker em proventos_b3")
con.sql("""SELECT ticker_antigo, nome_consultado, ticker_atual, classe_b3, tipo_b3, data_ex, valor
           FROM achados_3a_bruto WHERE ticker_atual IS NOT NULL ORDER BY ALL""").show(max_width=200)
if con.sql("SELECT count(*) FROM achados_3a_bruto WHERE ticker_atual IS NOT NULL AND ticker_antigo NOT IN (SELECT unnest(?))",
           params=[sorted(SO_LISTAR_3A)]).fetchall()[0][0]:
    raise ValueError("Parte 3a: registro achado pelo nome do sucessor já está pareado com outro ticker")
# D23 entre consultas: o mesmo registro vindo de dois nomes fica uma vez (o primeiro nome em ordem alfabética)
con.sql("""
    CREATE TEMP TABLE achados_3a AS
    SELECT * FROM achados_3a_bruto
    QUALIFY nome_cotahist = min(nome_cotahist) OVER (PARTITION BY ticker_antigo, classe_b3, tipo_b3, data_com, valor)
""")
con.sql("""
    CREATE TEMP TABLE achados_3a_par AS
    SELECT a.*, m.inicio IS TRUE AS inicio_marca
    FROM achados_3a a
    LEFT JOIN marcas m ON m.ticker = a.ticker_antigo AND m.data = a.data_ex
         AND m.letra = CASE a.tipo_b3 WHEN 'DIVIDENDO' THEN 'D' WHEN 'JRS CAP PROPRIO' THEN 'J' END
""")
con.sql(f"""
    CREATE TEMP TABLE tabela_3a AS
    SELECT t.ticker,
           (SELECT string_agg(nome, ' | ' ORDER BY nome) FROM nomes_3a n WHERE n.ticker = t.ticker) AS nomes_consultados,
           (SELECT count(*) FROM achados_3a a WHERE a.ticker_antigo = t.ticker) AS registros,
           (SELECT count(DISTINCT (data_ex, tipo_b3)) FROM achados_3a_par a
             WHERE a.ticker_antigo = t.ticker AND tipo_b3 IN ('DIVIDENDO', 'JRS CAP PROPRIO')) AS grupos_div_jcp,
           (SELECT count(DISTINCT (data_ex, tipo_b3)) FROM achados_3a_par a
             WHERE a.ticker_antigo = t.ticker AND inicio_marca) AS grupos_com_inicio_de_marca,
           (SELECT count(*) FROM marcas m WHERE m.ticker = t.ticker AND m.inicio AND m.letra IN ('D', 'J')
              AND year(m.data) BETWEEN {ANO_MIN} AND {ANO_MAX}) AS inicios_marca_dj,
           (SELECT count(*) FROM marcas m WHERE m.ticker = t.ticker AND m.inicio AND m.letra IN ('D', 'J')
              AND year(m.data) BETWEEN {ANO_MIN} AND {ANO_MAX}
              AND EXISTS (SELECT 1 FROM achados_3a a JOIN pregoes g ON g.data = a.data_ex
                          WHERE a.ticker_antigo = m.ticker AND abs(g.n - m.n) <= {JANELA_PREGOES}
                            AND m.letra = CASE a.tipo_b3 WHEN 'DIVIDENDO' THEN 'D' WHEN 'JRS CAP PROPRIO' THEN 'J' END)
           ) AS inicios_com_registro_ate_5_pregoes,
           t.ticker IN ({', '.join(f"'{t}'" for t in SO_LISTAR_3A)}) AS so_listado
    FROM (SELECT unnest({LACUNAS_3A}) AS ticker) t
""")
print("\nParte 3a — registros achados pelo nome do sucessor ou de mesmo CNPJ (taxa = grupos com início de marca / "
      "grupos; inversa = inícios de marca D/J com registro do tipo em até 5 pregões)")
con.sql("""
    SELECT ticker, coalesce(nomes_consultados, '(sem nome a consultar pelas fontes autorizadas)') AS nomes_consultados,
           registros, grupos_div_jcp, grupos_com_inicio_de_marca,
           round(grupos_com_inicio_de_marca / nullif(grupos_div_jcp, 0), 3) AS taxa,
           inicios_marca_dj, inicios_com_registro_ate_5_pregoes,
           round(inicios_com_registro_ate_5_pregoes / nullif(inicios_marca_dj, 0), 3) AS taxa_inversa, so_listado
    FROM tabela_3a ORDER BY ticker
""").show(max_rows=30, max_width=250)
for ticker, nomes in sorted(so_proprios.items()):
    print(f"  {ticker}: sucessor ou mesmo CNPJ só com o nome do próprio ticker ({', '.join(nomes)}), já consultado")
print("Parte 3a — registros achados")
con.sql("""
    SELECT ticker_antigo, nome_consultado, classe_b3, tipo_b3, data_com, data_ex, valor, repetido, inicio_marca
    FROM achados_3a_par ORDER BY ticker_antigo, data_ex, tipo_b3, valor
""").show(max_rows=500, max_width=250)

# ============================ Parte 3b — JBSS3 no Dados de Mercado ============================
print("\nParte 3b — JBSS3: Dados de Mercado", "baixado" if tem_dm_jbss3 else "INDISPONÍVEL")
print("Anos com dados nas duas fontes (ano da data com; B3 = registros pareados com JBSS3)")
con.sql("""
    WITH b3 AS (SELECT year(data_com) AS ano, count(*) AS n_b3, sum(valor) AS total_b3
                FROM proventos_b3 WHERE ticker = 'JBSS3' GROUP BY ano),
         dm AS (SELECT year(data_com) AS ano, count(*) AS n_dm, sum(valor) AS total_dm,
                       bool_or(ajustado) AS ajustado FROM dm_jbss3 GROUP BY ano)
    SELECT ano, n_b3, round(total_b3, 6) AS total_b3, n_dm, round(total_dm, 6) AS total_dm, ajustado
    FROM b3 JOIN dm USING (ano) ORDER BY ano
""").show(max_rows=40)
eventos_jbs = con.sql(f"""SELECT count(*) FROM quantidade_v WHERE ticker = 'JBSS3' AND situacao = 'dominio'
                         AND year(data_ex) BETWEEN 2020 AND 2025""").fetchall()[0][0]
ajustado_jbs = con.sql("SELECT count(*) FROM dm_jbss3 WHERE ajustado AND year(data_com) BETWEEN 2020 AND 2025").fetchall()[0][0]
print(f"Eventos de quantidade da JBSS3 em 2020–2025: {eventos_jbs}; valores do Dados de Mercado com '*' "
      f"em 2020–2025: {ajustado_jbs}")
if eventos_jbs and ajustado_jbs:
    raise SystemExit("JBSS3: houve evento de quantidade e o Dados de Mercado ajustou os valores: parar e relatar")
con.sql(f"""
    CREATE TEMP TABLE marcas_jbss3 AS
    SELECT data AS data_ex FROM read_csv('{PENDENCIAS}')
    WHERE ticker = 'JBSS3' AND letra = 'D' AND year(data) BETWEEN 2020 AND 2025
""")
n_marcas = con.sql("SELECT count(*) FROM marcas_jbss3").fetchall()[0][0]
print(f"Marcas de dividendo pendentes da JBSS3 em 2020–2025: {n_marcas} (esperado {JBSS3_MARCAS_ESPERADAS})")
if n_marcas != JBSS3_MARCAS_ESPERADAS:
    raise ValueError("JBSS3: número de marcas pendentes diferente do esperado")
con.sql("""
    CREATE TEMP TABLE par_jbss3 AS
    SELECT m.data_ex, count(d.valor) AS linhas_dm, any_value(d.valor) AS valor, any_value(d.data_com) AS data_com,
           (SELECT min(data) FROM pregoes WHERE data > any_value(d.data_com)) AS ex_pela_regra
    FROM marcas_jbss3 m LEFT JOIN dm_jbss3 d ON d.data_ex = m.data_ex AND d.tipo = 'Dividendo'
    GROUP BY m.data_ex
""")
print("Marcas × dividendos do Dados de Mercado com a mesma data ex (par único entra em `eventos`)")
con.sql("SELECT *, ex_pela_regra = data_ex AS ex_confere FROM par_jbss3 ORDER BY data_ex").show(max_width=200)

# ============================ Parte 4 — tabela `eventos` ============================
# Proventos da B3 com ticker. Repetidos entre consultas: fica a cópia do primeiro nome em ordem alfabética;
# as cópias de cada nome devem ter o mesmo número de registros
con.sql("""
    CREATE TEMP TABLE prov AS
    SELECT p.*, p.nome_cotahist = min(p.nome_cotahist) OVER g AS usado,
           count(*) OVER (PARTITION BY p.ticker, p.classe_b3, p.tipo_b3, p.data_com, p.valor, p.nome_cotahist) AS copias
    FROM proventos_b3 p WHERE p.ticker IS NOT NULL
    WINDOW g AS (PARTITION BY p.ticker, p.classe_b3, p.tipo_b3, p.data_com, p.valor)
""")
if con.sql("""SELECT count(*) FROM (SELECT ticker, classe_b3, tipo_b3, data_com, valor FROM prov
              GROUP BY ALL HAVING count(DISTINCT copias) > 1)""").fetchall()[0][0]:
    raise ValueError("repetido entre consultas com número de cópias diferente entre os nomes")
con.sql("""
    CREATE TEMP TABLE exclusoes AS
    SELECT id_registro, ticker, nome_consultado, data_ex, tipo_b3, valor,
           'repetido entre consultas (fica a cópia de ' || min(nome_cotahist) OVER g || ')' AS motivo
    FROM prov
    WINDOW g AS (PARTITION BY ticker, classe_b3, tipo_b3, data_com, valor)
    QUALIFY NOT usado
""")

con.sql("""
    CREATE TEMP TABLE linhas AS
    SELECT ticker, data_ex, 'provento' AS tipo_evento, NULL::DOUBLE AS fator_quantidade, valor AS valor_dinheiro,
           tipo_b3, 'B3' AS fonte, 'nome_proprio' AS origem,
           CASE WHEN pareamento IN ('inicio_exato', 'inicio_composto') THEN 'marca' ELSE 'nenhuma' END AS confirmacao,
           repetido AS parcela_repetida,
           CASE WHEN repetido AND ((nome_cotahist = 'CEMIG' AND tipo_b3 = 'JRS CAP PROPRIO' AND year(data_com) = 2018)
                                   OR (nome_cotahist IN ('ELETROBRAS', 'AXIA ENERGIA') AND data_com = DATE '2010-01-29'))
                THEN '""" + OBS_D23 + """' END AS nota, id_registro AS ordem
    FROM prov WHERE usado
    UNION ALL
    SELECT ticker_antigo, data_ex, 'provento', NULL, valor, tipo_b3, 'B3', 'nome_sucessor',
           CASE WHEN inicio_marca THEN 'marca' ELSE 'nenhuma' END, repetido,
           'nome consultado: ' || nome_consultado, id_registro
    FROM achados_3a_par WHERE ticker_antigo NOT IN (SELECT unnest(?))
    UNION ALL
    SELECT 'JBSS3', data_ex, 'provento', NULL, valor, 'DIVIDENDO', 'Dados de Mercado', 'outra_fonte', 'marca', FALSE,
           'marca de dividendo sem provento da B3; data com ' || strftime(data_com, '%d/%m/%Y'), 0
    FROM par_jbss3 WHERE linhas_dm = 1
    UNION ALL
    SELECT ticker, data_ex, tipo_evento, fator_quantidade, NULL, NULL, fonte,
           CASE WHEN par = 'externo' THEN 'outra_fonte' WHEN outra_raiz THEN 'nome_sucessor' ELSE 'nome_proprio' END,
           CASE WHEN categoria = 'preco' AND ok_marca THEN 'marca_e_preco' ELSE categoria END,
           FALSE,
           concat_ws('; ', CASE WHEN outra_raiz THEN 'raiz consultada: ' || raiz END,
                     CASE WHEN par = 'raiz' THEN 'atribuído pela raiz e classe (ISIN ' || isin || ' sem ticker no escopo)' END,
                     CASE WHEN par = 'raiz_unit' THEN 'atribuído à unit (' || observacao_b3 || ')' END,
                     CASE WHEN par = 'externo' THEN observacao_b3 END,
                     CASE WHEN par = 'externo' AND fonte = 'yfinance' AND fator_quantidade > 1
                          THEN 'fonte não distingue desdobramento de bonificação' END,
                     CASE WHEN NOT pregao_no_dia THEN 'sem pregão do ticker na data ex; preço de '
                          || strftime(data_anterior, '%d/%m/%Y') || ' a ' || strftime(data_preco, '%d/%m/%Y') END,
                     CASE WHEN mudou_fatcot THEN 'dia de mudança do fator de cotação' END), 0
    FROM quantidade_v WHERE situacao = 'dominio' AND categoria NOT IN ('pendencia', 'sem_preco')
    UNION ALL
    SELECT e.ticker, e.data_ex, 'provento', NULL, e.valor, 'ACOES ENTREGUES', 'B3', 'nome_proprio', z.categoria, FALSE,
           'ação entregue ' || e.ticker_entregue || ': ' || round(e.quantidade_recebida, 8) || ' por ação a '
           || round(e.preco_entregue, 6) || ' em ' || strftime(e.data_entregue, '%d/%m/%Y') || ' (' || e.rotulo || ')', 0
    FROM entregas e JOIN entregas_dia z USING (ticker, dia_chave)
    WHERE NOT e.perna_propria AND z.categoria <> 'pendencia'
""", params=[sorted(SO_LISTAR_3A)])

con.sql(f"""
    CREATE OR REPLACE TABLE eventos AS
    SELECT l.ticker, l.data_ex, l.tipo_evento,
           row_number() OVER (PARTITION BY l.ticker, l.data_ex, l.tipo_evento
                              ORDER BY l.tipo_b3, l.valor_dinheiro, l.fator_quantidade, l.ordem, l.origem) AS seq,
           l.fator_quantidade, l.valor_dinheiro,
           CASE l.tipo_b3 WHEN 'DIVIDENDO' THEN 'dividendo' WHEN 'JRS CAP PROPRIO' THEN 'jcp'
                          WHEN 'RENDIMENTO' THEN 'rendimento' WHEN 'REST CAP DIN' THEN 'restituicao_capital'
                          WHEN 'ACOES ENTREGUES' THEN 'acoes_entregues' END
               AS tipo_provento,
           l.fonte, l.origem, l.confirmacao, l.parcela_repetida,
           nullif(concat_ws('; ', nullif(l.nota, ''),
                            CASE WHEN EXISTS (SELECT 1 FROM linhas o WHERE o.ticker = l.ticker AND o.data_ex = l.data_ex
                                              AND (o.tipo_evento = 'provento') <> (l.tipo_evento = 'provento'))
                                 THEN '{OBS_MESMO_DIA}' END), '') AS observacao
    FROM linhas l
    ORDER BY l.ticker, l.data_ex, l.tipo_evento, seq
""")

# ============================ Parte 5 — checagens ============================
print("\nParte 5 — checagens")
n, chaves, fator_ruim, valor_ruim, dominio_ruim, nulos = con.sql("""
    SELECT count(*), count(DISTINCT (ticker, data_ex, tipo_evento, seq)),
           count(*) FILTER (WHERE tipo_evento <> 'provento' AND NOT coalesce(fator_quantidade > 0, FALSE)),
           count(*) FILTER (WHERE tipo_evento = 'provento' AND NOT coalesce(valor_dinheiro >= 0, FALSE)),
           count(*) FILTER (WHERE tipo_evento NOT IN ('grupamento', 'desdobramento', 'bonificacao', 'provento')
                              OR (tipo_evento = 'provento') <> (tipo_provento IS NOT NULL)
                              OR origem NOT IN ('nome_proprio', 'nome_sucessor', 'outra_fonte')
                              OR confirmacao NOT IN ('marca', 'preco', 'marca_e_preco', 'preco_aproximado',
                                                     'fator_liquido_1', 'nenhuma')),
           count(*) FILTER (WHERE ticker IS NULL OR data_ex IS NULL OR fonte IS NULL OR parcela_repetida IS NULL
                              OR year(data_ex) < 2010)
    FROM eventos
""").fetchall()[0]
print(f"Checagem 1: linhas={n} chaves={chaves} fator<=0 ou vazio={fator_ruim} valor<0 ou vazio={valor_ruim} "
      f"fora do domínio={dominio_ruim} nulos ou data ex<2010={nulos}")
if n != chaves or fator_ruim or valor_ruim or dominio_ruim or nulos:
    raise ValueError("eventos: checagem 1 reprovada")

total, usadas, excluidas = con.sql("""
    SELECT (SELECT count(*) FROM proventos_b3 WHERE ticker IS NOT NULL),
           (SELECT count(*) FROM prov WHERE usado), (SELECT count(*) FROM exclusoes)
""").fetchall()[0]
na_tabela = con.sql("SELECT count(*) FROM eventos WHERE fonte = 'B3' AND tipo_evento = 'provento' "
                    "AND origem = 'nome_proprio' AND tipo_provento <> 'acoes_entregues'").fetchall()[0][0]
print(f"Checagem 2: linhas de proventos_b3 com ticker={total}; usadas em eventos={usadas} "
      f"(linhas de nome próprio em eventos={na_tabela}); excluídas={excluidas}")
if usadas + excluidas != total or usadas != na_tabela:
    raise ValueError("eventos: checagem 2 reprovada (soma das partes diferente do total)")
itens, quant_tabela, quant_pend = con.sql("""
    SELECT count(*), (SELECT count(*) FROM eventos WHERE tipo_evento <> 'provento'),
           count(*) FILTER (WHERE categoria IN ('pendencia', 'sem_preco')) FROM quantidade_v WHERE situacao = 'dominio'
""").fetchall()[0]
print(f"Checagem 2 (quantidade): itens do domínio={itens}; em eventos={quant_tabela}; pendências pelo preço={quant_pend}")
if quant_tabela + quant_pend != itens:
    raise ValueError("eventos: checagem 2 reprovada (itens de quantidade)")
ent_itens, ent_tabela, ent_pend = con.sql("""
    SELECT count(*) FILTER (WHERE NOT perna_propria),
           (SELECT count(*) FROM eventos WHERE tipo_provento = 'acoes_entregues'),
           count(*) FILTER (WHERE NOT perna_propria AND (ticker, dia_chave) IN
                            (SELECT (ticker, dia_chave) FROM entregas_dia WHERE categoria = 'pendencia'))
    FROM entregas
""").fetchall()[0]
print(f"Checagem 2 (entregas de ações): itens={ent_itens}; em eventos={ent_tabela}; pendências={ent_pend}")
if ent_tabela + ent_pend != ent_itens:
    raise ValueError("eventos: checagem 2 reprovada (entregas de ações)")
print("Lista de exclusões")
con.sql("SELECT * FROM exclusoes ORDER BY ticker, data_ex, id_registro").show(max_rows=300, max_width=200)

con.sql(f"""
    CREATE TEMP TABLE quant_ev AS
    SELECT e.ticker, e.data_ex, g.n FROM eventos e ASOF JOIN pregoes g ON g.data >= e.data_ex
    WHERE e.tipo_evento <> 'provento'
""")
print(f"\nChecagem 3: início de marca com G ou B (2010+) sem evento de quantidade do ticker em até {JANELA_PREGOES} "
      "pregões (pendências)")
con.sql(f"""
    CREATE TEMP TABLE pend_gb AS
    SELECT m.ticker, m.data, m.letra, m.marca,
           (SELECT string_agg(DISTINCT q.rotulo || ' ' || q.situacao, ', ' ORDER BY q.rotulo || ' ' || q.situacao) FROM quantidade_v q JOIN pregoes g ON g.data = q.data_ex
             WHERE q.ticker = m.ticker AND abs(g.n - m.n) <= {JANELA_PREGOES} AND q.situacao <> 'dominio') AS evento_fora_do_dominio_perto
    FROM marcas m
    WHERE m.inicio AND m.letra IN ('G', 'B') AND year(m.data) BETWEEN {ANO_MIN} AND {ANO_MAX}
      AND NOT EXISTS (SELECT 1 FROM quant_ev q WHERE q.ticker = m.ticker AND abs(q.n - m.n) <= {JANELA_PREGOES})
""")
con.sql(f"""
    SELECT (SELECT count(*) FROM marcas WHERE inicio AND letra IN ('G', 'B') AND year(data) BETWEEN {ANO_MIN} AND {ANO_MAX})
               AS inicios_g_ou_b,
           count(*) AS pendentes, count(DISTINCT ticker) AS tickers FROM pend_gb
""").show()
con.sql("SELECT * FROM pend_gb ORDER BY ticker, data, letra").show(max_rows=300, max_width=200)

print(f"Checagem 4: dias com |retorno bruto de fechamento| > {RETORNO_EXTREMO:.0%} em tickers do escopo")
con.sql(f"""
    CREATE TEMP TABLE extremos AS
    SELECT d.ticker, d.data, round(d.fechamento / d.fech_ant - 1, 4) AS retorno, d.fech_ant, d.fechamento,
           d.mudou_fatcot, d.marca,
           EXISTS (SELECT 1 FROM eventos e WHERE e.ticker = d.ticker AND e.data_ex > d.data_ant AND e.data_ex <= d.data
                   AND e.tipo_evento <> 'provento') AS explicado,
           (SELECT string_agg(q.rotulo || ' ' || q.situacao, ', ' ORDER BY q.rotulo, q.situacao) FROM quantidade_v q
             WHERE q.ticker = d.ticker AND q.data_ex = d.data AND q.situacao <> 'dominio') AS evento_fora_do_dominio
    FROM dia d WHERE d.fech_ant IS NOT NULL AND abs(d.fechamento / d.fech_ant - 1) > {RETORNO_EXTREMO}
""")
con.sql("SELECT count(*) AS dias, count(*) FILTER (WHERE explicado) AS explicados, "
        "count(*) FILTER (WHERE NOT explicado) AS listados FROM extremos").show()
con.sql("SELECT * EXCLUDE (explicado) FROM extremos WHERE NOT explicado ORDER BY ticker, data").show(max_rows=300, max_width=200)

print("Checagem 5: os dias com mudou_fatcot (todos os tickers de `acoes`)")
con.sql("""
    SELECT *, CASE WHEN NOT no_escopo THEN 'fora do escopo (listado)'
                   WHEN evento IS NOT NULL THEN 'explicado' ELSE 'listado' END AS situacao
    FROM (SELECT a.ticker, a.data, a.fatcot, a.marca, a.ticker IN (SELECT ticker FROM escopo) AS no_escopo,
                 (SELECT string_agg(e.tipo_evento || ' ' || e.fator_quantidade, ', ' ORDER BY e.tipo_evento, e.seq) FROM eventos e
                   WHERE e.ticker = a.ticker AND e.data_ex = a.data AND e.tipo_evento <> 'provento') AS evento
          FROM acoes a WHERE a.mudou_fatcot)
    ORDER BY data, ticker
""").show(max_width=200)

print("Checagem 6: preco_por_acao ex / anterior × fator do dia nos dias com evento de quantidade em `eventos`")
con.sql("""
    CREATE TEMP TABLE razoes AS
    SELECT ticker, data_ex, fatores, fator_dia, razao_ppa, razao_ppa * fator_dia AS razao_corrigida, categoria
    FROM dia_q WHERE categoria <> 'pendencia'
""")
con.sql("""
    SELECT count(*) AS eventos, count(razao_corrigida) AS com_preco,
           round(quantile_cont(razao_corrigida, 0.01), 4) AS p01, round(quantile_cont(razao_corrigida, 0.05), 4) AS p05,
           round(quantile_cont(razao_corrigida, 0.25), 4) AS p25, round(median(razao_corrigida), 4) AS mediana,
           round(quantile_cont(razao_corrigida, 0.75), 4) AS p75, round(quantile_cont(razao_corrigida, 0.95), 4) AS p95,
           round(quantile_cont(razao_corrigida, 0.99), 4) AS p99
    FROM razoes
""").show(max_width=200)
con.sql(f"""
    SELECT * FROM razoes WHERE razao_corrigida IS NOT NULL
    ORDER BY abs(ln(razao_corrigida)) DESC, ticker, data_ex LIMIT {PIORES}
""").show(max_width=200)

print("Checagem 7: contagens")
for coluna in ("tipo_evento", "tipo_provento", "fonte", "origem", "confirmacao", "parcela_repetida"):
    con.sql(f"SELECT {coluna}, count(*) AS linhas FROM eventos GROUP BY ALL ORDER BY linhas DESC").show()
con.sql("""
    PIVOT (SELECT year(data_ex) AS ano, tipo_evento FROM eventos) ON tipo_evento USING count(*)
    GROUP BY ano ORDER BY ano
""").show(max_rows=30)
con.sql("""
    SELECT count(DISTINCT ticker) AS tickers, count(DISTINCT ticker) FILTER (WHERE tipo_evento <> 'provento')
               AS tickers_com_evento_de_quantidade,
           (SELECT count(*) FROM escopo) AS tickers_no_escopo
    FROM eventos
""").show()
print("Observações registradas")
con.sql("SELECT observacao, count(*) AS linhas FROM eventos WHERE observacao IS NOT NULL GROUP BY ALL "
        "ORDER BY linhas DESC, observacao").show(max_rows=100, max_width=250)
print("Mesma data ex com provento e evento de quantidade")
con.sql(f"""
    SELECT ticker, data_ex, string_agg(tipo_evento || coalesce(' ' || tipo_provento, '') || ' '
                                       || coalesce(fator_quantidade::VARCHAR, valor_dinheiro::VARCHAR), ', '
                                       ORDER BY tipo_evento, seq) AS eventos
    FROM eventos WHERE observacao LIKE '%{OBS_MESMO_DIA}%' GROUP BY ticker, data_ex ORDER BY ticker, data_ex
""").show(max_rows=200, max_width=250)

print("Checagem 8: Parte 3a — ver tabela acima")

# Parte 3c — pendências de marca D/J que continuam sem provento depois das Partes 3a e 3b, com o motivo
con.sql(f"""
    CREATE TEMP TABLE pend_dj AS
    SELECT p.ticker, p.data, p.letra, p.marca, p.escopo,
           CASE WHEN p.data > (SELECT min(data) FROM pregoes WHERE data > (SELECT max(data_com) FROM proventos_b3))
                     THEN 'posterior à última data com baixada da B3'
                WHEN p.empresa_sem_registros_na_b3 THEN 'empresa sem registros na B3'
                WHEN p.ticker_sem_linha_pareada THEN 'registros da B3 do nome sem par no período ou na classe'
                ELSE 'sem provento do tipo em até {JANELA_PREGOES} pregões' END AS motivo
    FROM read_csv('{PENDENCIAS}') p JOIN pregoes g ON g.data = p.data
    WHERE NOT EXISTS (SELECT 1 FROM eventos e JOIN pregoes h ON h.data = e.data_ex
                      WHERE e.ticker = p.ticker AND e.tipo_evento = 'provento' AND abs(h.n - g.n) <= {JANELA_PREGOES}
                        AND e.tipo_provento = CASE p.letra WHEN 'D' THEN 'dividendo' ELSE 'jcp' END)
""")
print(f"\nParte 3c — pendências de marca D/J (de {PENDENCIAS}) que continuam sem provento do tipo")
con.sql(f"""
    SELECT (SELECT count(*) FROM read_csv('{PENDENCIAS}')) AS pendencias_antes,
           count(*) AS pendencias_depois,
           (SELECT count(*) FROM read_csv('{PENDENCIAS}')) - count(*) AS resolvidas_por_3a_3b
    FROM pend_dj
""").show()
con.sql("SELECT motivo, count(*) AS marcas, count(DISTINCT ticker) AS tickers FROM pend_dj GROUP BY motivo "
        "ORDER BY marcas DESC").show(max_width=200)
con.sql("SELECT * FROM pend_dj ORDER BY ticker, data, letra").show(max_rows=300, max_width=200)

# ============================ Rodada 3 — pendências para a Fase 5 e saltos ============================
# Tabela lida pela Fase 5 (regra do responsável, 18/11/2026): dia NULL com (a) início de marca G/B sem evento de
# quantidade em até 5 pregões ou (b) mudança do fator de cotação sem evento; dia com |r| >= 40% sem nenhuma marca:
# retorno calculado, flag `salto_sem_marca`. 26/10/2026 (queda de mercado) fica fora do detector de saltos.
con.sql(f"""
    CREATE OR REPLACE TABLE pendencias_eventos AS
    SELECT m.ticker, m.data, 'marca_gb_sem_evento' AS motivo, string_agg(m.letra, '' ORDER BY m.letra) AS detalhe
    FROM marcas m
    WHERE m.inicio AND m.letra IN ('G', 'B') AND year(m.data) BETWEEN {ANO_MIN} AND {ANO_MAX}
      AND NOT EXISTS (SELECT 1 FROM quant_ev q WHERE q.ticker = m.ticker AND abs(q.n - m.n) <= {JANELA_PREGOES})
    GROUP BY m.ticker, m.data
    UNION ALL
    SELECT d.ticker, d.data, 'mudou_fatcot_sem_evento', 'marca ' || d.marca FROM dia d
    WHERE d.mudou_fatcot AND NOT EXISTS (SELECT 1 FROM eventos e WHERE e.ticker = d.ticker AND e.tipo_evento <> 'provento'
                                         AND e.data_ex > d.data_ant AND e.data_ex <= d.data)
    UNION ALL
    SELECT d.ticker, d.data, CASE WHEN d.marca = '' THEN 'salto_sem_marca' ELSE 'salto_com_marca' END,
           'retorno ' || round(d.preco_por_acao / d.ppa_ant - 1, 4) || coalesce('; marca ' || nullif(d.marca, ''), '')
    FROM dia d
    WHERE d.ppa_ant IS NOT NULL AND abs(d.preco_por_acao / d.ppa_ant - 1) >= {SALTO} AND d.data <> DATE '{DATA_MERCADO}'
      AND year(d.data) >= {ANO_MIN}
      AND NOT EXISTS (SELECT 1 FROM eventos e WHERE e.ticker = d.ticker AND e.tipo_evento <> 'provento'
                                         AND e.data_ex > d.data_ant AND e.data_ex <= d.data)
    UNION ALL
    SELECT ticker, coalesce(data_preco, data_ex), 'quantidade_pendente', fatores || '; resíduo ' || coalesce(round(residuo, 4)::VARCHAR, 'sem preço')
    FROM dia_q WHERE categoria IN ('pendencia', 'sem_preco')
    UNION ALL
    SELECT ticker, data_ex, 'acoes_entregues_pendente', entregas || '; ' || motivo FROM entregas_dia WHERE categoria = 'pendencia'
    ORDER BY ticker, data, motivo
""")
print("\nRodada 3 — pendências de eventos (tabela `pendencias_eventos`, lida pela Fase 5)")
con.sql("""SELECT motivo, count(*) AS dias, count(DISTINCT ticker) AS tickers FROM pendencias_eventos
           GROUP BY motivo ORDER BY motivo""").show()
con.sql("SELECT * FROM pendencias_eventos WHERE motivo NOT IN ('marca_gb_sem_evento') ORDER BY motivo, ticker, data").show(
    max_rows=400, max_width=250)
print("Checagem 3 antes/depois: rodada 2 = 190 de 326 inícios de marca G/B sem evento (79 tickers); agora =",
      con.sql("SELECT count(*), count(DISTINCT ticker) FROM pendencias_eventos WHERE motivo = 'marca_gb_sem_evento'").fetchall()[0])
print("Checagem 4 antes/depois: rodada 2 = 174 de 250 dias com |retorno de fechamento| > 50% sem evento; agora =",
      con.sql("SELECT count(*) FILTER (WHERE NOT explicado), count(*) FROM extremos").fetchall()[0])
print(f"Saltos |retorno de preco_por_acao| >= {SALTO:.0%} em {DATA_MERCADO} (queda de mercado pós-eleição, Ibovespa "
      "−13,88%; fora do detector, sem efeito no cálculo):",
      con.sql(f"""SELECT count(*) FROM dia WHERE data = DATE '{DATA_MERCADO}' AND ppa_ant IS NOT NULL
                   AND abs(preco_por_acao / ppa_ant - 1) >= {SALTO}""").fetchall()[0][0])
n_pend, n_chaves = con.sql("SELECT count(*), count(DISTINCT (ticker, data, motivo)) FROM pendencias_eventos").fetchall()[0]
if n_pend != n_chaves:
    raise ValueError("pendencias_eventos: chave (ticker, data, motivo) repetida")

con.commit()
con.close()
