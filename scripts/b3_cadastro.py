import base64
import hashlib
import json
import time
import urllib.request
from pathlib import Path

import duckdb

# Dados cadastrais do emissor na B3 (mesmo serviço dos proventos), só para os
# tickers de saída que o FCA da CVM não cobre (o FCA só traz ticker desde 2018).
# Serve para obter o código CVM e, por ele, o CNPJ no cadastro da CVM.

DATA_CORTE = "2026-11-01"
PASTA = Path("dados/brutos/b3_cadastro")
PAUSA_S = 3
AGENTE = "Mozilla/5.0 (X11; Linux x86_64) pesquisa-academica-PIBIC-UnB"
B3 = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall"

con = duckdb.connect("dados/pibic.duckdb")

# 1. Raízes (4 letras) dos tickers de saída do universo sem ticker no FCA
raizes = [r[0] for r in con.sql(f"""
    SELECT DISTINCT left(ticker, 4) AS raiz
    FROM acoes
    WHERE ticker IN (SELECT ticker FROM universo_mensal)
      AND ticker NOT IN (SELECT ticker FROM cvm_fca_valor_mobiliario WHERE ticker IS NOT NULL)
    GROUP BY ticker
    HAVING max(data) < DATE '{DATA_CORTE}'
    ORDER BY raiz
""").fetchall()]
print(len(raizes), "raízes a consultar")

# 2. Download: uma requisição por raiz, só se o arquivo ainda não existe
for raiz in raizes:
    destino = PASTA / f"suplemento_{raiz}.json"
    if destino.exists():
        continue
    PASTA.mkdir(parents=True, exist_ok=True)
    parametros = json.dumps({"issuingCompany": raiz, "language": "pt-br"}, separators=(",", ":"))
    url = f"{B3}/GetListedSupplementCompany/{base64.b64encode(parametros.encode()).decode()}"
    pedido = urllib.request.Request(url, headers={"User-Agent": AGENTE})
    with urllib.request.urlopen(pedido, timeout=60) as resposta:
        conteudo = resposta.read()
    destino.write_bytes(conteudo)
    with open("dados/hashes.txt", "a") as hashes:
        hashes.write(f"{hashlib.sha256(conteudo).hexdigest()}  {destino}\n")
    print("baixado:", destino, len(conteudo), "bytes")
    time.sleep(PAUSA_S)

# 3. Tabela: uma linha por raiz consultada (código CVM vazio se a B3 não devolveu emissor)
linhas = []
for raiz in raizes:
    # raiz desconhecida da B3: resposta vazia (0 bytes) ou lista vazia
    dados = json.loads((PASTA / f"suplemento_{raiz}.json").read_text() or "[]")
    emissor = dados[0] if isinstance(dados, list) and dados else {}
    codigo = (emissor.get("codeCVM") or "").strip()
    linhas.append((raiz, int(codigo) if codigo.isdigit() and int(codigo) > 0 else None,
                   (emissor.get("tradingName") or "").strip() or None))
con.sql("CREATE OR REPLACE TABLE b3_emissores (raiz VARCHAR, codigo_cvm INTEGER, nome_pregao VARCHAR)")
con.executemany("INSERT INTO b3_emissores VALUES (?, ?, ?)", linhas)

# Checagem 1: unicidade da chave e nulos
con.sql("""
    SELECT count(*) AS linhas, count(DISTINCT raiz) AS raizes,
           count(codigo_cvm) AS com_codigo_cvm,
           count(*) FILTER (WHERE codigo_cvm IS NULL) AS sem_codigo_cvm
    FROM b3_emissores
""").show()

# Checagem 2: código CVM -> CNPJ no cadastro da CVM (quantos CNPJs por código)
#             e nome da B3 ao lado do nome do COTAHIST, para conferência manual
con.sql(f"""
    WITH tickers AS (
        SELECT ticker, arg_max(nome, data) AS nome_cotahist, max(data) AS ultimo_pregao
        FROM acoes
        WHERE ticker IN (SELECT ticker FROM universo_mensal)
          AND ticker NOT IN (SELECT ticker FROM cvm_fca_valor_mobiliario WHERE ticker IS NOT NULL)
        GROUP BY ticker HAVING max(data) < DATE '{DATA_CORTE}'
    )
    SELECT t.ticker, t.ultimo_pregao, t.nome_cotahist, b.nome_pregao AS nome_b3, b.codigo_cvm,
           count(DISTINCT c.cnpj) AS cnpjs,
           any_value(c.denominacao_social) AS denominacao_cvm
    FROM tickers t
    JOIN b3_emissores b ON b.raiz = left(t.ticker, 4)
    LEFT JOIN cvm_cadastro c ON c.codigo_cvm = b.codigo_cvm
    GROUP BY ALL ORDER BY cnpjs = 0, t.ultimo_pregao
""").show(max_rows=60, max_width=220)

con.close()
