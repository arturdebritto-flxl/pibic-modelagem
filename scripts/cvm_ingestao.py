import hashlib
import time
import urllib.request
from pathlib import Path

BASE = "https://dados.cvm.gov.br/dados/CIA_ABERTA"
PASTA = Path("dados/brutos/cvm")
ANOS = range(2010, 2027)
PAUSA_S = 1

ARQUIVOS = (
    ["CAD/DADOS/cad_cia_aberta.csv", "CAD/META/meta_cad_cia_aberta.txt",
     "DOC/FCA/META/fca_cia_aberta.zip", "DOC/IPE/META/meta_ipe_cia_aberta.txt"]
    + [f"DOC/FCA/DADOS/fca_cia_aberta_{a}.zip" for a in ANOS]
    + [f"DOC/IPE/DADOS/ipe_cia_aberta_{a}.zip" for a in ANOS]
)

# 1. Download: só o que ainda não existe (brutos nunca são sobrescritos);
#    o hash de cada arquivo novo é acrescentado a dados/hashes.txt
for caminho in ARQUIVOS:
    # o dicionário do FCA tem o mesmo padrão de nome dos dados: prefixo "meta_"
    nome = caminho.split("/")[-1]
    destino = PASTA / ("meta_" + nome if caminho == "DOC/FCA/META/fca_cia_aberta.zip" else nome)
    if destino.exists():
        continue
    PASTA.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(f"{BASE}/{caminho}", timeout=120) as resposta:
        conteudo = resposta.read()
    destino.write_bytes(conteudo)
    with open("dados/hashes.txt", "a") as hashes:
        hashes.write(f"{hashlib.sha256(conteudo).hexdigest()}  {destino}\n")
    print("baixado:", destino, len(conteudo), "bytes")
    time.sleep(PAUSA_S)

# Data de download (data de modificação do arquivo local) — o cadastro muda todo dia
print("\nArquivos em", PASTA)
for arquivo in sorted(PASTA.iterdir()):
    print(f"  {time.strftime('%Y-%m-%d', time.localtime(arquivo.stat().st_mtime))}"
          f"  {arquivo.stat().st_size:>9}  {arquivo.name}")

# 2. Extração dos ZIPs para pasta temporária (os brutos não são tocados).
#    Formato conferido nos dicionários META e nos arquivos: separador ';',
#    codificação Latin-1, cabeçalho na 1ª linha, sem aspas, datas AAAA-MM-DD.
import tempfile
import zipfile

import duckdb

tmp = Path(tempfile.mkdtemp())
linhas_arquivo = {}
for z in sorted(PASTA.glob("*_cia_aberta_2*.zip")):
    with zipfile.ZipFile(z) as arq:
        for nome in arq.namelist():
            if nome.startswith(("ipe_cia_aberta_2", "fca_cia_aberta_valor_mobiliario_")):
                arq.extract(nome, tmp)
                linhas_arquivo[nome] = (tmp / nome).read_bytes().count(b"\n") - 1
linhas_arquivo["cad_cia_aberta.csv"] = (PASTA / "cad_cia_aberta.csv").read_bytes().count(b"\n") - 1

LER = "delim = ';', header = true, quote = '', escape = '', encoding = 'latin-1', all_varchar = true, filename = true"

con = duckdb.connect("dados/pibic.duckdb")

# 3. Cadastro de companhias abertas (só os campos usados; o bruto guarda o resto)
con.sql(f"""
    CREATE OR REPLACE TABLE cvm_cadastro AS
    SELECT CNPJ_CIA                AS cnpj,
           CAST(CD_CVM AS INTEGER) AS codigo_cvm,
           DENOM_SOCIAL            AS denominacao_social,
           DENOM_COMERC            AS denominacao_comercial,
           CAST(DT_REG AS DATE)    AS data_registro,
           CAST(DT_CANCEL AS DATE) AS data_cancelamento,
           MOTIVO_CANCEL           AS motivo_cancelamento,
           SIT                     AS situacao,
           CAST(DT_INI_SIT AS DATE) AS data_situacao,
           SIT_EMISSOR             AS situacao_emissor,
           SETOR_ATIV              AS setor
    FROM read_csv('{PASTA}/cad_cia_aberta.csv', {LER})
""")

# 4. FCA — valores mobiliários (ticker ↔ CNPJ), todos os anos
con.sql(f"""
    CREATE OR REPLACE TABLE cvm_fca_valor_mobiliario AS
    SELECT CAST(regexp_extract(filename, '(\\d{{4}})\\.csv$', 1) AS INTEGER) AS ano_arquivo,
           CNPJ_Companhia                       AS cnpj,
           CAST(Data_Referencia AS DATE)        AS data_referencia,
           CAST(Versao AS INTEGER)              AS versao,
           CAST(ID_Documento AS BIGINT)         AS id_documento,
           Nome_Empresarial                     AS nome_empresarial,
           Valor_Mobiliario                     AS valor_mobiliario,
           Sigla_Classe_Acao_Preferencial       AS sigla_classe_pn,
           Classe_Acao_Preferencial             AS classe_pn,
           Codigo_Negociacao                    AS ticker,
           Composicao_BDR_Unit                  AS composicao_bdr_unit,
           Mercado                              AS mercado,
           Sigla_Entidade_Administradora        AS sigla_entidade,
           Entidade_Administradora              AS entidade,
           CAST(Data_Inicio_Negociacao AS DATE) AS data_inicio_negociacao,
           CAST(Data_Fim_Negociacao AS DATE)    AS data_fim_negociacao,
           Segmento                             AS segmento,
           CAST(Data_Inicio_Listagem AS DATE)   AS data_inicio_listagem,
           CAST(Data_Fim_Listagem AS DATE)      AS data_fim_listagem
    FROM read_csv('{tmp}/fca_cia_aberta_valor_mobiliario_*.csv', {LER})
""")

# 5. IPE — índice dos documentos periódicos e eventuais, 2010–2026
con.sql(f"""
    CREATE OR REPLACE TABLE cvm_ipe AS
    SELECT CAST(regexp_extract(filename, '(\\d{{4}})\\.csv$', 1) AS INTEGER) AS ano_arquivo,
           CNPJ_Companhia                AS cnpj,
           Nome_Companhia                AS nome_companhia,
           CAST(Codigo_CVM AS INTEGER)   AS codigo_cvm,
           CAST(Data_Referencia AS DATE) AS data_referencia,
           Categoria                     AS categoria,
           Tipo                          AS tipo,
           Especie                       AS especie,
           Assunto                       AS assunto,
           CAST(Data_Entrega AS DATE)    AS data_entrega,
           Tipo_Apresentacao             AS tipo_apresentacao,
           Protocolo_Entrega             AS protocolo_entrega,
           CAST(Versao AS INTEGER)       AS versao,
           Link_Download                 AS link
    FROM read_csv('{tmp}/ipe_cia_aberta_*.csv', {LER})
""")

# Checagem 1: linhas lidas = linhas dos arquivos (sem o cabeçalho)
esperado = {
    "cvm_cadastro": linhas_arquivo["cad_cia_aberta.csv"],
    "cvm_fca_valor_mobiliario": sum(n for a, n in linhas_arquivo.items() if a.startswith("fca")),
    "cvm_ipe": sum(n for a, n in linhas_arquivo.items() if a.startswith("ipe")),
}
print("\nChecagem 1: linhas nos arquivos x linhas na tabela")
for tabela, n in esperado.items():
    lidas = con.sql(f"SELECT count(*) FROM {tabela}").fetchall()[0][0]
    print(f"  {tabela}: arquivos {n}, tabela {lidas}", "OK" if n == lidas else "DIVERGENTE")

# Checagem 2: cadastro — candidatos a chave, nulos e faixa de datas
con.sql("""
    SELECT count(*) AS linhas,
           count(DISTINCT cnpj) AS cnpjs, count(DISTINCT codigo_cvm) AS codigos_cvm,
           count(*) FILTER (WHERE cnpj IS NULL) AS cnpj_nulo,
           count(*) FILTER (WHERE data_cancelamento IS NOT NULL) AS canceladas,
           count(*) FILTER (WHERE data_cancelamento IS NOT NULL
                              AND motivo_cancelamento IS NULL) AS cancelada_sem_motivo,
           min(data_registro) AS registro_min, max(data_cancelamento) AS cancel_max
    FROM cvm_cadastro
""").show()

# Checagem 3: FCA — linhas e tickers preenchidos por ano do arquivo
con.sql("""
    SELECT ano_arquivo, count(*) AS linhas,
           count(ticker) AS com_ticker, count(DISTINCT ticker) AS tickers,
           count(*) FILTER (WHERE cnpj IS NULL) AS cnpj_nulo,
           min(data_referencia) AS ref_min, max(data_referencia) AS ref_max
    FROM cvm_fca_valor_mobiliario
    GROUP BY ano_arquivo ORDER BY ano_arquivo
""").show(max_rows=30)

# Checagem 4: FCA — linhas idênticas e tickers ligados a mais de um CNPJ
con.sql("""
    SELECT (SELECT count(*) FROM (SELECT 1 FROM cvm_fca_valor_mobiliario
             GROUP BY ALL HAVING count(*) > 1))                    AS grupos_linhas_identicas,
           (SELECT count(*) FROM (SELECT ticker FROM cvm_fca_valor_mobiliario
             WHERE ticker IS NOT NULL
             GROUP BY ticker HAVING count(DISTINCT cnpj) > 1))     AS tickers_com_varios_cnpj
""").show()

# Checagem 5: IPE — linhas por ano, unicidade do link, nulos e faixa de datas
con.sql("""
    SELECT ano_arquivo, count(*) AS linhas, count(DISTINCT link) AS links,
           count(*) FILTER (WHERE cnpj IS NULL) AS cnpj_nulo,
           count(*) FILTER (WHERE data_entrega IS NULL) AS entrega_nula,
           count(*) FILTER (WHERE protocolo_entrega IS NULL) AS sem_protocolo,
           min(data_entrega) AS entrega_min, max(data_entrega) AS entrega_max
    FROM cvm_ipe
    GROUP BY ano_arquivo ORDER BY ano_arquivo
""").show(max_rows=30)

# Checagem 6: IPE — categorias de documento
con.sql("""
    SELECT categoria, count(*) AS linhas FROM cvm_ipe
    GROUP BY categoria ORDER BY linhas DESC
""").show(max_rows=50)

con.close()
