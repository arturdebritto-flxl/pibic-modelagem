# pibic.modelagem

PIBIC (UnB, Ciências Contábeis): "Modelagem Preditiva e Prescritiva com IA para o Mercado de Capitais
Brasileiro". Este repositório trata os dados da B3 (COTAHIST, proventos e eventos corporativos), da CVM e do
Banco Central até um painel mensal de ações e alvos de retorno. Estado atual: **painel provisório** (as saídas
de `dados/manuais/saidas.csv` ainda não foram verificadas).

## Ordem de execução

Da raiz do repositório, com `uv` (Python 3.15) e o banco DuckDB em `dados/pibic.duckdb`:

```
uv run scripts/ingestao.py
uv run scripts/limpeza.py
uv run scripts/universo.py
uv run scripts/cvm_ingestao.py
uv run scripts/b3_cadastro.py
uv run scripts/saidas.py
uv run scripts/proventos.py
uv run scripts/eventos.py
uv run scripts/series.py
uv run scripts/retornos.py
uv run scripts/painel.py
```

Conferências, sem gravar no banco: `scripts/confere_d23.py`, `scripts/confere_cotahist_2026.py`,
`scripts/dossie_saidas.py` (dossiê de verificação das saídas em `resultados/dossie_saidas/`). Scripts
`scripts/explora_*.py` são explorações registradas.

Os arquivos baixados ficam em `dados/brutos/` (fora do Git), com SHA-256 em `dados/hashes.txt`
(`sha256sum -c dados/hashes.txt`). Regras de tratamento e achados sobre os dados estão em `notas/`; os relatórios
de cada script, em `resultados/`.
