# 🔍 CNPJ Prospector — B2B Lead Mining com Dados Públicos da Receita Federal

Pipeline completo para importar, estruturar e consultar a base pública de CNPJs da Receita Federal do Brasil (68M+ empresas), gerando listas de prospecção segmentadas por CNAE, UF e situação cadastral.

## 📊 O que você consegue fazer

- **Filtrar** 68 milhões de empresas por setor (CNAE), estado (UF) e status (ativa/inativa)
- **Cruzar** dados de estabelecimentos, sócios, município e CNAE em uma única consulta
- **Exportar** listas prontas para Excel com CNPJ, nome, telefone, e-mail e nome do sócio
- **Consultar** CNAEs por palavra-chave, buscar empresa por nome, listar sócios por CNPJ

## 🗂️ Estrutura do Projeto

```
cnpj-prospector/
├── importar_rfb_v2.ipynb     # Importação completa para PostgreSQL (~3h)
├── consulta_rfb.ipynb        # Consultas e exportação de listas
└── README.md
```

## 🏗️ Arquitetura

```
Receita Federal (dados.gov.br)
        ↓ download manual (arquivos CSV latin-1)
  Python (psycopg2)
        ↓ streaming + batch de 10.000 linhas
  PostgreSQL 16 — banco rfb_cnpj
        ↓ JOINs + filtros
  Excel / CSV — lista de leads
```

### Tabelas criadas

| Tabela | Registros | Descrição |
|---|---|---|
| `empresas` | 68,6 M | Razão social, natureza jurídica, porte |
| `estabelecimentos` | 71,8 M | Endereço, telefone, e-mail, CNAE, situação |
| `socios` | 62,2 M | Nome, qualificação, data de entrada |
| `cnaes` | ~1.300 | Tabela de códigos CNAE |
| `municipios` | ~5.600 | Tabela de municípios |

## ⚙️ Pré-requisitos

- Python 3.10+
- PostgreSQL 14+ (local)
- ~80 GB de espaço em disco (banco + arquivos fonte)
- Jupyter / VS Code com extensão Jupyter

```bash
pip install psycopg2-binary pandas openpyxl tqdm
```

## 🚀 Como usar

### 1. Baixar os dados
Acesse [dados.gov.br](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) e baixe os arquivos:
- `EMPRECSV` (10 arquivos) — Empresas
- `ESTABELE` (10 arquivos) — Estabelecimentos
- `SOCIOCSV` (10 arquivos) — Sócios
- `CNAECSV` — Códigos CNAE
- `MUNICCSV` — Municípios

### 2. Importar para o PostgreSQL
Abra `importar_rfb_v2.ipynb` e execute célula a célula:
- Célula 1: configurações (banco, pasta dos dados)
- Célula 2: cria as tabelas
- Célula 3: importa CNAEs e Municípios (~2 min)
- Célula 4: importa Empresas (~50 min)
- Célula 5: importa Estabelecimentos (~90 min)
- Célula 6: importa Sócios (~45 min)
- Célula 7: verifica totais

### 3. Consultar e exportar
Abra `consulta_rfb.ipynb`:
- Edite os filtros de CNAE, UF e situação na Célula 4
- Execute e exporte para Excel com a Célula 5

**Exemplo de filtro:**
```python
CNAES_ALVO = ('8630501','8630502','8630503')  # Clínicas médicas
UFS_ALVO   = ('PE','CE','GO','MT')
SITUACAO   = '02'  # 02 = Ativa
```

## 📝 Exemplo de saída

| CNPJ | Nome | Município | UF | Telefone | E-mail | Sócio |
|---|---|---|---|---|---|---|
| 39239037000102 | LC MORAIS - ESPAÇO SAÚDE | ABAIARA | CE | (88) 81263443 | — | LUIS CESAR MORAIS |
| 59334937000146 | CLÍNICA DR CARLOS IURY | ACARAÚ | CE | (88) 82127728 | drcarlosiury@gmail.com | CARLOS IURY FURTADO |

## ⚡ Performance

| Etapa | Tempo aproximado |
|---|---|
| Empresas (68M) | ~50 min |
| Estabelecimentos (71M) | ~90 min |
| Sócios (62M) | ~45 min |
| Consulta com JOINs | < 5 seg |

## 📌 Casos de uso

- Prospecção B2B por setor e região
- Validação e enriquecimento de base de clientes
- Análise de mercado e densidade empresarial por CNAE/UF
- Due diligence de cadastro de sócios

## ⚖️ Conformidade LGPD

Os dados utilizados são **públicos** e disponibilizados pela Receita Federal com base no **art. 8º da Lei de Acesso à Informação (Lei 12.527/2011)** e no **art. 7º, §3º da LGPD (Lei 13.709/2018)**, que permite o tratamento de dados tornados manifestamente públicos pelo titular. O uso para prospecção comercial B2B com finalidade legítima é permitido, desde que respeitados os direitos dos titulares (opt-out, acesso e retificação).

---

**Desenvolvido por:** Nayara · Contabilidade, FP&A e Valuation para PMEs brasileiras  
**Contato:** info@veritasyn.com.br
