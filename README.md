# 📊 Dados Públicos - BCB

Repositório de dados públicos consolidados do Banco Central do Brasil, prontos para consumo em ferramentas de BI.

## Datasets disponíveis

### Cooperados por Cooperativa

* **Arquivo:** `dados/bcb/cooperados/cooperados_por_cooperativa.csv`
* **Fonte:** [BCB - Documento 5300](https://www.bcb.gov.br/estabilidadefinanceira/cooperados_cooperativa)
* **Período:** 01/2020 a 12/2025 (72 meses)
* **Separador:** `;`
* **Encoding:** `UTF-8`

**Colunas:**

| Coluna | Descrição |
| --- | --- |
| `CNPJ` | Texto, padronizado com zeros à esquerda (8 dígitos) |
| `Total de Cooperados` | Inteiro |
| `Cooperados PF` | Inteiro |
| `Cooperados PJ` | Inteiro |
| `Sexo Feminino` | Inteiro |
| `Sexo Masculino` | Inteiro |
| `Sexo nao Informado` | Inteiro |
| `Periodo` | Data (YYYY-MM-DD, dia fixo = 01) |

> Coluna `Nome` removida para otimizar tamanho.

---

### ESTBAN Municipal - Dados Estratégicos

* **Arquivo:** `dados/bcb/estban/estban_municipal_estrategico.csv`
* **Fonte:** [BCB - ESTBAN Documento 4500](https://www.bcb.gov.br/estatisticas/estatisticabancariamunicipios)
* **Período:** 2023-01 a 2025-09 (33/33 meses)
* **Separador:** `;`
* **Encoding:** `UTF-8`
* **Tamanho:** ~55 MB

**Colunas de Identificação:**

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `DATA_BASE` | date | Data de referência (YYYY-MM-01) |
| `UF` | text | Unidade Federativa |
| `CODMUN` | text | Código do município (BCB) |
| `MUNICIPIO` | text | Nome do município |
| `CNPJ` | text | CNPJ raiz da IF (8 dígitos) |
| `NOME_INSTITUICAO` | text | Nome da instituição financeira |

**Colunas de Crédito (Ativo):**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `OP_CREDITO_TOTAL` | 160 | Total de Operações de Crédito |
| `EMPRESTIMOS_TITULOS` | 161 | Empréstimos e Títulos Descontados (Capital de Giro) |
| `FINANCIAMENTOS` | 162 | Financiamentos (Veículos, Bens) |
| `FIN_RURAIS_AGRICOLA` | 163 | Financiamentos Rurais - Custeio/Investimento Agrícola |
| `FIN_AGROINDUSTRIAIS` | 167 | Financiamentos Agroindustriais |
| `FIN_IMOBILIARIOS` | 169 | Financiamentos Imobiliários |
| `OUTRAS_OP_CREDITO` | 171 | Outras Operações de Crédito (PF) |

**Colunas de Risco:**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `PROVISAO_CREDITO` | 174 | Provisão p/ Créditos de Liquidação Duvidosa |

**Colunas de Captação (Passivo):**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `DEP_VISTA_TOTAL` | 401-419 | Depósitos à Vista (consolidado) |
| `DEP_POUPANCA` | 420 | Depósitos de Poupança |
| `DEP_PRAZO` | 432 | Depósitos a Prazo (CDB/RDB) |

**Colunas Patrimoniais:**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `ATIVO_TOTAL` | 399 | Total do Ativo |
| `PATRIMONIO_LIQUIDO` | 610 | Patrimônio Líquido |

**KPIs Derivados:**

| Coluna | Fórmula | Descrição |
| --- | --- | --- |
| `IDX_PROVISAO_CREDITO` | abs(174) / 160 × 100 | Índice de provisão sobre crédito (%) |
| `PENETRACAO_RURAL` | 163 / 160 × 100 | Participação do crédito rural no total (%) |
| `MIX_POUPANCA` | 420 / (401-419 + 420 + 432) × 100 | Peso da poupança na captação total (%) |

---

## 🔌 Uso no Power BI

### Cooperados por Cooperativa

**Obter Dados → Consulta em Branco → Editor Avançado:**

```
let
    Url = "https://raw.githubusercontent.com/mazoir/dados_publicos/main/dados/bcb/cooperados/cooperados_por_cooperativa.csv",
    Fonte = Csv.Document(Web.Contents(Url), [Delimiter=";", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Cabecalho = Table.PromoteHeaders(Fonte, [PromoteAllScalars=true]),
    Tipagem = Table.TransformColumnTypes(Cabecalho, {
        {"CNPJ", type text},
        {"Total de Cooperados", Int64.Type},
        {"Cooperados PF", Int64.Type},
        {"Cooperados PJ", Int64.Type},
        {"Sexo Feminino", Int64.Type},
        {"Sexo Masculino", Int64.Type},
        {"Sexo nao Informado", Int64.Type},
        {"Periodo", type date}
    })
in
    Tipagem
```

### ESTBAN Municipal Estratégico

```
let
    Url = "https://raw.githubusercontent.com/mazoir/dados_publicos/main/dados/bcb/estban/estban_municipal_estrategico.csv",
    Fonte = Csv.Document(Web.Contents(Url), [Delimiter=";", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Cabecalho = Table.PromoteHeaders(Fonte, [PromoteAllScalars=true]),
    Tipagem = Table.TransformColumnTypes(Cabecalho, {
        {"DATA_BASE", type date},
        {"UF", type text},
        {"CODMUN", type text},
        {"MUNICIPIO", type text},
        {"CNPJ", type text},
        {"NOME_INSTITUICAO", type text},
        {"OP_CREDITO_TOTAL", type number},
        {"EMPRESTIMOS_TITULOS", type number},
        {"FINANCIAMENTOS", type number},
        {"FIN_RURAIS_AGRICOLA", type number},
        {"FIN_AGROINDUSTRIAIS", type number},
        {"FIN_IMOBILIARIOS", type number},
        {"OUTRAS_OP_CREDITO", type number},
        {"PROVISAO_CREDITO", type number},
        {"ATIVO_TOTAL", type number},
        {"DEP_VISTA_TOTAL", type number},
        {"DEP_POUPANCA", type number},
        {"DEP_PRAZO", type number},
        {"PATRIMONIO_LIQUIDO", type number},
        {"IDX_PROVISAO_CREDITO", type number},
        {"PENETRACAO_RURAL", type number},
        {"MIX_POUPANCA", type number}
    })
in
    Tipagem
```

**Configuração no Power BI Service (refresh agendado):**

1. Publique o relatório
2. Configurações do dataset → Credenciais → fonte Web → "Anônimo"
3. Agende a atualização

---

## 🔄 Atualização dos dados

### Cooperados

```bash
pip install requests pandas
python pipeline_cooperados.py
```

### ESTBAN Municipal

```bash
pip install requests pandas tqdm
python pipeline_estban.py
```

Opções:
```bash
# Sem push automático
python pipeline_estban.py --no-push

# Período customizado
python pipeline_estban.py --inicio 2024-01 --fim 2025-09
```

## Última atualização

06/02/2026 00:12

---

*Gerado automaticamente pelos pipelines BCB.*
