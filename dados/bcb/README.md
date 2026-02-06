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
* **Tamanho:** ~45 MB

**Colunas de Identificação:**

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `Período` | date | Data de referência (YYYY-MM-01) |
| `CODMUN` | text | Código do município (BCB) |
| `CNPJ` | text | CNPJ raiz da IF (8 dígitos) |

**Colunas de Crédito (Ativo):**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `Operações de Crédito Total` | 160 | Total de Operações de Crédito |
| `Empréstimos e Títulos Descontados` | 161 | Empréstimos e Títulos Descontados (Capital de Giro) |
| `Financiamentos` | 162 | Financiamentos (Veículos, Bens) |
| `Financiamentos Rurais Agrícola` | 163 | Financiamentos Rurais - Custeio/Investimento Agrícola |
| `Financiamentos Agroindustriais` | 167 | Financiamentos Agroindustriais |
| `Financiamentos Imobiliários` | 169 | Financiamentos Imobiliários |
| `Outras Operações de Crédito` | 171 | Outras Operações de Crédito (PF) |

**Colunas de Risco:**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `Provisão para Créditos de Liquidação Duvidosa` | 174 | Provisão p/ Créditos de Liquidação Duvidosa |

**Colunas de Captação (Passivo):**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `Depósitos à Vista Total` | 401-419 | Depósitos à Vista (consolidado) |
| `Depósitos de Poupança` | 420 | Depósitos de Poupança |
| `Depósitos a Prazo` | 432 | Depósitos a Prazo (CDB/RDB) |

**Colunas Patrimoniais:**

| Coluna | Verbete | Descrição |
| --- | --- | --- |
| `Ativo Total` | 399 | Total do Ativo |
| `Patrimônio Líquido` | 610 | Patrimônio Líquido |

**KPIs Derivados:**

| Coluna | Fórmula | Descrição |
| --- | --- | --- |
| `Índice Provisão / Crédito (%)` | abs(174) / 160 × 100 | Índice de provisão sobre crédito (%) |
| `Penetração Rural (%)` | 163 / 160 × 100 | Participação do crédito rural no total (%) |
| `Mix Poupança (%)` | 420 / (401-419 + 420 + 432) × 100 | Peso da poupança na captação total (%) |

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
    Download = Web.Contents(Url),
    Descomprimido = Binary.Decompress(Download, Compression.GZip),
    Fonte = Csv.Document(Descomprimido, [Delimiter=";", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Cabecalho = Table.PromoteHeaders(Fonte, [PromoteAllScalars=true]),
    Tipagem = Table.TransformColumnTypes(Cabecalho, {
        {"Período", type date},
        {"CODMUN", type text},
        {"CNPJ", type text},
        {"Operações de Crédito Total", type number},
        {"Empréstimos e Títulos Descontados", type number},
        {"Financiamentos", type number},
        {"Financiamentos Rurais Agrícola", type number},
        {"Financiamentos Agroindustriais", type number},
        {"Financiamentos Imobiliários", type number},
        {"Outras Operações de Crédito", type number},
        {"Provisão para Créditos de Liquidação Duvidosa", type number},
        {"Ativo Total", type number},
        {"Depósitos à Vista Total", type number},
        {"Depósitos de Poupança", type number},
        {"Depósitos a Prazo", type number},
        {"Patrimônio Líquido", type number},
        {"Índice Provisão / Crédito (%)", type number},
        {"Penetração Rural (%)", type number},
        {"Mix Poupança (%)", type number}
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

06/02/2026 00:35

---

*Gerado automaticamente pelos pipelines BCB.*
