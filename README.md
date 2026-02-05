# 📊 Dados Públicos - BCB

Repositório de dados públicos consolidados do Banco Central do Brasil, prontos para consumo em ferramentas de BI.

## Datasets disponíveis

### Cooperados por Cooperativa
- **Arquivo:** `dados/bcb/cooperados/cooperados_por_cooperativa.csv`
- **Fonte:** [BCB - Documento 5300](https://www.bcb.gov.br/estabilidadefinanceira/cooperados_cooperativa)
- **Período:** 01/2020 a 12/2025 (72 meses)
- **Separador:** `;`
- **Encoding:** `UTF-8`

**Colunas adicionadas:**
| Coluna | Descrição |
|--------|-----------|
| `CNPJ` | Padronizado com zeros à esquerda (8 dígitos) |
| `Periodo` | Formato `DD/MM/AAAA` (dia fixo = 01) |

## 🔌 Uso no Power BI

**Obter Dados → Consulta em Branco → Editor Avançado:**

```powerquery
let
    Url = "https://raw.githubusercontent.com/mazoir/dados_publicos/main/dados/bcb/cooperados/cooperados_por_cooperativa.csv",
    Fonte = Csv.Document(Web.Contents(Url), [Delimiter=";", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Cabecalho = Table.PromoteHeaders(Fonte, [PromoteAllScalars=true])
in
    Cabecalho
```

**Configuração no Power BI Service (refresh agendado):**
1. Publique o relatório
2. Configurações do dataset → Credenciais → fonte Web → "Anônimo"
3. Agende a atualização

## 🔄 Atualização dos dados

Execute no Codespace:
```bash
pip install requests pandas
python pipeline_cooperados.py
```

## Última atualização
05/02/2026 23:23

---
*Gerado automaticamente pelo pipeline BCB.*
