# RAIS + Novo CAGED (MTE/PDET) → CSV de MG com ETL em PowerShell

Script: [`pipeline_rais_caged.ps1`](../pipeline_rais_caged.ps1), na raiz do repositório.

Ele baixa os microdados oficiais da **RAIS** (vínculos e estabelecimentos) e do **Novo CAGED** (movimentações, declarações fora do prazo e exclusões), de **2020 em diante**, direto do FTP do PDET/MTE. O ETL roda dentro do próprio PowerShell. A saída é um CSV por arquivo de origem, **só com os registros da UF escolhida (padrão: MG)** e com cabeçalho legível. As execuções seguintes são incrementais.

> **Sumário**
> 1. [O que o script entrega](#1-o-que-o-script-entrega)
> 2. [Requisitos](#2-requisitos)
> 3. [Primeira execução](#3-primeira-execução-passo-a-passo)
> 4. [Exemplos de uso](#4-exemplos-de-uso)
> 5. [Parâmetros](#5-parâmetros)
> 6. [Estrutura de saída](#6-estrutura-de-saída)
> 7. [Colunas](#7-colunas)
> 8. [Regras de tratamento (ETL)](#8-regras-de-tratamento-etl)
> 9. [Conferência e qualidade](#9-conferência-e-qualidade)
> 10. [Power BI (Power Query)](#10-power-bi-power-query)
> 11. [SQL Server](#11-sql-server)
> 12. [Agendamento](#12-agendamento-agendador-de-tarefas)
> 13. [Volume, tempo e disco](#13-volume-tempo-e-disco)
> 14. [Solução de problemas](#14-solução-de-problemas)
> 15. [Cuidados analíticos](#15-cuidados-analíticos)
> 16. [VS Code: abrir, rodar e depurar](#16-vs-code-abrir-rodar-e-depurar)

---

## 1. O que o script entrega

| Grupo | Origem no FTP (`ftp://ftp.mtps.gov.br/pdet/microdados`) | Granularidade | Filtro da UF | Arquivo de saída |
| --- | --- | --- | --- | --- |
| `RAIS_VINC` | `RAIS/<ano>/RAIS_VINC_PUB_MG_ES_RJ.7z` (+ `RAIS_VINC_PUB_NI.7z`, quando existe) | 1 linha por vínculo no ano | município do **estabelecimento** começa com `31` | `RAIS\VINCULOS\RAIS_VINC_MG_<ano>.csv` (e `..._<ano>_NI.csv`) |
| `RAIS_ESTAB` | `RAIS/<ano>/RAIS_ESTAB_PUB.7z` | 1 linha por estabelecimento | coluna `UF` = `31` | `RAIS\ESTABELECIMENTOS\RAIS_ESTAB_MG_<ano>.csv` |
| `CAGED_MOV` | `NOVO CAGED/<ano>/<AAAAMM>/CAGEDMOV<AAAAMM>.7z` | 1 linha por movimentação | coluna `uf` = `31` | `CAGED\MOV\CAGEDMOV_MG_<AAAAMM>.csv` |
| `CAGED_FOR` | `NOVO CAGED/<ano>/<AAAAMM>/CAGEDFOR<AAAAMM>.7z` | declarações fora do prazo | coluna `uf` = `31` | `CAGED\FOR\CAGEDFOR_MG_<AAAAMM>.csv` |
| `CAGED_EXC` | `NOVO CAGED/<ano>/<AAAAMM>/CAGEDEXC<AAAAMM>.7z` | exclusões | coluna `uf` = `31` | `CAGED\EXC\CAGEDEXC_MG_<AAAAMM>.csv` |

Como o pipeline funciona, em cinco etapas:

1. **Descoberta.** Lista o FTP e encontra todos os arquivos do período. Anos e meses novos e republicações do MTE são detectados sozinhos. A pasta `Legado` do Novo CAGED é ignorada.
2. **Download.** Roda em paralelo, retoma arquivos `.part` interrompidos (só se o arquivo no FTP ainda for o mesmo: o `.part` leva tamanho e data no nome) e tenta de novo com espera crescente. O tamanho baixado é conferido com o do FTP e a integridade com o CRC do 7-Zip. Se o arquivo crescer no FTP durante o download (republicação), o item fica em erro na hora (`REPUBLICADO_DURANTE_DOWNLOAD`), sem baixar tudo de novo várias vezes, e volta na próxima execução. Se encolher, fica em erro depois das tentativas (tamanho divergente); se mudar mantendo o tamanho, o CRC do 7-Zip acusa e o arquivo é baixado de novo uma vez.
3. **Extração em streaming.** O comando é `7z e -so`. O TXT descompactado, que passa de 5 GB por ano na RAIS de MG/ES/RJ, nunca é gravado em disco.
4. **Transformação.** O motor padrão é C#, compilado em memória via `Add-Type`. O motor PowerShell puro serve de contingência e gera saída byte a byte idêntica. A etapa filtra a UF, unifica os layouts de todos os anos, limpa e normaliza os valores e cria as colunas derivadas.
5. **Carga.** Grava os CSVs em UTF-8 e, em `_controle`, o manifesto com contagens e métricas, o dicionário de colunas, o mapeamento de cabeçalhos e a DDL para SQL Server. Se pedido, consolida tudo em um arquivo por grupo.

Durante a execução, barras de progresso na janela do PowerShell mostram a etapa atual (listagem do FTP, download e ETL, consolidação), o percentual geral, a velocidade, o tempo restante estimado e uma barra por arquivo em andamento (veja [o que aparece na tela](#o-que-aparece-na-tela)).

Só uma execução por vez usa a mesma pasta de saída (trava em `_controle\execucao.lock`): uma execução agendada e outra manual não disputam downloads nem o manifesto. A pasta pode ser movida, renomeada ou acessada por outra letra de unidade ou caminho UNC; os caminhos do manifesto são recalculados a cada execução.

---

## 2. Requisitos

| Item | Detalhe |
| --- | --- |
| PowerShell | **Windows PowerShell 5.1** (Windows 10/11, Server 2016+) com .NET Framework 4.5+, **ou PowerShell 7+** |
| 7-Zip | `7z.exe` instalado (Program Files, registro ou PATH). Sem 7-Zip, use `-Caminho7Zip` ou `-Baixar7Zip`, que baixa o `7zr.exe` oficial de www.7-zip.org para `<Saida>\_ferramentas` |
| Rede | Saída para `ftp.mtps.gov.br`, **porta 21 + portas passivas**. Redes corporativas costumam bloquear FTP; teste antes com `-SomenteListar` |
| Modo de linguagem | `FullLanguage`. Sob AppLocker/WDAC em `ConstrainedLanguage` o script para com mensagem clara |
| Encoding do arquivo | O `.ps1` é **UTF-8 com BOM** e deve continuar assim. Sem o BOM, o Windows PowerShell 5.1 lê os acentos errado. Se editar no Bloco de Notas, salve como "UTF-8 com BOM" |
| Disco | Veja a [seção 13](#13-volume-tempo-e-disco). Com `-CompactarSaida`, a saída fica de 8 a 10 vezes menor |

---

## 3. Primeira execução (passo a passo)

```powershell
# 1) Baixe o script (ou clone o repositório; para usar no VS Code, veja a seção 16).
#    Enquanto o PR não for mesclado, troque "main" por "claude/eager-euler-vzxla1" na URL abaixo.
Invoke-WebRequest -UseBasicParsing -OutFile .\pipeline_rais_caged.ps1 `
    -Uri 'https://raw.githubusercontent.com/mazoir/dados_publicos/main/pipeline_rais_caged.ps1'

# 2) Desbloqueie o arquivo baixado da internet e libere a execução só nesta sessão
Unblock-File .\pipeline_rais_caged.ps1
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# 3) Simulação: mostra o que existe no FTP e quanto será baixado, sem baixar nada
.\pipeline_rais_caged.ps1 -SomenteListar

# 4) Carga completa: RAIS + Novo CAGED, 2020 até hoje, MG
.\pipeline_rais_caged.ps1

# 5) Depois, rode quando quiser: só o que é novo ou republicado é processado
.\pipeline_rais_caged.ps1
```

A ajuda completa fica no próprio script: `Get-Help .\pipeline_rais_caged.ps1 -Full`.

### O que aparece na tela

As mensagens do log rolam normalmente e, junto delas, ficam as barras de progresso. Durante o download e o ETL há uma barra geral e, abaixo dela, uma por arquivo em andamento. No **Windows PowerShell 5.1** o painel fica no topo da janela:

```
 Etapa 2 de 3: download e ETL (MG)
    45% | 14 de 31 arquivos | 3,4 MB/s | faltam ~6 min 30 s
    [ooooooooooooooooooooooooooooooooo                                           ]
    decorrido 5 min 12 s | baixados 1,20 GB | 3 em andamento | 14 na fila
   CAGED_MOV_202401
      baixando 45% | 12,3 MB de 27,1 MB | 2,1 MB/s | faltam ~7 s
      [oooooooooooooooooooooo                                                  ]
   RAIS_VINC_2023_MG_ES_RJ
      processando 38% | 1.234.567 linhas | 198.789 linhas/s | faltam ~1 min 40 s
      [ooooooooooooooooo                                                       ]
```

No **PowerShell 7** cada barra ocupa uma linha, no pé do terminal (o texto fica dentro da barra, que se preenche):

```
Etapa 2 de 3: download e ETL (MG) [45% | 14 de 31 arquivos | 3,4 MB/s | faltam ~6 min 30 s          ]
  CAGED_MOV_202401 [baixando 45% | 12,3 MB de 27,1 MB | 2,1 MB/s | faltam ~7 s                       ]
  RAIS_VINC_2023_MG_ES_RJ [processando 38% | 1.234.567 linhas | 198.789 linhas/s | faltam ~1 min 40 s]
```

- **Percentual geral:** cada arquivo pesa o seu tamanho no FTP; metade é o download e metade o ETL. O tempo restante é uma estimativa pelo ritmo desde o início da etapa ("calculando" nos primeiros segundos).
- **Barra do arquivo:** no download, o percentual baixado e a velocidade; no ETL, o percentual do arquivo descompactado já lido, as linhas lidas e as linhas por segundo. Com o motor PowerShell (contingência), o ETL mostra só as linhas.
- **Listagem do FTP e consolidação** têm barra própria ("Etapa 1 de 3", "Etapa 3 de 3"), com a pasta ou o arquivo da vez.
- **No log:** a cada 60 s (parâmetro `-IntervaloProgresso`) vai uma linha com o mesmo resumo, útil nas execuções agendadas, em que ninguém vê a janela. Cada arquivo concluído traz a contagem (`OK - ... [14 de 31]`) e o fim do download/ETL traz tempo total, erros e volume baixado:

```
2026-11-15 14:05:12 [INFO ] Progresso: 45% | 14 de 31 arquivos | 3,4 MB/s | faltam ~6 min 30 s | baixados 1,20 GB | decorrido 5 min 12 s | em andamento: CAGED_MOV_202401 baixando 45%, RAIS_VINC_2023_MG_ES_RJ processando 38%
2026-11-15 14:11:40 [OK   ] Download e ETL concluídos em 11 min 40 s: 31 de 31 arquivos; 2,71 GB baixados (3,9 MB/s em média).
```

---

## 4. Exemplos de uso

```powershell
# Só o Novo CAGED (movimentações) desde 2024, com um CSV consolidado no final
.\pipeline_rais_caged.ps1 -Saida D:\MTE -Bases CAGED_MOV -AnoInicial 2024 -Consolidar

# RAIS completa (vínculos + estabelecimentos) de 2022 a 2024, saída compactada (.csv.gz)
.\pipeline_rais_caged.ps1 -Bases RAIS -AnoInicial 2022 -AnoFinal 2024 -CompactarSaida

# Para abrir no Excel pt-BR: vírgula decimal e rótulos descritivos no cabeçalho
.\pipeline_rais_caged.ps1 -Bases CAGED -SeparadorDecimal Virgula -Cabecalho Descritivo

# Outra UF (o filtro e o arquivo regional da RAIS são escolhidos sozinhos)
.\pipeline_rais_caged.ps1 -UF SP -Saida D:\MTE_SP

# Reprocessar tudo, reaproveitando os .7z já baixados (conexão direta; pelo proxy eles são baixados de novo)
.\pipeline_rais_caged.ps1 -Reprocessar -ManterCompactados

# Execução não interativa (Agendador de Tarefas / CMD): -Bases como texto separado por vírgula
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\pipeline_rais_caged.ps1 -Bases "RAIS,CAGED_MOV" -Paralelismo 4
```

---

## 5. Parâmetros

| Parâmetro | Padrão | Descrição |
| --- | --- | --- |
| `-Saida` | `$HOME\Dados\MTE` | Pasta raiz de saída |
| `-AnoInicial` | `2020` | Primeiro ano. Na RAIS é o ano-base; no CAGED, o ano da competência do arquivo. Mínimo 2020, quando começa o Novo CAGED |
| `-AnoFinal` | ano corrente | Último ano. A RAIS do ano corrente ainda não existe e é ignorada |
| `-Bases` | `RAIS,CAGED` | `RAIS`, `CAGED` ou os grupos `RAIS_VINC`, `RAIS_ESTAB`, `CAGED_MOV`, `CAGED_FOR`, `CAGED_EXC`. Aceita lista ou texto com vírgulas |
| `-UF` | `MG` | Sigla da UF filtrada (27 UFs) |
| `-Cabecalho` | `Padronizado` | `Padronizado` (snake_case, igual em todos os anos), `Descritivo` (rótulos em português) ou `Original` (cabeçalho do MTE, sem colunas derivadas e sem unificar layouts) |
| `-Delimitador` | `;` | `;`, `,`, `\|` ou `TAB` |
| `-SeparadorDecimal` | `Ponto` | `Ponto` serve para SQL Server, Python e Power Query com cultura en-US; `Virgula` serve para Excel pt-BR |
| `-ManterValoresBrutos` | desligado | Não normaliza valores: só filtra, converte o encoding e reescreve o CSV |
| `-CompactarSaida` | desligado | Grava `.csv.gz` (8 a 10 vezes menor) |
| `-SemBOM` | desligado | UTF-8 sem BOM. O padrão é com BOM, para o Excel reconhecer os acentos |
| `-Consolidar` | desligado | No final, gera um arquivo único por grupo em `<Saida>\CONSOLIDADO`, com todos os períodos OK do manifesto. Se algum período tiver sido gerado com outra configuração (decimal, delimitador, cabeçalho, valores brutos ou versão do layout de saída, após atualizar o script) ou estiver sem o arquivo, o consolidado anterior é mantido e o grupo fica em erro até a base ficar homogênea. A mensagem do log diz o que fazer; em geral, basta rodar uma vez com o período completo (`-AnoInicial 2020`) |
| `-Paralelismo` | `3` | Arquivos processados ao mesmo tempo, de 1 a 8. O FTP do MTE costuma aceitar até 4 conexões |
| `-Reprocessar` | desligado | Ignora o manifesto e reprocessa tudo |
| `-ManterCompactados` | desligado | Mantém os `.7z` em `<Saida>\_downloads` |
| `-Caminho7Zip` | automático | Caminho do `7z.exe` (pode ser relativo à pasta atual) |
| `-Baixar7Zip` | desligado | Se não achar o 7-Zip, baixa o `7zr.exe` oficial |
| `-Motor` | `Auto` | `Auto` usa C# se possível e PowerShell como contingência. Também aceita `CSharp` ou `PowerShell` |
| `-UsarProxySistema` | desligado | Usa o proxy do sistema para o FTP. **Só tem efeito no Windows PowerShell 5.1** (o .NET do PowerShell 7 não usa proxy em FTP). Pelo proxy, o FTP não informa tamanho nem data: um download interrompido recomeça do zero, o download não é conferido pelo tamanho, com `-Reprocessar` os `.7z` guardados por `-ManterCompactados` são baixados de novo e republicações do MTE **não são detectadas** (rode com `-Reprocessar` de tempos em tempos). O total "para baixar" do log e a estimativa de espaço livre não contam esses arquivos (aparecem como "de tamanho desconhecido"). Alternar entre conexão direta e proxy na mesma pasta não marca nada como republicado; só os arquivos processados apenas pelo proxy são reprocessados uma vez, na primeira execução com conexão direta (o manifesto não tinha tamanho e data para comparar) |
| `-FtpBase` | `ftp://ftp.mtps.gov.br/pdet/microdados` | Raiz dos microdados. Num espelho com listagem Unix, um arquivo publicado como link simbólico vem sem tamanho: a republicação só é detectada quando a data do link muda de dia, o download não é conferido pelo tamanho e, se interrompido, recomeça do zero (o log avisa; rode com `-Reprocessar` de tempos em tempos). O FTP do MTE não tem esse problema |
| `-Tentativas` | `5` | Tentativas por listagem ou download, de 1 a 20 |
| `-IntervaloProgresso` | `60` | Segundos entre as linhas "Progresso:" no log e no console, de 0 a 3600 (0 desliga). A barra da janela é atualizada a cada segundo, qualquer que seja o valor |
| `-SomenteListar` | desligado | Simulação: lista o que seria baixado e processado |

**Códigos de saída:**

| Código | Significado |
| --- | --- |
| `0` | Sucesso |
| `1` | Terminou, mas algo ficou pendente: arquivo em erro (veja o manifesto), pasta do FTP que não pôde ser listada, consolidação recusada, manifesto ou mapeamento fora do arquivo principal (gravado só como `.pendente.csv`, ou nem nele) ou mapeamento ilegível |
| `2` | Falha geral, outra execução usando a mesma pasta ou interrupção com Ctrl+C |

---

## 6. Estrutura de saída

```text
<Saida>\
├── RAIS\
│   ├── VINCULOS\           RAIS_VINC_MG_2020.csv ... RAIS_VINC_MG_<ano>.csv  (+ RAIS_VINC_MG_<ano>_NI.csv)
│   └── ESTABELECIMENTOS\   RAIS_ESTAB_MG_2020.csv ...
├── CAGED\
│   ├── MOV\                CAGEDMOV_MG_202001.csv ... (um por competência)
│   ├── FOR\                CAGEDFOR_MG_<AAAAMM>.csv
│   └── EXC\                CAGEDEXC_MG_<AAAAMM>.csv
├── CONSOLIDADO\            (com -Consolidar) ex.: CAGEDMOV_MG_202001_202609.csv
├── _controle\
│   ├── manifesto_MG.csv                 controle incremental + métricas de conferência
│   ├── dicionario_colunas.csv           coluna, rótulo, tipo e origem de cada coluna de saída
│   ├── mapeamento_cabecalhos_MG.csv     linhagem: cabeçalho de origem → coluna de saída, por arquivo
│   ├── ddl_sqlserver_MG.sql             CREATE TABLE de staging + modelo de BULK INSERT
│   ├── execucao.lock                    trava: uma execução por vez nesta pasta
│   ├── *.pendente.csv                   (só se o manifesto/mapeamento estava aberto no Excel; incorporado na próxima execução)
│   └── logs\execucao_MG_<data_hora>_<PID>.log
├── _downloads\             .7z temporários (apagados após o processamento, salvo -ManterCompactados)
└── _ferramentas\           7zr.exe (só com -Baixar7Zip)
```

Formato dos CSVs:

- UTF-8, com BOM por padrão; quebra de linha CRLF; delimitador `;`.
- Ponto decimal por padrão.
- Aspas só quando o valor contém o delimitador, aspas ou quebra de linha, seguindo a RFC 4180.
- A primeira linha é sempre o cabeçalho.

---

## 7. Colunas

No modo **Padronizado** (padrão), as colunas e a ordem são **as mesmas em todos os anos**. Assim dá para empilhar 2020 a 2025 sem ajustar colunas, mesmo com o MTE mudando nomes e layout entre os anos (a RAIS de 2023 em diante foi republicada em um layout novo). Uma coluna que não existe em um ano fica vazia nesse ano. Os **valores** são os publicados em cada ano: alguns códigos de "não se aplica" mudaram no layout 2023+ (veja a [seção 15](#15-cuidados-analíticos) e a coluna `observacao` do `dicionario_colunas.csv`).

Se o MTE publicar uma coluna nova, ainda fora do dicionário, ela **não se perde**. Ela entra no final, com nome em snake_case, e gera um aviso no log e no `mapeamento_cabecalhos_MG.csv`.

No modo **Descritivo**, a ordem é a mesma, com os rótulos em português da coluna "Rótulo" abaixo. No modo **Original**, sai o cabeçalho do arquivo do MTE, sem colunas derivadas e sem unificar layouts entre anos.

Os tipos determinam o tratamento:

- **texto:** só trim e nulos. **Zeros à esquerda são preservados** (CBO, CNAE, natureza jurídica, município, CEP). O CEP numérico de 7 dígitos ganha o zero à esquerda: o layout 2023+ da RAIS Estabelecimentos publica o CEP como número (`7112000` → `07112000`). Outros valores (ex.: `0`) ficam como publicados e, com `-ManterValoresBrutos`, nada muda (seção 8). Em MG os CEPs começam com 3 e não mudam.
- **inteiro** e **decimal:** normalização numérica (seção 8).

#### RAIS Vínculos

| # | Coluna (Padronizado) | Tipo | Rótulo (Descritivo) | Origem |
| ---: | --- | --- | --- | --- |
| 1 | `ano` | inteiro | Ano-base | derivada |
| 2 | `id_municipio` | texto | Município (código IBGE, 7 dígitos) | derivada |
| 3 | `id_municipio_6` | texto | Município do estabelecimento (código IBGE, 6 dígitos) | arquivo |
| 4 | `id_municipio_6_trabalho` | texto | Município de trabalho (código IBGE, 6 dígitos) | arquivo |
| 5 | `bairros_sp` | texto | Bairro (São Paulo) | arquivo |
| 6 | `bairros_fortaleza` | texto | Bairro (Fortaleza) | arquivo |
| 7 | `bairros_rj` | texto | Bairro (Rio de Janeiro) | arquivo |
| 8 | `distritos_sp` | texto | Distrito (São Paulo) | arquivo |
| 9 | `regiao_adm_df` | texto | Região administrativa (DF) | arquivo |
| 10 | `cnae_2_classe` | texto | CNAE 2.0 - classe | arquivo |
| 11 | `cnae_2_subclasse` | texto | CNAE 2.0 - subclasse | arquivo |
| 12 | `cnae_95_classe` | texto | CNAE 95 - classe | arquivo |
| 13 | `ibge_subsetor` | inteiro | Subsetor IBGE | arquivo |
| 14 | `natureza_juridica` | texto | Natureza jurídica | arquivo |
| 15 | `tamanho_estabelecimento` | inteiro | Tamanho do estabelecimento (faixa de empregados) | arquivo |
| 16 | `tipo_estabelecimento` | inteiro | Tipo de estabelecimento (código) | arquivo |
| 17 | `tipo_estabelecimento_nome` | texto | Tipo de estabelecimento (nome) | arquivo |
| 18 | `ind_cei_vinculado` | inteiro | Indicador de CEI vinculado | arquivo |
| 19 | `ind_simples` | inteiro | Indicador de optante pelo Simples | arquivo |
| 20 | `tipo_vinculo` | inteiro | Tipo de vínculo | arquivo |
| 21 | `categoria_trabalhador` | inteiro | Categoria do trabalhador | arquivo |
| 22 | `vinculo_ativo_3112` | inteiro | Vínculo ativo em 31/12 | arquivo |
| 23 | `ind_vinculo_abandonado` | inteiro | Indicador de vínculo abandonado | arquivo |
| 24 | `tipo_admissao` | inteiro | Tipo de admissão | arquivo |
| 25 | `mes_admissao` | inteiro | Mês de admissão | arquivo |
| 26 | `mes_desligamento` | inteiro | Mês de desligamento | arquivo |
| 27 | `motivo_desligamento` | inteiro | Motivo do desligamento | arquivo |
| 28 | `tempo_emprego` | decimal | Tempo de emprego (meses) | arquivo |
| 29 | `faixa_tempo_emprego` | inteiro | Faixa de tempo de emprego | arquivo |
| 30 | `qtd_horas_contratadas` | inteiro | Horas contratadas por semana | arquivo |
| 31 | `faixa_horas_contratadas` | inteiro | Faixa de horas contratadas | arquivo |
| 32 | `ind_trabalho_intermitente` | inteiro | Indicador de trabalho intermitente | arquivo |
| 33 | `ind_trabalho_parcial` | inteiro | Indicador de trabalho parcial | arquivo |
| 34 | `cbo_2002` | texto | Ocupação (CBO 2002) | arquivo |
| 35 | `sexo` | inteiro | Sexo | arquivo |
| 36 | `idade` | inteiro | Idade | arquivo |
| 37 | `faixa_etaria` | inteiro | Faixa etária | arquivo |
| 38 | `raca_cor` | inteiro | Raça/cor | arquivo |
| 39 | `escolaridade_apos_2005` | inteiro | Escolaridade (após 2005) | arquivo |
| 40 | `nacionalidade` | inteiro | Nacionalidade | arquivo |
| 41 | `ano_chegada_brasil` | inteiro | Ano de chegada ao Brasil | arquivo |
| 42 | `ind_pcd` | inteiro | Indicador de pessoa com deficiência | arquivo |
| 43 | `tipo_deficiencia` | inteiro | Tipo de deficiência | arquivo |
| 44 | `vl_remuneracao_dezembro` | decimal | Remuneração de dezembro (R$) | arquivo |
| 45 | `vl_remuneracao_dezembro_sm` | decimal | Remuneração de dezembro (salários mínimos) | arquivo |
| 46 | `faixa_remuneracao_dezembro_sm` | inteiro | Faixa de remuneração de dezembro (SM) | arquivo |
| 47 | `vl_remuneracao_media` | decimal | Remuneração média no ano (R$) | arquivo |
| 48 | `vl_remuneracao_media_sm` | decimal | Remuneração média no ano (salários mínimos) | arquivo |
| 49 | `faixa_remuneracao_media_sm` | inteiro | Faixa de remuneração média (SM) | arquivo |
| 50 | `vl_remuneracao_janeiro` | decimal | Remuneração de janeiro (R$) | arquivo |
| 51 | `vl_remuneracao_fevereiro` | decimal | Remuneração de fevereiro (R$) | arquivo |
| 52 | `vl_remuneracao_marco` | decimal | Remuneração de março (R$) | arquivo |
| 53 | `vl_remuneracao_abril` | decimal | Remuneração de abril (R$) | arquivo |
| 54 | `vl_remuneracao_maio` | decimal | Remuneração de maio (R$) | arquivo |
| 55 | `vl_remuneracao_junho` | decimal | Remuneração de junho (R$) | arquivo |
| 56 | `vl_remuneracao_julho` | decimal | Remuneração de julho (R$) | arquivo |
| 57 | `vl_remuneracao_agosto` | decimal | Remuneração de agosto (R$) | arquivo |
| 58 | `vl_remuneracao_setembro` | decimal | Remuneração de setembro (R$) | arquivo |
| 59 | `vl_remuneracao_outubro` | decimal | Remuneração de outubro (R$) | arquivo |
| 60 | `vl_remuneracao_novembro` | decimal | Remuneração de novembro (R$) | arquivo |
| 61 | `causa_afastamento_1` | inteiro | Causa do afastamento 1 | arquivo |
| 62 | `causa_afastamento_2` | inteiro | Causa do afastamento 2 | arquivo |
| 63 | `causa_afastamento_3` | inteiro | Causa do afastamento 3 | arquivo |
| 64 | `qtd_dias_afastamento` | inteiro | Dias de afastamento no ano | arquivo |

#### RAIS Estabelecimentos

| # | Coluna (Padronizado) | Tipo | Rótulo (Descritivo) | Origem |
| ---: | --- | --- | --- | --- |
| 1 | `ano` | inteiro | Ano-base | derivada |
| 2 | `id_uf` | inteiro | UF (código IBGE) | arquivo |
| 3 | `id_municipio` | texto | Município (código IBGE, 7 dígitos) | derivada |
| 4 | `id_municipio_6` | texto | Município (código IBGE, 6 dígitos) | arquivo |
| 5 | `cep` | texto | CEP do estabelecimento | arquivo |
| 6 | `bairros_sp` | texto | Bairro (São Paulo) | arquivo |
| 7 | `bairros_fortaleza` | texto | Bairro (Fortaleza) | arquivo |
| 8 | `bairros_rj` | texto | Bairro (Rio de Janeiro) | arquivo |
| 9 | `distritos_sp` | texto | Distrito (São Paulo) | arquivo |
| 10 | `regiao_adm_df` | texto | Região administrativa (DF) | arquivo |
| 11 | `cnae_2_classe` | texto | CNAE 2.0 - classe | arquivo |
| 12 | `cnae_2_subclasse` | texto | CNAE 2.0 - subclasse | arquivo |
| 13 | `cnae_95_classe` | texto | CNAE 95 - classe | arquivo |
| 14 | `ibge_subsetor` | inteiro | Subsetor IBGE | arquivo |
| 15 | `natureza_juridica` | texto | Natureza jurídica | arquivo |
| 16 | `tamanho_estabelecimento` | inteiro | Tamanho do estabelecimento (faixa de empregados) | arquivo |
| 17 | `tipo_estabelecimento` | inteiro | Tipo de estabelecimento (código) | arquivo |
| 18 | `tipo_estabelecimento_nome` | texto | Tipo de estabelecimento (nome) | arquivo |
| 19 | `qtd_vinculos_ativos` | inteiro | Vínculos ativos em 31/12 | arquivo |
| 20 | `qtd_vinculos_clt` | inteiro | Vínculos CLT ativos em 31/12 | arquivo |
| 21 | `qtd_vinculos_estatutarios` | inteiro | Vínculos estatutários ativos em 31/12 | arquivo |
| 22 | `ind_atividade_ano` | inteiro | Indicador de atividade no ano | arquivo |
| 23 | `ind_rais_negativa` | inteiro | Indicador de RAIS negativa | arquivo |
| 24 | `ind_cei_vinculado` | inteiro | Indicador de CEI vinculado | arquivo |
| 25 | `ind_pat` | inteiro | Indicador de participante do PAT | arquivo |
| 26 | `ind_simples` | inteiro | Indicador de optante pelo Simples | arquivo |

#### Novo CAGED (MOV, FOR, EXC)

| # | Coluna (Padronizado) | Tipo | Rótulo (Descritivo) | Origem |
| ---: | --- | --- | --- | --- |
| 1 | `competencia_movimentacao` | inteiro | Competência da movimentação (AAAAMM) | arquivo |
| 2 | `competencia_declarada` | inteiro | Competência da declaração (AAAAMM) | arquivo |
| 3 | `competencia_exclusao` | inteiro | Competência da exclusão (AAAAMM) | arquivo (só EXC) |
| 4 | `regiao` | inteiro | Região (código IBGE) | arquivo |
| 5 | `id_uf` | inteiro | UF (código IBGE) | arquivo |
| 6 | `id_municipio` | texto | Município (código IBGE, 7 dígitos) | derivada |
| 7 | `id_municipio_6` | texto | Município (código IBGE, 6 dígitos) | arquivo |
| 8 | `cnae_2_secao` | texto | Seção CNAE 2.0 | arquivo |
| 9 | `cnae_2_subclasse` | texto | Subclasse CNAE 2.0 | arquivo |
| 10 | `saldo_movimentacao` | inteiro | Saldo da movimentação (1 = admissão; -1 = desligamento) | arquivo |
| 11 | `tipo_movimentacao` | inteiro | Tipo de movimentação | arquivo |
| 12 | `cbo_2002` | texto | Ocupação (CBO 2002) | arquivo |
| 13 | `categoria` | inteiro | Categoria do trabalhador | arquivo |
| 14 | `grau_instrucao` | inteiro | Grau de instrução | arquivo |
| 15 | `idade` | inteiro | Idade | arquivo |
| 16 | `horas_contratuais` | decimal | Horas contratuais semanais | arquivo |
| 17 | `raca_cor` | inteiro | Raça/cor | arquivo |
| 18 | `sexo` | inteiro | Sexo | arquivo |
| 19 | `tipo_deficiencia` | inteiro | Tipo de deficiência | arquivo |
| 20 | `tipo_empregador` | inteiro | Tipo de empregador | arquivo |
| 21 | `tipo_estabelecimento` | inteiro | Tipo de estabelecimento | arquivo |
| 22 | `tamanho_estabelecimento_jan` | inteiro | Tamanho do estabelecimento em janeiro (faixa) | arquivo |
| 23 | `ind_trabalho_intermitente` | inteiro | Indicador de trabalho intermitente | arquivo |
| 24 | `ind_trabalho_parcial` | inteiro | Indicador de trabalho parcial | arquivo |
| 25 | `ind_aprendiz` | inteiro | Indicador de aprendiz | arquivo |
| 26 | `salario` | decimal | Salário mensal (R$) | arquivo |
| 27 | `unidade_salario` | inteiro | Unidade do salário fixo (código) | arquivo |
| 28 | `valor_salario_fixo` | decimal | Valor do salário fixo (R$) | arquivo |
| 29 | `origem_informacao` | inteiro | Origem da informação (1 eSocial, 2 CAGED, 3 Empregador Web) | arquivo |
| 30 | `ind_fora_prazo` | inteiro | Indicador de declaração fora do prazo | arquivo |
| 31 | `ind_exclusao` | inteiro | Indicador de exclusão | arquivo (só EXC) |


---

## 8. Regras de tratamento (ETL)

**Formatos de origem**, detectados automaticamente arquivo a arquivo pelo cabeçalho:

| Fonte | Encoding | Delimitador | Decimal | Observação |
| --- | --- | --- | --- | --- |
| RAIS 2020–2022 | Latin-1 | `;` | vírgula, com ponto de milhar | cabeçalho repete "Tipo Estab" (código e nome) |
| RAIS 2023+ (republicada) | Latin-1 | `,` com aspas | ponto | cabeçalhos com sufixo " - Código"; arquivo interno `.COMT` |
| Novo CAGED | UTF-8 | `;` | vírgula | nomes de coluna minúsculos e sem espaço (`saldomovimentação`) |

**Aspas e quebras de linha.** Aspas só têm significado na fonte que as usa (RAIS 2023+: `,` e cabeçalho entre aspas). Nos formatos `;` (RAIS até 2022 e Novo CAGED), uma aspa perdida é um caractere comum e não pode "engolir" as linhas seguintes. Na fonte com aspas, um campo entre aspas pode continuar por até 20 linhas físicas além da primeira; uma aspa sem fechamento até lá é tratada como solta (o campo vai até o fim da linha) e o registro é contado como irregular. Fim de linha CR, LF ou CRLF é aceito.

**Limpeza**, que vale para todas as colunas, salvo `-ManterValoresBrutos`:

- Trim de espaços, incluindo o espaço não separável.
- Viram vazio: `{ñ class}` e qualquer valor iniciado por `{`, sequências de `*` e `n/d`.

**Números** (colunas inteiro e decimal):

| Origem | Saída | Regra |
| --- | --- | --- |
| `0000686,80` | `686.80` | zeros à esquerda removidos; vírgula vira ponto |
| `1.234,56` | `1234.56` | ponto de milhar removido |
| `  599,9` | `599.9` | espaços removidos |
| `,00` | `0.00` | zero antes do separador |
| `044` | `44` | inteiro sem zeros à esquerda |
| `3.0` (RAIS 2023+) | `3` | inteiro sem fração zero |
| `+5` | `5` | sinal `+` removido |
| `-1` | `-1` | negativos preservados (ex.: `saldo_movimentacao`) |

Com `-SeparadorDecimal Virgula`, o ponto decimal vira vírgula na saída.

**CEP** (`cep` da RAIS Estabelecimentos): 7 dígitos viram 8 com o zero à esquerda; outros valores ficam como estão. Desligado com `-ManterValoresBrutos`.

**Colunas derivadas:**

- `ano`: ano-base, só na RAIS.
- `id_municipio`: código IBGE de **7 dígitos**, calculado a partir dos 6 dígitos do MTE. Usa o dígito verificador oficial do IBGE e as 9 exceções da tabela do IBGE: 2201919, 2201988, 2202251, 2611533, 3117836, 3152131, 4305871, 5203939 e 5203962. É a chave para cruzar com malhas e dados do IBGE (geomarketing).

**Filtro da UF:**

- **CAGED e RAIS Estabelecimentos:** igualdade com o código IBGE da UF (MG = `31`).
- **RAIS Vínculos:** prefixo `31` no **município do estabelecimento** (coluna "Município"). O município de trabalho ("Mun Trab") é mantido em `id_municipio_6_trabalho`. O arquivo `NI` (região não identificada) também é lido; se não tiver nenhum registro da UF, fica `SEM_DADOS` no manifesto, o que é normal.

---

## 9. Conferência e qualidade

O `_controle\manifesto_MG.csv` tem uma linha por arquivo de origem. Os campos mais úteis:

| Campo | Para que serve |
| --- | --- |
| `status` | `OK`, `SEM_DADOS` (o arquivo não tem registros da UF; normal no `NI`) ou `ERRO` (veja `mensagem`; a próxima execução tenta de novo) |
| `linhas_lidas` / `linhas_saida` | registros do Brasil (ou da região) lidos e registros da UF gravados |
| `linhas_irregulares` | registros malformados (deveria ser 0): número de campos diferente do cabeçalho ou aspas sem fechamento. Os da UF são todos contados, ajustados e gravados. Fora da UF são contados (e descartados) os curtos demais para ter a coluna do filtro, os com aspas sem fechamento, os com aspas na fonte que usa aspas (RAIS 2023+) e aqueles cuja coluna da UF não traz um código de UF/município válido (sinal de coluna deslocada). Por desempenho, nos formatos `;` (RAIS até 2022 e Novo CAGED), um registro de outra UF com código de UF válido e campos a mais ou a menos é descartado sem contagem. Os 5 primeiros de cada arquivo aparecem no log com o número da linha |
| `metrica_soma` | RAIS Vínculos: soma de `vinculo_ativo_3112`, o **estoque em 31/12**. RAIS Estab.: soma de `qtd_vinculos_ativos`. CAGED: soma de `saldo_movimentacao` |
| `metrica_positivos` / `metrica_negativos` | no CAGED: admissões e desligamentos do arquivo |
| `tamanho_remoto` / `data_remota` | detectam republicação pelo MTE, que leva a novo download |
| `assinatura` | configuração que gerou a saída: versão do layout de saída (`L2` desde a versão 1.1.0 do script), UF, cabeçalho, delimitador, decimal etc. Se a configuração mudar, o arquivo é reprocessado. Por isso, depois de atualizar o script para uma versão com outro layout, cada arquivo é reprocessado uma vez, na primeira execução que o cobre (e baixado de novo se não estiver em `_downloads`; com `-ManterCompactados` os `.7z` ficam guardados). Períodos e bases fora da execução continuam no layout antigo até serem cobertos; até lá, o `-Consolidar` recusa o grupo |

No final da execução, o log mostra um resumo por grupo e a **conferência da carga do Novo CAGED por ano do arquivo** (MOV + FOR − EXC). Ela confere a carga (deve bater com a soma de `saldo_movimentacao` dos CSVs), mas agrupa pelo ano do **nome** do arquivo. O "saldo com ajustes" do painel oficial soma FOR e subtrai EXC na `competencia_movimentacao` de cada linha; para comparar com o painel, use a consulta da [seção 11](#11-sql-server). A soma só do MOV por ano corresponde ao saldo "sem ajustes". O `metrica_soma` da RAIS Vínculos pode ser comparado com o estoque oficial de MG.

O `dicionario_colunas.csv` traz, na coluna `observacao`, as mudanças de significado de códigos entre layouts e como somar o saldo do CAGED.

**Arquivos de controle abertos no Excel** não derrubam a execução. Com o manifesto ou o mapeamento bloqueado, o script avisa, continua os downloads e tenta gravar o arquivo a cada arquivo concluído. Enquanto ele estiver bloqueado, o progresso vai para `<nome>.pendente.csv` (ex.: `manifesto_MG.pendente.csv`), e por isso uma interrupção não perde o que já foi processado. Não abra o `.pendente.csv` durante a execução: se ele também ficar bloqueado, o log avisa e, até um dos dois ser liberado, o progresso fica só na memória. No fim, se o arquivo continuar bloqueado depois de 30 s de novas tentativas, fica o `.pendente.csv` (código de saída 1). A execução seguinte o incorpora sozinha, mesmo quando não há nada novo para processar. Um `.pendente.csv` mais antigo que o principal (ex.: o principal foi salvo depois pelo Excel) não é incorporado: ele fica guardado como `<nome>.pendente_ignorado_<data_hora>.csv` para conferência. Dicionário e DDL bloqueados só geram aviso; eles são regravados na próxima execução.

Para consultar o manifesto ou o mapeamento, abra-os no Excel e **feche sem salvar**. Salvo pelo Excel, o arquivo muda de formato: perde o UTF-8 (acentos de `mensagem` e de caminhos viram `�`), as datas passam a `dd/mm/aaaa` (o script aceita esse formato em `data_remota`) e, se o separador mudar, o arquivo deixa de ser lido. Salvo depois de um `.pendente.csv`, o principal fica mais novo e o pendente deixa de ser incorporado. Um arquivo de controle sem as colunas esperadas não é sobrescrito: o manifesto faz a execução parar com código `2`, e o mapeamento é deixado como está (código `1`). Corrija-o ou renomeie-o (sem o manifesto, tudo é baixado e processado de novo; sem o mapeamento, a linhagem dos arquivos já processados só volta com `-Reprocessar`).

---

## 10. Power BI (Power Query)

Para ler **uma pasta inteira** (todos os meses ou anos de um grupo) em uma tabela: Obter Dados → Consulta em Branco → Editor Avançado.

```m
let
    // ===== Parâmetros (ajuste) =====
    Pasta   = "C:\Users\SEU_USUARIO\Dados\MTE\CAGED\MOV",  // pasta do grupo gerada pelo script
    Prefixo = "CAGEDMOV_MG_",     // RAIS_VINC_MG_ | RAIS_ESTAB_MG_ | CAGEDMOV_MG_ | CAGEDFOR_MG_ | CAGEDEXC_MG_
    Gzip    = false,              // true se o script rodou com -CompactarSaida

    // ===== Leitura =====
    Arquivos  = Folder.Files(Pasta),
    Filtrados = Table.SelectRows(Arquivos, each
                    Text.StartsWith([Name], Prefixo)
                    and Text.EndsWith(Text.Lower([Name]), if Gzip then ".csv.gz" else ".csv")),
    LerCsv    = (conteudo as binary) as table =>
                    let
                        Bruto = if Gzip then Binary.Decompress(conteudo, Compression.GZip) else conteudo,
                        Csv   = Csv.Document(Bruto, [Delimiter = ";", Encoding = 65001, QuoteStyle = QuoteStyle.Csv])
                    in
                        Table.PromoteHeaders(Csv, [PromoteAllScalars = true]),
    Tabelas   = Table.AddColumn(Filtrados, "Dados", each LerCsv([Content])),
    Combinado = Table.Combine(Tabelas[Dados]),

    // ===== Tipagem: cultura en-US porque o script grava decimal com ponto =====
    Tipado    = Table.TransformColumnTypes(Combinado, {
                    {"competencia_movimentacao", Int64.Type},
                    {"id_uf", Int64.Type},
                    {"id_municipio", type text},
                    {"cnae_2_subclasse", type text},
                    {"cbo_2002", type text},
                    {"saldo_movimentacao", Int64.Type},
                    {"idade", Int64.Type},
                    {"salario", type number}
                }, "en-US")
in
    Tipado
```

Boas práticas para volume grande:

- **Tipagem com cultura `"en-US"`.** Com o Power BI em pt-BR e sem esse argumento, `1234.56` vira `123456`.
- **Códigos com zeros à esquerda ficam como texto** no fato e nas dimensões: `cbo_2002`, `cnae_2_subclasse`, `cnae_2_classe`, `natureza_juridica`, `id_municipio`. Assim os relacionamentos não quebram.
- **Corte colunas logo no início** com `Table.SelectColumns`. A RAIS Vínculos tem 64 colunas e de 7 a 9 milhões de linhas por ano em MG. Para estoque, filtre `vinculo_ativo_3112 = 1`.
- **Modelo estrela.** Fatos: vínculos, estabelecimentos e movimentações. Dimensões: município IBGE (`id_municipio`), CBO, CNAE subclasse, calendário (competência AAAAMM ou ano).
- **Volumes grandes.** Para todos os anos da RAIS Vínculos, prefira carregar no SQL Server (seção 11) e importar views agregadas no Power BI.

---

## 11. SQL Server

1. Execute o `_controle\ddl_sqlserver_MG.sql`. Ele cria o schema `stg` e uma tabela por grupo, por exemplo `[stg].[mte_caged_mov_mg]`, no layout padronizado. **Atenção: ele recria as tabelas (DROP + CREATE).**
2. Carregue os arquivos. O caminho precisa ser **visível pelo serviço do SQL Server**: disco do servidor ou compartilhamento com permissão para a conta do serviço. Exige SQL Server 2017+ (`FORMAT = 'CSV'`), saída com ponto decimal e sem `-CompactarSaida`.

```sql
/* Carga de todos os meses do Novo CAGED (MOV) gerados pelo script.
   Ajuste @pasta, @per (primeira competência) e @fim (última). */
SET NOCOUNT ON;
DECLARE @pasta NVARCHAR(400) = N'D:\MTE\CAGED\MOV\';
DECLARE @per   INT = 202001, @fim INT = 202609, @sql NVARCHAR(MAX);

TRUNCATE TABLE [stg].[mte_caged_mov_mg];   -- carga completa (evita duplicar)

WHILE @per <= @fim
BEGIN
    SET @sql = N'BULK INSERT [stg].[mte_caged_mov_mg] FROM ''' + @pasta
             + N'CAGEDMOV_MG_' + CONVERT(NVARCHAR(6), @per) + N'.csv'' WITH (FORMAT = ''CSV'', '
             + N'FIRSTROW = 2, FIELDTERMINATOR = '';'', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', TABLOCK);';
    BEGIN TRY
        EXEC sys.sp_executesql @sql;
        PRINT CONCAT('OK      ', @per);
    END TRY
    BEGIN CATCH
        PRINT CONCAT('FALHOU  ', @per, ' - ', ERROR_MESSAGE());   -- ex.: competência ainda não publicada
    END CATCH;
    SET @per = CASE WHEN @per % 100 = 12 THEN @per + 89 ELSE @per + 1 END;   -- 202012 -> 202101
END;

/* Fatos grandes: columnstore comprime 5-10x e acelera agregações. */
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_mte_caged_mov_mg] ON [stg].[mte_caged_mov_mg];

/* Conferência: no MOV, competencia_movimentacao = competência do arquivo; a soma por ano
   deve bater com a soma de metrica_soma do manifesto (grupo CAGED_MOV) no mesmo ano. */
SELECT competencia_movimentacao / 100 AS ano,
       SUM(CASE WHEN saldo_movimentacao > 0 THEN 1 ELSE 0 END) AS admissoes,
       SUM(CASE WHEN saldo_movimentacao < 0 THEN 1 ELSE 0 END) AS desligamentos,
       SUM(saldo_movimentacao)                                AS saldo
FROM [stg].[mte_caged_mov_mg]
GROUP BY competencia_movimentacao / 100
ORDER BY ano;
```

**Saldo com ajustes** (o do painel oficial): carregue também FOR e EXC (mesmo modelo, trocando `MOV` por `FOR` e `EXC`) e some pela competência da movimentação, com a exclusão subtraindo:

```sql
/* Saldo com ajustes por ano: MOV + FOR - EXC, cada linha na sua competencia_movimentacao. */
SELECT competencia_movimentacao / 100 AS ano,
       SUM(s)                         AS saldo_com_ajustes
FROM (
    SELECT competencia_movimentacao, saldo_movimentacao AS s FROM [stg].[mte_caged_mov_mg]
    UNION ALL
    SELECT competencia_movimentacao, saldo_movimentacao      FROM [stg].[mte_caged_for_mg]
    UNION ALL
    SELECT competencia_movimentacao, -saldo_movimentacao     FROM [stg].[mte_caged_exc_mg]
) AS t
GROUP BY competencia_movimentacao / 100
ORDER BY ano;
```

Esse é o critério do saldo com ajustes do painel. O resultado reflete as declarações publicadas até o último arquivo carregado: cada divulgação traz FOR e EXC que revisam meses anteriores. Se a carga começar depois de 2020 (ex.: `-AnoInicial 2024`), os FOR e EXC de 2024 trazem competências de 2023 incompletas: desconsidere os anos anteriores ao início da carga.

Para a RAIS, use o mesmo modelo de carga, iterando os anos: `RAIS_VINC_MG_<ano>.csv` e, quando existir, `RAIS_VINC_MG_<ano>_NI.csv`.

---

## 12. Agendamento (Agendador de Tarefas)

O Novo CAGED sai todo mês e a RAIS uma vez por ano. Como a execução é incremental, rodar toda semana é barato: quando não há novidade, o script só lista o FTP.

```powershell
$acao = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument (
    '-NoProfile -ExecutionPolicy Bypass -File "C:\Scripts\pipeline_rais_caged.ps1" ' +
    '-Saida "D:\MTE" -Bases "RAIS,CAGED" -Paralelismo 3')
$gatilho = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At '03:00'
Register-ScheduledTask -TaskName 'MTE - RAIS e Novo CAGED (MG)' -Action $acao -Trigger $gatilho `
    -Description 'Atualiza incrementalmente RAIS e Novo CAGED filtrados para MG'
```

O resultado da última execução aparece no Agendador com os códigos da [seção 5](#5-parâmetros): `0` = sucesso, `1` = algo pendente (arquivos em erro, pasta do FTP não listada, consolidação recusada, manifesto ou mapeamento bloqueado ou ilegível), `2` = falha geral, outra execução na mesma pasta ou interrupção. O log detalhado fica em `<Saida>\_controle\logs`. Se a tarefa disparar enquanto uma execução manual ainda roda na mesma pasta, ela sai na hora com código `2`, sem mexer em nada. Cada execução grava o próprio log (o nome leva data, hora e PID). Como a tarefa agendada roda sem janela, o andamento fica nas linhas "Progresso:" do log (a cada `-IntervaloProgresso` segundos); no VS Code, a tarefa "MTE 5 - Acompanhar o log mais recente" mostra essas linhas ao vivo ([seção 16](#16-vs-code-abrir-rodar-e-depurar)).

---

## 13. Volume, tempo e disco

Os números são **ordens de grandeza para MG**. Os exatos de download aparecem no `-SomenteListar` (pelo proxy, o FTP não informa os tamanhos).

| Grupo | Linhas de MG | CSV (sem compactar) |
| --- | --- | --- |
| RAIS Vínculos | ~7–9 milhões por ano | ~2 GB por ano |
| RAIS Estabelecimentos | ~0,8–1 milhão por ano | ~100–150 MB por ano |
| Novo CAGED MOV | ~350–450 mil por mês | ~50 MB por mês |

- **Total de 2020 até hoje:** perto de 15–20 GB de CSV, ou cerca de 2 GB com `-CompactarSaida`.
- **Download:** os `.7z` do Novo CAGED e da RAIS Estabelecimentos são nacionais. Os de RAIS Vínculos são regionais (MG/ES/RJ).
- **Pico de disco:** saída + `Paralelismo` × maior `.7z`. Os `.7z` são apagados depois de processados, salvo com `-ManterCompactados`.
- **Tempo:** a rede costuma ser o gargalo, porque o FTP do MTE é lento em alguns horários. O motor C# processa de 150 a 250 mil linhas por segundo. O motor PowerShell, usado só como contingência, é cerca de 15 a 20 vezes mais lento.

---

## 14. Solução de problemas

| Sintoma | Causa provável e solução |
| --- | --- |
| Não lista o FTP / timeout | A rede bloqueia FTP (porta 21 e portas passivas). Teste `Test-NetConnection ftp.mtps.gov.br -Port 21`. Se só houver proxy, use `-UsarProxySistema` no Windows PowerShell 5.1 (no PowerShell 7 ele não tem efeito); senão, rode de outra rede. O FTP do MTE também fica fora do ar às vezes: rode de novo mais tarde, porque a execução é incremental |
| "7-Zip não encontrado" | Instale o 7-Zip, informe `-Caminho7Zip` ou use `-Baixar7Zip` |
| "-Caminho7Zip ... não existe ou não respondeu como 7-Zip" | O caminho não existe nesta sessão (ex.: unidade de rede que o Agendador não enxerga) ou não é o `7z.exe` (o `7zFM.exe` e o `7zG.exe` abrem janela e são encerrados após 30 s). O script procura outra instalação sozinho |
| "não pode ser carregado porque a execução de scripts foi desabilitada" | `Unblock-File` + `-ExecutionPolicy Bypass` (seção 3) |
| Acentos estranhos nas mensagens | O `.ps1` perdeu o BOM ao ser editado: salve como UTF-8 com BOM. O log em arquivo é sempre UTF-8 |
| Log avisa que o motor C# não compilou | Política de segurança bloqueando o compilador (`csc.exe`) ou falta do .NET Framework 4.5+. O script segue com o motor PowerShell: mesma saída, porém mais lento |
| `status = ERRO` no manifesto | Veja a coluna `mensagem` e o log. A próxima execução tenta de novo só o que ficou pendente. Em `ARQUIVO_CORROMPIDO` o arquivo já é baixado de novo uma vez, sozinho |
| Arquivo de saída em uso | Feche o CSV no Excel ou no Power BI Desktop antes de rodar. Só aquele arquivo fica em erro; a próxima execução tenta de novo |
| "Outra execução do script já está usando a pasta" | Há outra execução (agendada ou manual) na mesma `-Saida`. Espere ela terminar. A trava é liberada sozinha quando o processo antigo termina, mesmo depois de uma queda |
| "Sem permissão para criar ou abrir a trava" | `_controle\execucao.lock` está como somente leitura ou pertence a outra conta (ex.: tarefa agendada com outro usuário). Ajuste as permissões ou apague o arquivo quando nenhuma execução estiver rodando |
| "Manifesto não gravado" / `*.pendente.csv` | O manifesto ou o mapeamento estava aberto (ex.: Excel). Feche-o **sem salvar**; a próxima execução incorpora o `.pendente.csv` sozinha. Não abra o `.pendente.csv` enquanto o script roda |
| "Mapeamento de cabeçalhos desta execução NÃO foi gravado" | O mapeamento e o `.pendente.csv` dele ficaram bloqueados até o fim. A mensagem lista os arquivos sem linhagem: rode com `-Reprocessar` limitado a eles (`-Bases`, `-AnoInicial`, `-AnoFinal`) |
| "... não tem a(s) coluna(s) ..." / "... não tem nenhuma das colunas esperadas ..." | O manifesto ou o mapeamento foi salvo por outro programa (ex.: Excel com outro separador) ou teve o cabeçalho apagado. O arquivo não foi sobrescrito: corrija-o (cabeçalho original, separador `;`) ou renomeie-o. Sem o manifesto, tudo é baixado e processado de novo; sem o mapeamento, a linhagem dos arquivos já processados só volta com `-Reprocessar` |
| `*.pendente_ignorado_<data_hora>.csv` | Um `.pendente.csv` era mais antigo que o arquivo principal (ex.: o principal foi salvo depois pelo Excel) e não foi incorporado. Confira e apague. Os arquivos que só ele registrava já são reprocessados na mesma execução (os de fora da faixa dela, na primeira que os cobrir); a linhagem que só estiver nele (mapeamento) não volta sozinha: copie as linhas para o mapeamento ou use `-Reprocessar` nesses períodos |
| "Consolidação de ... falhou: período(s) gerado(s) com outra configuração" | Parte dos arquivos foi gerada com outro decimal, delimitador ou cabeçalho, ou por uma versão anterior do script (outro layout de saída). Rode uma vez o período completo (`-AnoInicial 2020`) com a configuração desejada; `-Reprocessar` com o mesmo período parcial não resolve. O consolidado anterior foi mantido |
| "Consolidação de ... falhou: ... não está(ão) mais no FTP" | O arquivo daquele período saiu do FTP (retirado ou movido para `Legado`) e a saída antiga não pode ser regenerada. Com o script parado, apague a linha do período em `_controle\manifesto_MG.csv` (no Bloco de Notas) e o CSV correspondente; a consolidação seguinte sai sem ele |
| "Pasta ignorada nesta execução" | Uma pasta de ano/mês do FTP não pôde ser listada. O restante foi processado (código 1); a pasta volta na próxima execução |
| `REPUBLICADO_DURANTE_DOWNLOAD` | O arquivo cresceu no FTP durante a execução (republicação). Ele é baixado de novo na próxima |
| Primeira execução depois de atualizar o script reprocessa tudo | A versão do layout de saída mudou (ex.: 1.0.0 → 1.1.0) e, com ela, a assinatura do manifesto. Cada arquivo é reprocessado uma vez, na primeira execução que o cobre; com `-ManterCompactados` os `.7z` guardados são reaproveitados. Com período parcial (ex.: `-AnoInicial 2024`), os anos de fora ficam no layout antigo e o `-Consolidar` recusa o grupo (código 1) até uma execução com o período completo |
| Ctrl+C | A execução para, os downloads parciais (`.part`) ficam para serem retomados (os de tamanho desconhecido, pelo proxy ou de link simbólico, recomeçam do zero) e os CSVs parciais (`.tmp`) são apagados (um que sobrar sai na execução seguinte). O manifesto e o mapeamento já registraram cada arquivo concluído (salvo se o `.pendente.csv` também estava bloqueado: o log avisa). Código de saída `2` |
| Colunas novas no final do CSV | O MTE publicou uma coluna fora do dicionário. Ela foi preservada; veja `mapeamento_cabecalhos_MG.csv` |
| A barra de progresso não aparece | `$ProgressPreference` está como `SilentlyContinue` na sessão (volte para `Continue`), a saída está redirecionada para arquivo ou a execução é agendada (sem janela). O andamento continua nas linhas "Progresso:" do log |
| O painel de progresso cobre as mensagens (Windows PowerShell 5.1) | É o jeito do 5.1 de desenhar as barras: o painel fica no topo da janela e o log rola por baixo. Aumente a altura da janela ou do terminal, ou rode no PowerShell 7 (`pwsh`), em que cada barra ocupa uma linha |
| A barra do arquivo fica parada em "processando N linhas", sem percentual | Motor PowerShell (contingência, sem o contador de bytes) ou um arquivo cujo tamanho descompactado o 7-Zip não informou. O ETL continua normalmente |
| Tarefas e depuração bloqueadas no VS Code | A pasta está no Modo Restrito. Confie nela (aviso no topo ou "Manage Workspace Trust"); veja a [seção 16](#16-vs-code-abrir-rodar-e-depurar) |

---

## 15. Cuidados analíticos

- **RAIS Vínculos = vínculos do ano, não pessoas nem estoque.** Para o estoque em 31/12, filtre `vinculo_ativo_3112 = 1`. Uma pessoa com dois empregos aparece duas vezes.
- **Território da RAIS Vínculos.** O filtro usa o **município do estabelecimento**. Para análises pelo local de trabalho, use `id_municipio_6_trabalho` (de 2023 em diante, confira a proporção de `999999`, "não informado", antes de usar). A RAIS não traz município de residência.
- **RAIS 2023+ republicada.** O layout novo trouxe colunas que não existiam, como `ind_vinculo_abandonado` e `categoria_trabalhador`, que ficam vazias nos anos anteriores. Confira séries longas com cuidado.
- **RAIS Vínculos 2023+: códigos de "não se aplica" mudaram** (conferido na amostra real de 2025 de MG). `causa_afastamento_1..3`: sem afastamento = `99` até 2022 e `999` de 2023 em diante. `ano_chegada_brasil`: `0` (publicado `0000`) até 2022 e `1198` de 2023 em diante para quem não é estrangeiro; identifique estrangeiros pela `nacionalidade`. Filtros como `causa_afastamento_1 <> 99` ou `ano_chegada_brasil > 0` dão resultado errado se aplicados aos dois períodos sem ajuste.
- **Novo CAGED: competência.** `competencia_movimentacao` é o mês em que a admissão ou o desligamento ocorreu. `competencia_declarada` é o mês em que foi declarado. O arquivo do mês `AAAAMM` traz as declarações daquele mês. O saldo com ajustes do MTE combina MOV, FOR (declarações fora do prazo) e EXC (exclusões), atribuindo FOR e EXC à `competencia_movimentacao` de cada linha, com a EXC de sinal invertido (consulta na [seção 11](#11-sql-server)).
- **Nulos.** `{ñ class}`, `****` e `n/d` viram vazio (NULL), não zero. Códigos como `-1` ("ignorado") são mantidos: trate-os nas dimensões.
- **LGPD.** São microdados públicos e desidentificados do MTE (sem CPF, nome ou CNPJ na versão pública). Mesmo assim, evite cruzamentos que permitam reidentificar pessoas em municípios pequenos.

---

## 16. VS Code: abrir, rodar e depurar

O repositório traz a pasta `.vscode` pronta: tarefas para rodar o pipeline com um clique (com as barras de progresso no terminal do VS Code), configurações de depuração (F5) e as extensões recomendadas.

| Arquivo | O que faz |
| --- | --- |
| `.vscode/tasks.json` | Tarefas "MTE 1" a "MTE 5": simulação, teste rápido, carga completa (CSV ou `.gz`) e acompanhamento do log ao vivo |
| `.vscode/launch.json` | Depuração pela extensão PowerShell: simulação, teste rápido e "com os parâmetros que eu digitar" |
| `.vscode/settings.json` | Mantém os `.ps1` em **UTF-8 com BOM** (obrigatório para o Windows PowerShell 5.1) e o estilo de formatação do script |
| `.vscode/extensions.json` | Recomenda a extensão **PowerShell** (Microsoft) e a **Rainbow CSV** (para ver os CSVs de saída com colunas alinhadas) |

**1. Trazer o projeto para o computador.** Com o Git instalado, num terminal:

```powershell
git clone https://github.com/mazoir/dados_publicos.git
cd dados_publicos
git checkout claude/eager-euler-vzxla1   # enquanto o PR não for mesclado; depois, fique no main
code .
```

Pelo próprio VS Code: `Ctrl+Shift+P` → **Git: Clone** → cole a URL do repositório → abra a pasta → no canto inferior esquerdo, troque o branch para `claude/eager-euler-vzxla1`. Se preferir baixar o ZIP do branch pelo GitHub, extraia, abra a pasta e rode `Get-ChildItem -Recurse | Unblock-File` no terminal, para o Windows não bloquear os arquivos vindos da internet.

**2. Preparar o VS Code.**
- Instale as extensões recomendadas quando o VS Code oferecer (ou procure "PowerShell" da Microsoft na aba de extensões).
- Confie na pasta (aviso de **Modo Restrito** no topo, ou `Ctrl+Shift+P` → **Workspaces: Manage Workspace Trust**). No Modo Restrito, o VS Code não roda tarefas nem depuração.
- Confira na barra de status, com o `pipeline_rais_caged.ps1` aberto, que o encoding é **UTF-8 with BOM**. Se aparecer só "UTF-8", clique nele → **Save with Encoding** → **UTF-8 with BOM**.

**3. Rodar pelas tarefas** (`Terminal` → **Run Task...**, ou `Ctrl+Shift+P` → **Tasks: Run Task**):

| Tarefa | O que faz |
| --- | --- |
| MTE 1 - Simulação | `-SomenteListar`: lista o FTP e mostra o que seria baixado, sem baixar nada. Comece por ela |
| MTE 2 - Teste rápido | Baixa e processa só o CAGED de exclusões de um ano (poucos MB): confere rede, 7-Zip e ETL em poucos minutos |
| MTE 3 - Carga completa (CSV) | Bases e período escolhidos, com consolidação por grupo no final. É incremental: rodar de novo só processa o que é novo |
| MTE 4 - Carga completa (.gz) | Igual à 3, gravando `.csv.gz` (8 a 10 vezes menor) |
| MTE 5 - Acompanhar o log | Mostra o fim do log mais recente e segue as linhas novas (ex.: uma execução agendada em andamento). Feche pela lixeira do terminal |

Ao rodar, o VS Code pergunta, em sequência: qual PowerShell (`powershell.exe` = 5.1, sempre presente no Windows; `pwsh` = 7, se instalado), a pasta de saída, a UF, as bases e o ano inicial; basta Enter para aceitar o padrão. Cada tarefa abre um PowerShell novo num painel próprio do terminal, com o log e as barras de progresso; no fim, o VS Code mostra o código de saída (`0` sucesso, `1` algo pendente, `2` falha: [seção 5](#5-parâmetros)). As tarefas já passam `-ExecutionPolicy Bypass`, então não é preciso mudar a política de execução. Para usar outros parâmetros, rode no terminal do VS Code, por exemplo `.\pipeline_rais_caged.ps1 -Saida D:\MTE -Bases CAGED_MOV -AnoInicial 2024 -Consolidar`.

**4. Depurar (F5).** Na aba **Run and Debug**, escolha uma configuração "MTE" e aperte F5. Dá para pôr pontos de parada no script (clique à esquerda do número da linha) e inspecionar variáveis. A configuração "com os parâmetros que eu digitar" pede a linha de parâmetros. Cada sessão abre um terminal temporário da extensão PowerShell, que usa a versão escolhida na barra de status (clique no número da versão para trocar entre 5.1 e 7). Para cargas longas, prefira as tarefas: a depuração deixa a execução mais lenta.

**5. Ver os resultados.** Abra a pasta de saída no VS Code (`File` → **Open Folder**, ou arraste para a barra lateral) para ver `_controle\manifesto_MG.csv`, os logs e os CSVs (com a Rainbow CSV, as colunas ficam alinhadas). Arquivos de vários GB são lentos no editor: para conferir, prefira o Power Query ou o SQL Server ([seções 10](#10-power-bi-power-query) e [11](#11-sql-server)). Não salve o manifesto nem o mapeamento pelo editor ou pelo Excel ([seção 9](#9-conferência-e-qualidade)).
