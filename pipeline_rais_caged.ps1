<#
.SYNOPSIS
    Baixa os microdados da RAIS e do Novo CAGED (MTE/PDET) desde 2020, executa o ETL em PowerShell e
    grava CSVs somente com os registros de uma UF (padrão: MG), com cabeçalho legível.

.DESCRIPTION
    Pipeline completo (Windows PowerShell 5.1 ou PowerShell 7+):

    1. DESCOBERTA  - Lista o FTP oficial do PDET (ftp://ftp.mtps.gov.br/pdet/microdados) e encontra os
                     arquivos do período. Anos/meses novos e republicações do MTE são detectados sozinhos.
                       RAIS ........ /RAIS/<ano>/RAIS_VINC_PUB_<região>.7z  e  RAIS_ESTAB_PUB.7z
                       Novo CAGED .. /NOVO CAGED/<ano>/<ano><mês>/CAGED{MOV|FOR|EXC}<ano><mês>.7z
                     A pasta "Legado" do Novo CAGED (versões antigas até 09/2021) é ignorada.
    2. DOWNLOAD    - Paralelo, com retomada (.part), novas tentativas com espera crescente e conferência
                     do tamanho informado pelo FTP. A integridade é confirmada pelo CRC do 7-Zip.
    3. EXTRAÇÃO    - O .7z é lido em streaming (7z e -so): o TXT descompactado (5+ GB/ano na RAIS de
                     MG/ES/RJ) nunca é gravado em disco.
    4. TRANSFORMAÇÃO (motor C# compilado via Add-Type; motor PowerShell puro como contingência, com
                     saída idêntica):
                     - filtro da UF: CAGED e RAIS Estabelecimentos pela coluna UF; RAIS Vínculos pelo
                       município do estabelecimento (2 primeiros dígitos = código IBGE da UF);
                     - os dois formatos da RAIS: antigo (2020-2022: ';', decimal com vírgula) e o
                       republicado em 2026 (2023+: ',', aspas, decimal com ponto, arquivo .COMT);
                     - encoding detectado automaticamente (RAIS em Latin-1, Novo CAGED em UTF-8);
                     - cabeçalhos padronizados e IGUAIS em todos os anos (snake_case) ou descritivos;
                     - limpeza: trim; "{ñ class}", "****" e "n/d" viram vazio; números normalizados
                       (zeros à esquerda, separador de milhar, separador decimal único);
                     - colunas derivadas: ano-base (RAIS) e código IBGE de 7 dígitos do município.
    5. CARGA       - CSV UTF-8 (BOM opcional), um arquivo por arquivo de origem, manifesto de controle
                     com contagens e métricas de conferência, dicionário de colunas, mapeamento de
                     cabeçalhos, DDL para SQL Server e consolidação opcional por base.

    Execuções seguintes são INCREMENTAIS: só processa o que é novo, foi republicado pelo MTE, teve a
    saída apagada ou foi gerado com outra configuração (delimitador, cabeçalho, decimal etc.).

.PARAMETER Saida
    Pasta raiz de saída. Padrão: <perfil do usuário>\Dados\MTE.

.PARAMETER AnoInicial
    Primeiro ano (RAIS: ano-base; CAGED: ano da competência do arquivo). Mínimo 2020 (início do Novo CAGED).

.PARAMETER AnoFinal
    Último ano. Padrão: ano corrente (a RAIS do ano corrente ainda não existe e é simplesmente ignorada).

.PARAMETER Bases
    RAIS, CAGED (atalhos) ou os grupos RAIS_VINC, RAIS_ESTAB, CAGED_MOV, CAGED_FOR, CAGED_EXC.
    Aceita lista (-Bases RAIS,CAGED_MOV) ou texto separado por vírgula (-Bases "RAIS,CAGED_MOV").

.PARAMETER UF
    Sigla da UF filtrada. Padrão: MG.

.PARAMETER Cabecalho
    Padronizado (snake_case, igual em todos os anos - padrão), Descritivo (rótulos em português) ou
    Original (cabeçalho do arquivo do MTE, sem colunas derivadas e sem unificação entre layouts).

.PARAMETER Delimitador
    Delimitador do CSV de saída: ';' (padrão), ',', '|' ou 'TAB'.

.PARAMETER SeparadorDecimal
    Ponto (padrão; ideal para SQL Server/Python/Power Query com cultura en-US) ou Virgula (Excel pt-BR).

.PARAMETER ManterValoresBrutos
    Não normaliza valores (só filtra, converte encoding e reescreve o CSV). Desliga trim, nulos e números.

.PARAMETER CompactarSaida
    Grava .csv.gz em vez de .csv (8-10x menor; o Power Query lê com Binary.Decompress).

.PARAMETER SemBOM
    Grava UTF-8 sem BOM (o padrão é com BOM, para o Excel reconhecer acentos).

.PARAMETER Consolidar
    Ao final, gera um CSV único por grupo (ex.: CAGEDMOV_MG_202001_202609.csv) em <Saida>\CONSOLIDADO.

.PARAMETER Paralelismo
    Arquivos processados ao mesmo tempo (download + ETL). Padrão 3; o FTP do MTE costuma aceitar até 4.

.PARAMETER Reprocessar
    Ignora o manifesto e reprocessa tudo.

.PARAMETER ManterCompactados
    Mantém os .7z baixados em <Saida>\_downloads (útil para reprocessar sem baixar de novo).

.PARAMETER Caminho7Zip
    Caminho do 7z.exe, se não estiver no local padrão nem no PATH.

.PARAMETER Baixar7Zip
    Se o 7-Zip não for encontrado, baixa o executável oficial autônomo (7zr.exe) de www.7-zip.org.

.PARAMETER Motor
    Auto (padrão: C# se possível, senão PowerShell), CSharp ou PowerShell.

.PARAMETER UsarProxySistema
    Acessa o FTP pelo proxy do sistema (por padrão a conexão FTP é direta). Funciona no Windows
    PowerShell 5.1; nesse modo a retomada de download pode não estar disponível.

.PARAMETER FtpBase
    Raiz dos microdados no FTP. Padrão: ftp://ftp.mtps.gov.br/pdet/microdados

.PARAMETER Tentativas
    Tentativas por download/listagem antes de desistir. Padrão 5.

.PARAMETER SomenteListar
    Só mostra o que existe no FTP e o que seria baixado/processado (simulação), sem baixar nada.

.EXAMPLE
    .\pipeline_rais_caged.ps1
    RAIS + Novo CAGED de 2020 até o ano corrente, UF = MG, saída em %USERPROFILE%\Dados\MTE.

.EXAMPLE
    .\pipeline_rais_caged.ps1 -SomenteListar
    Mostra os arquivos disponíveis e o volume a baixar, sem baixar nada.

.EXAMPLE
    .\pipeline_rais_caged.ps1 -Saida D:\MTE -Bases CAGED_MOV -AnoInicial 2024 -Consolidar

.EXAMPLE
    powershell.exe -ExecutionPolicy Bypass -File .\pipeline_rais_caged.ps1 -Paralelismo 4 -CompactarSaida
    Execução agendada (Agendador de Tarefas), saída compactada.

.NOTES
    Versão ...... 1.0.0
    Requisitos .. PowerShell 5.1+ (.NET Framework 4.5+) ou PowerShell 7+; 7-Zip (ou -Baixar7Zip);
                  saída liberada para ftp.mtps.gov.br (porta 21 + portas passivas); modo FullLanguage.
    Encoding .... Este arquivo é UTF-8 COM BOM (obrigatório para o Windows PowerShell 5.1 ler os acentos).
    Códigos de saída: 0 = sucesso; 1 = terminou com arquivos em erro; 2 = falha geral.
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]$Saida = (Join-Path -Path $HOME -ChildPath (Join-Path -Path 'Dados' -ChildPath 'MTE')),

    [ValidateRange(2020, 2100)]
    [int]$AnoInicial = 2020,

    [ValidateRange(2020, 2100)]
    [int]$AnoFinal = (Get-Date).Year,

    # Aceita lista do PowerShell (-Bases RAIS,CAGED_MOV) e texto separado por vírgula, que é como o valor
    # chega via "powershell.exe -File" (Agendador de Tarefas). Validado no início da execução.
    [ArgumentCompleter({ 'RAIS', 'CAGED', 'RAIS_VINC', 'RAIS_ESTAB', 'CAGED_MOV', 'CAGED_FOR', 'CAGED_EXC' })]
    [string[]]$Bases = @('RAIS', 'CAGED'),

    [ValidateSet('AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA',
        'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO')]
    [string]$UF = 'MG',

    [ValidateSet('Padronizado', 'Descritivo', 'Original')]
    [string]$Cabecalho = 'Padronizado',

    [ValidateSet(';', ',', '|', 'TAB')]
    [string]$Delimitador = ';',

    [ValidateSet('Ponto', 'Virgula')]
    [string]$SeparadorDecimal = 'Ponto',

    [switch]$ManterValoresBrutos,
    [switch]$CompactarSaida,
    [switch]$SemBOM,
    [switch]$Consolidar,

    [ValidateRange(1, 8)]
    [int]$Paralelismo = 3,

    [switch]$Reprocessar,
    [switch]$ManterCompactados,
    [string]$Caminho7Zip,
    [switch]$Baixar7Zip,

    [ValidateSet('Auto', 'CSharp', 'PowerShell')]
    [string]$Motor = 'Auto',

    [switch]$UsarProxySistema,
    [string]$FtpBase = 'ftp://ftp.mtps.gov.br/pdet/microdados',

    [ValidateRange(1, 20)]
    [int]$Tentativas = 5,

    [switch]$SomenteListar
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

#region ============================ 1. CONFIGURAÇÃO ESTÁTICA ============================

$Script:VersaoScript = '1.0.0'
# Versão do layout de saída: mudar este valor força o reprocessamento de tudo (use ao alterar o dicionário).
$Script:VersaoLayout = '1'
$Script:Inicio = Get-Date
$Script:ArquivoLog = $null
$Script:Utf8ComBom = New-Object System.Text.UTF8Encoding -ArgumentList $true

$Script:CodigoUF = @{
    RO = '11'; AC = '12'; AM = '13'; RR = '14'; PA = '15'; AP = '16'; TO = '17'
    MA = '21'; PI = '22'; CE = '23'; RN = '24'; PB = '25'; PE = '26'; AL = '27'; SE = '28'; BA = '29'
    MG = '31'; ES = '32'; RJ = '33'; SP = '35'
    PR = '41'; SC = '42'; RS = '43'
    MS = '50'; MT = '51'; GO = '52'; DF = '53'
}

# Arquivo regional de vínculos da RAIS (desde 2018) que contém cada UF. O arquivo "NI" (região não
# identificada, desde 2022) também é lido: é pequeno e qualquer registro da UF que estiver nele entra.
$Script:RegiaoRais = @{
    RO = 'NORTE'; AC = 'NORTE'; AM = 'NORTE'; RR = 'NORTE'; PA = 'NORTE'; AP = 'NORTE'; TO = 'NORTE'
    MA = 'NORDESTE'; PI = 'NORDESTE'; CE = 'NORDESTE'; RN = 'NORDESTE'; PB = 'NORDESTE'
    PE = 'NORDESTE'; AL = 'NORDESTE'; SE = 'NORDESTE'; BA = 'NORDESTE'
    MG = 'MG_ES_RJ'; ES = 'MG_ES_RJ'; RJ = 'MG_ES_RJ'; SP = 'SP'
    PR = 'SUL'; SC = 'SUL'; RS = 'SUL'
    MS = 'CENTRO_OESTE'; MT = 'CENTRO_OESTE'; GO = 'CENTRO_OESTE'; DF = 'CENTRO_OESTE'
}

# Metadados de cada grupo (unidade de saída).
#   Filtro  : colunas padronizadas usadas para filtrar a UF, em ordem de preferência
#             (id_uf = igualdade com o código IBGE; id_municipio_6 = prefixo do código do município).
#   Metrica : coluna somada no manifesto para conferência com os números oficiais.
$Script:MetaGrupos = [ordered]@{
    RAIS_VINC  = @{ Base = 'RAIS'; Pasta = @('RAIS', 'VINCULOS'); Prefixo = 'RAIS_VINC'; Dicionario = 'RAIS_VINC'
        Metrica = 'vinculo_ativo_3112'; Filtro = @('id_municipio_6'); EncodingPadrao = 'iso-8859-1'; Tabela = 'mte_rais_vinculos'
    }
    RAIS_ESTAB = @{ Base = 'RAIS'; Pasta = @('RAIS', 'ESTABELECIMENTOS'); Prefixo = 'RAIS_ESTAB'; Dicionario = 'RAIS_ESTAB'
        Metrica = 'qtd_vinculos_ativos'; Filtro = @('id_uf', 'id_municipio_6'); EncodingPadrao = 'iso-8859-1'; Tabela = 'mte_rais_estabelecimentos'
    }
    CAGED_MOV  = @{ Base = 'CAGED'; Pasta = @('CAGED', 'MOV'); Prefixo = 'CAGEDMOV'; Dicionario = 'CAGED'
        Metrica = 'saldo_movimentacao'; Filtro = @('id_uf', 'id_municipio_6'); EncodingPadrao = 'utf-8'; Tabela = 'mte_caged_mov'
    }
    CAGED_FOR  = @{ Base = 'CAGED'; Pasta = @('CAGED', 'FOR'); Prefixo = 'CAGEDFOR'; Dicionario = 'CAGED'
        Metrica = 'saldo_movimentacao'; Filtro = @('id_uf', 'id_municipio_6'); EncodingPadrao = 'utf-8'; Tabela = 'mte_caged_for'
    }
    CAGED_EXC  = @{ Base = 'CAGED'; Pasta = @('CAGED', 'EXC'); Prefixo = 'CAGEDEXC'; Dicionario = 'CAGED'
        Metrica = 'saldo_movimentacao'; Filtro = @('id_uf', 'id_municipio_6'); EncodingPadrao = 'utf-8'; Tabela = 'mte_caged_exc'
    }
}

# ------------------------------------------------------------------------------------------
# DICIONÁRIO DE COLUNAS (fonte única para os dois motores, para o dicionário e para a DDL)
# Formato: coluna_padronizada | tipo | grupos | chaves | rótulo descritivo
#   tipo   : T = texto/código (só trim; preserva zeros à esquerda), I = inteiro, D = decimal
#   grupos : vazio = todos os grupos do dicionário; senão, lista (ex.: EXC)
#   chaves : nomes de origem normalizados (sem acento, minúsculos, só [a-z0-9], sem o sufixo
#            " - Código"). A MESMA chave em duas linhas atende cabeçalhos repetidos, na ordem em que
#            aparecem (a RAIS até 2022 traz "Tipo Estab" duas vezes: código e nome).
#            "=ano" e "=id_municipio" marcam colunas derivadas.
# Cobertura conferida: RAIS Vínculos 2020-2022 (60 colunas) e 2023-2025 republicada (62);
# RAIS Estabelecimentos 2014-2022 (24) e 2023-2025 (23); Novo CAGED MOV/FOR (28) e EXC (30).
# ------------------------------------------------------------------------------------------
$Script:DefRaisVinc = @'
ano                           |I| |=ano                                         |Ano-base
id_municipio                  |T| |=id_municipio                                |Município (código IBGE, 7 dígitos)
id_municipio_6                |T| |municipio                                    |Município do estabelecimento (código IBGE, 6 dígitos)
id_municipio_6_trabalho       |T| |muntrab,municipiotrab                        |Município de trabalho (código IBGE, 6 dígitos)
bairros_sp                    |T| |bairrossp                                    |Bairro (São Paulo)
bairros_fortaleza             |T| |bairrosfortaleza                             |Bairro (Fortaleza)
bairros_rj                    |T| |bairrosrj                                    |Bairro (Rio de Janeiro)
distritos_sp                  |T| |distritossp                                  |Distrito (São Paulo)
regiao_adm_df                 |T| |regioesadmdf,regiaoadmdf                     |Região administrativa (DF)
cnae_2_classe                 |T| |cnae20classe                                 |CNAE 2.0 - classe
cnae_2_subclasse              |T| |cnae20subclasse                              |CNAE 2.0 - subclasse
cnae_95_classe                |T| |cnae95classe                                 |CNAE 95 - classe
ibge_subsetor                 |I| |ibgesubsetor                                 |Subsetor IBGE
natureza_juridica             |T| |naturezajuridica                             |Natureza jurídica
tamanho_estabelecimento       |I| |tamanhoestabelecimento                       |Tamanho do estabelecimento (faixa de empregados)
tipo_estabelecimento          |I| |tipoestab,tipoestabelecimento                |Tipo de estabelecimento (código)
tipo_estabelecimento_nome     |T| |tipoestab,tipoestabelecimentonome            |Tipo de estabelecimento (nome)
ind_cei_vinculado             |I| |indceivinculado                              |Indicador de CEI vinculado
ind_simples                   |I| |indsimples,indestabelecimentoparticipantesimples|Indicador de optante pelo Simples
tipo_vinculo                  |I| |tipovinculo                                  |Tipo de vínculo
categoria_trabalhador         |I| |categoriatrabalhador                         |Categoria do trabalhador
vinculo_ativo_3112            |I| |vinculoativo3112,indvinculoativo3112         |Vínculo ativo em 31/12
ind_vinculo_abandonado        |I| |indvinculoabandonado                         |Indicador de vínculo abandonado
tipo_admissao                 |I| |tipoadmissao,tipoadmissaotrabalhador         |Tipo de admissão
mes_admissao                  |I| |mesadmissao                                  |Mês de admissão
mes_desligamento              |I| |mesdesligamento                              |Mês de desligamento
motivo_desligamento           |I| |motivodesligamento                           |Motivo do desligamento
tempo_emprego                 |D| |tempoemprego                                 |Tempo de emprego (meses)
faixa_tempo_emprego           |I| |faixatempoemprego                            |Faixa de tempo de emprego
qtd_horas_contratadas         |I| |qtdhoracontr                                 |Horas contratadas por semana
faixa_horas_contratadas       |I| |faixahoracontrat                             |Faixa de horas contratadas
ind_trabalho_intermitente     |I| |indtrabintermitente,indtrabalhointermitente  |Indicador de trabalho intermitente
ind_trabalho_parcial          |I| |indtrabparcial,indtrabalhoparcial            |Indicador de trabalho parcial
cbo_2002                      |T| |cboocupacao2002,cbo2002ocupacao              |Ocupação (CBO 2002)
sexo                          |I| |sexotrabalhador,sexo                         |Sexo
idade                         |I| |idade                                        |Idade
faixa_etaria                  |I| |faixaetaria                                  |Faixa etária
raca_cor                      |I| |racacor                                      |Raça/cor
escolaridade_apos_2005        |I| |escolaridadeapos2005                         |Escolaridade (após 2005)
nacionalidade                 |I| |nacionalidade                                |Nacionalidade
ano_chegada_brasil            |I| |anochegadabrasil                             |Ano de chegada ao Brasil
ind_pcd                       |I| |indportadordefic                             |Indicador de pessoa com deficiência
tipo_deficiencia              |I| |tipodefic,tipodeficiencia                    |Tipo de deficiência
vl_remuneracao_dezembro       |D| |vlremundezembronom,vlremdezembronom          |Remuneração de dezembro (R$)
vl_remuneracao_dezembro_sm    |D| |vlremundezembrosm,vlremdezembrosm            |Remuneração de dezembro (salários mínimos)
faixa_remuneracao_dezembro_sm |I| |faixaremundezemsm,faixaremdezsm              |Faixa de remuneração de dezembro (SM)
vl_remuneracao_media          |D| |vlremunmedianom,vlremmedianom                |Remuneração média no ano (R$)
vl_remuneracao_media_sm       |D| |vlremunmediasm,vlremmediasm                  |Remuneração média no ano (salários mínimos)
faixa_remuneracao_media_sm    |I| |faixaremunmediasm,faixaremmediasm            |Faixa de remuneração média (SM)
vl_remuneracao_janeiro        |D| |vlremjaneirosc,vlremjaneirocc                |Remuneração de janeiro (R$)
vl_remuneracao_fevereiro      |D| |vlremfevereirosc,vlremfevereirocc            |Remuneração de fevereiro (R$)
vl_remuneracao_marco          |D| |vlremmarcosc,vlremmarcocc                    |Remuneração de março (R$)
vl_remuneracao_abril          |D| |vlremabrilsc,vlremabrilcc                    |Remuneração de abril (R$)
vl_remuneracao_maio           |D| |vlremmaiosc,vlremmaiocc                      |Remuneração de maio (R$)
vl_remuneracao_junho          |D| |vlremjunhosc,vlremjunhocc                    |Remuneração de junho (R$)
vl_remuneracao_julho          |D| |vlremjulhosc,vlremjulhocc                    |Remuneração de julho (R$)
vl_remuneracao_agosto         |D| |vlremagostosc,vlremagostocc                  |Remuneração de agosto (R$)
vl_remuneracao_setembro       |D| |vlremsetembrosc,vlremsetembrocc              |Remuneração de setembro (R$)
vl_remuneracao_outubro        |D| |vlremoutubrosc,vlremoutubrocc                |Remuneração de outubro (R$)
vl_remuneracao_novembro       |D| |vlremnovembrosc,vlremnovembrocc              |Remuneração de novembro (R$)
causa_afastamento_1           |I| |causaafastamento1                            |Causa do afastamento 1
causa_afastamento_2           |I| |causaafastamento2                            |Causa do afastamento 2
causa_afastamento_3           |I| |causaafastamento3                            |Causa do afastamento 3
qtd_dias_afastamento          |I| |qtddiasafastamento                           |Dias de afastamento no ano
'@

$Script:DefRaisEstab = @'
ano                           |I| |=ano                                         |Ano-base
id_uf                         |I| |uf                                           |UF (código IBGE)
id_municipio                  |T| |=id_municipio                                |Município (código IBGE, 7 dígitos)
id_municipio_6                |T| |municipio                                    |Município (código IBGE, 6 dígitos)
cep                           |T| |cepestab                                     |CEP do estabelecimento
bairros_sp                    |T| |bairrossp                                    |Bairro (São Paulo)
bairros_fortaleza             |T| |bairrosfortaleza                             |Bairro (Fortaleza)
bairros_rj                    |T| |bairrosrj                                    |Bairro (Rio de Janeiro)
distritos_sp                  |T| |distritossp                                  |Distrito (São Paulo)
regiao_adm_df                 |T| |regioesadmdf,regiaoadmdf                     |Região administrativa (DF)
cnae_2_classe                 |T| |cnae20classe                                 |CNAE 2.0 - classe
cnae_2_subclasse              |T| |cnae20subclasse                              |CNAE 2.0 - subclasse
cnae_95_classe                |T| |cnae95classe                                 |CNAE 95 - classe
ibge_subsetor                 |I| |ibgesubsetor                                 |Subsetor IBGE
natureza_juridica             |T| |naturezajuridica                             |Natureza jurídica
tamanho_estabelecimento       |I| |tamanhoestabelecimento                       |Tamanho do estabelecimento (faixa de empregados)
tipo_estabelecimento          |I| |tipoestab,tipoestabelecimento                |Tipo de estabelecimento (código)
tipo_estabelecimento_nome     |T| |tipoestab,tipoestabelecimentonome            |Tipo de estabelecimento (nome)
qtd_vinculos_ativos           |I| |qtdvinculosativos                            |Vínculos ativos em 31/12
qtd_vinculos_clt              |I| |qtdvinculosclt                               |Vínculos CLT ativos em 31/12
qtd_vinculos_estatutarios     |I| |qtdvinculosestatutarios                      |Vínculos estatutários ativos em 31/12
ind_atividade_ano             |I| |indatividadeano                              |Indicador de atividade no ano
ind_rais_negativa             |I| |indraisnegativa                              |Indicador de RAIS negativa
ind_cei_vinculado             |I| |indceivinculado                              |Indicador de CEI vinculado
ind_pat                       |I| |indestabparticipapat,indestabparticipantepat |Indicador de participante do PAT
ind_simples                   |I| |indsimples,indestabparticipantesimples       |Indicador de optante pelo Simples
'@

$Script:DefCaged = @'
competencia_movimentacao      |I|    |competenciamov                            |Competência da movimentação (AAAAMM)
competencia_declarada         |I|    |competenciadec                            |Competência da declaração (AAAAMM)
competencia_exclusao          |I|EXC |competenciaexc                            |Competência da exclusão (AAAAMM)
regiao                        |I|    |regiao                                    |Região (código IBGE)
id_uf                         |I|    |uf                                        |UF (código IBGE)
id_municipio                  |T|    |=id_municipio                             |Município (código IBGE, 7 dígitos)
id_municipio_6                |T|    |municipio                                 |Município (código IBGE, 6 dígitos)
cnae_2_secao                  |T|    |secao                                     |Seção CNAE 2.0
cnae_2_subclasse              |T|    |subclasse                                 |Subclasse CNAE 2.0
saldo_movimentacao            |I|    |saldomovimentacao                         |Saldo da movimentação (1 = admissão; -1 = desligamento)
tipo_movimentacao             |I|    |tipomovimentacao                          |Tipo de movimentação
cbo_2002                      |T|    |cbo2002ocupacao                           |Ocupação (CBO 2002)
categoria                     |I|    |categoria                                 |Categoria do trabalhador
grau_instrucao                |I|    |graudeinstrucao                           |Grau de instrução
idade                         |I|    |idade                                     |Idade
horas_contratuais             |D|    |horascontratuais                          |Horas contratuais semanais
raca_cor                      |I|    |racacor                                   |Raça/cor
sexo                          |I|    |sexo                                      |Sexo
tipo_deficiencia              |I|    |tipodedeficiencia                         |Tipo de deficiência
tipo_empregador               |I|    |tipoempregador                            |Tipo de empregador
tipo_estabelecimento          |I|    |tipoestabelecimento                       |Tipo de estabelecimento
tamanho_estabelecimento_jan   |I|    |tamestabjan                               |Tamanho do estabelecimento em janeiro (faixa)
ind_trabalho_intermitente     |I|    |indtrabintermitente                       |Indicador de trabalho intermitente
ind_trabalho_parcial          |I|    |indtrabparcial                            |Indicador de trabalho parcial
ind_aprendiz                  |I|    |indicadoraprendiz                         |Indicador de aprendiz
salario                       |D|    |salario                                   |Salário mensal (R$)
unidade_salario               |I|    |unidadesalariocodigo                      |Unidade do salário fixo (código)
valor_salario_fixo            |D|    |valorsalariofixo                          |Valor do salário fixo (R$)
origem_informacao             |I|    |origemdainformacao                        |Origem da informação (1 eSocial, 2 CAGED, 3 Empregador Web)
ind_fora_prazo                |I|    |indicadordeforadoprazo                    |Indicador de declaração fora do prazo
ind_exclusao                  |I|EXC |indicadordeexclusao                       |Indicador de exclusão
'@

# Funções copiadas para os runspaces de trabalho (não podem depender de variáveis $Script:).
$Script:FuncoesWorker = @(
    'Write-LogFila', 'New-FtpRequest', 'Invoke-FtpDownload', 'Start-Processo7z', 'Invoke-Processo',
    'Get-EntradasArquivo', 'Test-ArquivoIntegro', 'Read-AmostraEntrada', 'Get-EncodingAmostra', 'Get-CabecalhoAmostra',
    'Get-DelimitadorCabecalho', 'ConvertFrom-LinhaCsv', 'Remove-Acentos', 'ConvertTo-ChaveCabecalho',
    'ConvertTo-Slug', 'New-PlanoEtl', 'ConvertTo-PlanoCSharp', 'Get-MunicipioIbge7', 'New-EscritorCsv',
    'ConvertTo-LinhaCsvSaida', 'Invoke-MotorPowerShell', 'Invoke-EtlArquivo', 'Invoke-ProcessarItem'
)

#endregion

#region ============================ 2. MOTOR C# (Add-Type) ============================
# Compatível com C# 5 (compilador do .NET Framework usado pelo Windows PowerShell 5.1) e com o Roslyn
# do PowerShell 7. Mantido só com ASCII de propósito. As regras de normalização são as mesmas do motor
# PowerShell (Invoke-MotorPowerShell): os dois produzem arquivos byte a byte idênticos.
$Script:CodigoCSharp = @'
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;

namespace EtlMteV1
{
    public sealed class PlanoEtl
    {
        public int NumCamposOrigem;
        public char DelimitadorOrigem = ';';
        public bool DecimalVirgulaOrigem = true;
        public int IndiceFiltro = -1;
        public bool FiltroPorPrefixo;
        public string ValorFiltro = "";
        public char[] TiposOrigem = new char[0];
        // Saida -> origem: >= 0 indice da coluna de origem; -1 vazio; -2 id_municipio derivado;
        // <= -10 constante (indice = -10 - valor).
        public int[] Plano = new int[0];
        public string[] Constantes = new string[0];
        public int IndiceMunicipio6 = -1;
        public int IndiceMetrica = -1;
        public char DecimalSaida = '.';
        public bool ManterBrutos;
    }

    public sealed class ResultadoEtl
    {
        public long LinhasLidas;
        public long LinhasSaida;
        public long LinhasIrregulares;
        public decimal MetricaSoma;
        public long MetricaPositivos;
        public long MetricaNegativos;
        public List<string> Avisos = new List<string>();
    }

    public static class Normalizacao
    {
        // Mesmo conjunto do \s das expressoes regulares do .NET.
        public static bool EhEspaco(char c)
        {
            if (c == ' ' || (c >= '\t' && c <= '\r') || c == '\u0085') return true;
            if (c < '\u00A0') return false;
            UnicodeCategory cat = CharUnicodeInfo.GetUnicodeCategory(c);
            return cat == UnicodeCategory.SpaceSeparator || cat == UnicodeCategory.LineSeparator
                || cat == UnicodeCategory.ParagraphSeparator;
        }

        public static string Aparar(string v)
        {
            int ini = 0, fim = v.Length - 1;
            while (ini <= fim && EhEspaco(v[ini])) ini++;
            while (fim >= ini && EhEspaco(v[fim])) fim--;
            if (ini == 0 && fim == v.Length - 1) return v;
            return v.Substring(ini, fim - ini + 1);
        }

        // (?i)^(?:\{[^\r\n]*|\*+|n/d)\z  ->  "{n class}", "******", "n/d"
        public static bool EhNulo(string v)
        {
            int n = v.Length;
            if (n == 0) return false;
            char c0 = v[0];
            if (c0 == '{')
            {
                for (int i = 1; i < n; i++) { if (v[i] == '\r' || v[i] == '\n') return false; }
                return true;
            }
            if (c0 == '*')
            {
                for (int i = 1; i < n; i++) { if (v[i] != '*') return false; }
                return true;
            }
            return n == 3 && (v[0] == 'n' || v[0] == 'N') && v[1] == '/' && (v[2] == 'd' || v[2] == 'D');
        }

        // Normalizacao numerica (N1..N9). decimalVirgula = formato de origem com virgula decimal e
        // ponto de milhar (RAIS ate 2022 e Novo CAGED); false = ponto decimal (RAIS republicada).
        public static string Numero(string v, bool inteiro, bool decimalVirgula)
        {
            if (v.Length == 0) return v;
            // Caminho rapido (caso mais comum): so digitos ASCII -> apenas remove zeros a esquerda.
            // Equivalente a cadeia completa abaixo para esse tipo de valor.
            bool soDigitos = true;
            for (int i = 0; i < v.Length; i++) { char c = v[i]; if (c < '0' || c > '9') { soDigitos = false; break; } }
            if (soDigitos)
            {
                if (v.Length == 1 || v[0] != '0') return v;
                int z = 0;
                while (z < v.Length - 1 && v[z] == '0') z++;
                return v.Substring(z);
            }
            bool temEspaco = false;
            for (int i = 0; i < v.Length; i++) { if (EhEspaco(v[i])) { temEspaco = true; break; } }
            if (temEspaco)
            {
                StringBuilder sb = new StringBuilder(v.Length);
                for (int i = 0; i < v.Length; i++) { if (!EhEspaco(v[i])) sb.Append(v[i]); }
                v = sb.ToString();
                if (v.Length == 0) return v;
            }
            if (decimalVirgula)
            {
                if (v.IndexOf('.') >= 0 && v.IndexOf(',') >= 0) v = RemoverPontosAntesDeVirgula(v);
                if (v.IndexOf('.') >= 0 && EhMilharPuro(v)) v = v.Replace(".", "");
                if (v.IndexOf(',') >= 0) v = v.Replace(',', '.');
            }
            else
            {
                int ic = v.IndexOf(',');
                if (ic >= 0 && EhVirgulaUnicaNumerica(v, ic)) v = v.Substring(0, ic) + "." + v.Substring(ic + 1);
            }
            v = RemoverZerosEsquerda(v);
            int s = (v.Length > 0 && (v[0] == '+' || v[0] == '-')) ? 1 : 0;
            if (v.Length > s + 1 && v[s] == '.' && char.IsDigit(v[s + 1])) v = v.Substring(0, s) + "0" + v.Substring(s);
            if (v.Length >= 2 && v[v.Length - 1] == '.' && char.IsDigit(v[v.Length - 2])) v = v.Substring(0, v.Length - 1);
            if (v.Length >= 2 && v[0] == '+' && (char.IsDigit(v[1]) || v[1] == '.')) v = v.Substring(1);
            if (inteiro) v = RemoverFracaoZero(v);
            return v;
        }

        private static string RemoverPontosAntesDeVirgula(string v)
        {
            StringBuilder sb = new StringBuilder(v.Length);
            int n = v.Length;
            for (int i = 0; i < n; i++)
            {
                char c = v[i];
                if (c == '.')
                {
                    int j = i + 1;
                    while (j < n && (char.IsDigit(v[j]) || v[j] == '.')) j++;
                    if (j < n && v[j] == ',') continue;
                }
                sb.Append(c);
            }
            return sb.ToString();
        }

        private static bool EhMilharPuro(string v)
        {
            int n = v.Length, i = 0;
            if (i < n && (v[i] == '+' || v[i] == '-')) i++;
            int d = 0;
            while (i < n && char.IsDigit(v[i])) { d++; i++; }
            if (d < 1 || d > 3) return false;
            int grupos = 0;
            while (i < n)
            {
                if (v[i] != '.') return false;
                i++;
                int k = 0;
                while (i < n && k < 3 && char.IsDigit(v[i])) { k++; i++; }
                if (k != 3) return false;
                grupos++;
            }
            return grupos > 0;
        }

        private static bool EhVirgulaUnicaNumerica(string v, int ic)
        {
            int i = (v.Length > 0 && (v[0] == '+' || v[0] == '-')) ? 1 : 0;
            for (int k = i; k < ic; k++) { if (!char.IsDigit(v[k])) return false; }
            for (int k = ic + 1; k < v.Length; k++) { if (!char.IsDigit(v[k])) return false; }
            return true;
        }

        private static string RemoverZerosEsquerda(string v)
        {
            int s = (v.Length > 0 && (v[0] == '+' || v[0] == '-')) ? 1 : 0;
            int z = 0;
            while (s + z < v.Length && v[s + z] == '0') z++;
            if (z == 0) return v;
            int remover = (s + z < v.Length && char.IsDigit(v[s + z])) ? z : z - 1;
            if (remover <= 0) return v;
            return v.Substring(0, s) + v.Substring(s + remover);
        }

        private static string RemoverFracaoZero(string v)
        {
            int p = v.IndexOf('.');
            if (p < 0 || p == v.Length - 1) return v;
            int i = (v[0] == '+' || v[0] == '-') ? 1 : 0;
            if (p - i < 1) return v;
            for (int k = i; k < p; k++) { if (!char.IsDigit(v[k])) return v; }
            for (int k = p + 1; k < v.Length; k++) { if (v[k] != '0') return v; }
            return v.Substring(0, p);
        }

        public static string PontoParaVirgula(string v)
        {
            int p = v.IndexOf('.');
            if (p < 0 || p == v.Length - 1) return v;
            int i = (v[0] == '+' || v[0] == '-') ? 1 : 0;
            if (p - i < 1) return v;
            for (int k = i; k < p; k++) { if (!char.IsDigit(v[k])) return v; }
            for (int k = p + 1; k < v.Length; k++) { if (!char.IsDigit(v[k])) return v; }
            return v.Substring(0, p) + "," + v.Substring(p + 1);
        }

        // Codigo IBGE de 7 digitos (com digito verificador) a partir do codigo de 6 digitos.
        public static string Municipio7(string m6)
        {
            if (m6 == null || m6.Length != 6) return "";
            for (int i = 0; i < 6; i++) { if (m6[i] < '0' || m6[i] > '9') return ""; }
            switch (m6.Substring(0, 2))
            {
                case "11": case "12": case "13": case "14": case "15": case "16": case "17":
                case "21": case "22": case "23": case "24": case "25": case "26": case "27": case "28": case "29":
                case "31": case "32": case "33": case "35": case "41": case "42": case "43":
                case "50": case "51": case "52": case "53":
                    break;
                default:
                    return "";
            }
            switch (m6)
            {
                case "220191": return "2201919";
                case "220198": return "2201988";
                case "220225": return "2202251";
                case "261153": return "2611533";
                case "311783": return "3117836";
                case "315213": return "3152131";
                case "430587": return "4305871";
                case "520393": return "5203939";
                case "520396": return "5203962";
            }
            int soma = 0;
            for (int i = 0; i < 6; i++)
            {
                int p = (m6[i] - '0') * ((i % 2) + 1);
                if (p > 9) p -= 9;
                soma += p;
            }
            return m6 + ((10 - (soma % 10)) % 10).ToString(CultureInfo.InvariantCulture);
        }

        // Parser CSV tolerante (RFC 4180 + aspas fora do inicio do campo tratadas como literais).
        // Retorna false se o registro termina dentro de aspas (campo com quebra de linha).
        public static bool ParseRegistro(string s, char d, List<string> campos, StringBuilder sb)
        {
            campos.Clear();
            sb.Length = 0;
            bool emAspas = false, inicio = true;
            int n = s.Length;
            for (int i = 0; i < n; i++)
            {
                char c = s[i];
                if (emAspas)
                {
                    if (c == '"')
                    {
                        if (i + 1 < n && s[i + 1] == '"') { sb.Append('"'); i++; }
                        else emAspas = false;
                    }
                    else sb.Append(c);
                }
                else if (c == d) { campos.Add(sb.ToString()); sb.Length = 0; inicio = true; }
                else if (c == '"' && inicio) { emAspas = true; inicio = false; }
                else { sb.Append(c); inicio = false; }
            }
            campos.Add(sb.ToString());
            return !emAspas;
        }
    }

    public sealed class Processador : IDisposable
    {
        private StreamWriter _w;
        private readonly char _delimSaida;
        private readonly ResultadoEtl _r = new ResultadoEtl();
        private readonly List<string> _campos = new List<string>(128);
        private readonly StringBuilder _sb = new StringBuilder(256);
        private readonly StringBuilder _linha = new StringBuilder(4096);
        private readonly Dictionary<string, string> _cacheMun = new Dictionary<string, string>(StringComparer.Ordinal);
        private long _lidas;
        private volatile bool _cancelar;

        public bool Cancelar { get { return _cancelar; } set { _cancelar = value; } }
        public long LinhasLidas { get { return Interlocked.Read(ref _lidas); } }

        public Processador(string caminhoSaida, string[] cabecalhoSaida, char delimitadorSaida, bool gzip, bool bom)
        {
            _delimSaida = delimitadorSaida;
            Stream fs = new FileStream(caminhoSaida, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 20);
            Stream destino = gzip ? (Stream)new GZipStream(fs, CompressionMode.Compress) : fs;
            _w = new StreamWriter(destino, new UTF8Encoding(bom), 1 << 20);
            _w.NewLine = "\r\n";
            _linha.Length = 0;
            for (int j = 0; j < cabecalhoSaida.Length; j++)
            {
                if (j > 0) _linha.Append(_delimSaida);
                AnexarCampo(_linha, cabecalhoSaida[j] ?? "");
            }
            _w.WriteLine(_linha.ToString());
        }

        public void ProcessarStream(Stream entrada, string nomeEncoding, PlanoEtl p)
        {
            if (_w == null) throw new ObjectDisposedException("Processador");
            Encoding enc = Encoding.GetEncoding(nomeEncoding);
            int n = p.NumCamposOrigem;
            char d = p.DelimitadorOrigem;
            bool brutos = p.ManterBrutos;
            bool decVirgSaida = p.DecimalSaida == ',' && !brutos;
            string[] valores = new string[Math.Max(n, 1)];
            int[] plano = p.Plano;
            using (StreamReader sr = new StreamReader(entrada, enc, false, 1 << 20))
            {
                string linha = sr.ReadLine(); // cabecalho (o plano ja foi montado a partir dele)
                if (linha == null) return;
                long numLinha = 1;
                while ((linha = sr.ReadLine()) != null)
                {
                    numLinha++;
                    if (linha.Length == 0) continue;
                    long lidas = ++_lidas;
                    if ((lidas & 0x3FFF) == 0 && _cancelar) throw new OperationCanceledException("Processamento cancelado.");
                    long linhaInicio = numLinha;
                    bool temAspas = linha.IndexOf('"') >= 0;
                    string vFiltro;
                    if (!temAspas)
                    {
                        vFiltro = CampoSimples(linha, p.IndiceFiltro, d);
                    }
                    else
                    {
                        string reg = linha;
                        while (!Normalizacao.ParseRegistro(reg, d, _campos, _sb))
                        {
                            string prox = sr.ReadLine();
                            if (prox == null) break;
                            numLinha++;
                            reg = reg + "\n" + prox;
                        }
                        vFiltro = p.IndiceFiltro >= 0 && p.IndiceFiltro < _campos.Count ? _campos[p.IndiceFiltro] : null;
                    }
                    if (vFiltro == null) continue;
                    vFiltro = vFiltro.Trim();
                    bool passa = p.FiltroPorPrefixo
                        ? vFiltro.StartsWith(p.ValorFiltro, StringComparison.Ordinal)
                        : string.Equals(vFiltro, p.ValorFiltro, StringComparison.Ordinal);
                    if (!passa) continue;
                    if (!temAspas) DividirSimples(linha, d, _campos);
                    if (_campos.Count != n)
                    {
                        _r.LinhasIrregulares++;
                        if (_r.LinhasIrregulares <= 5)
                            _r.Avisos.Add("Linha " + linhaInicio.ToString(CultureInfo.InvariantCulture) + ": "
                                + _campos.Count.ToString(CultureInfo.InvariantCulture) + " campos (esperado "
                                + n.ToString(CultureInfo.InvariantCulture) + "); registro ajustado.");
                    }
                    for (int i = 0; i < n; i++)
                    {
                        string v = i < _campos.Count ? _campos[i] : "";
                        if (!brutos)
                        {
                            v = Normalizacao.Aparar(v);
                            if (Normalizacao.EhNulo(v)) v = "";
                            char t = p.TiposOrigem[i];
                            if (t == 'I' || t == 'D') v = Normalizacao.Numero(v, t == 'I', p.DecimalVirgulaOrigem);
                        }
                        valores[i] = v;
                    }
                    if (p.IndiceMetrica >= 0)
                    {
                        string mv = valores[p.IndiceMetrica];
                        if (brutos)
                        {
                            mv = Normalizacao.Aparar(mv);
                            if (Normalizacao.EhNulo(mv)) mv = "";
                            mv = Normalizacao.Numero(mv, p.TiposOrigem[p.IndiceMetrica] == 'I', p.DecimalVirgulaOrigem);
                        }
                        decimal dv = 0m;
                        bool numerico;
                        if (mv == "1") { dv = 1m; numerico = true; }
                        else if (mv == "0") { dv = 0m; numerico = true; }
                        else if (mv == "-1") { dv = -1m; numerico = true; }
                        else numerico = mv.Length > 0 && decimal.TryParse(mv, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                            CultureInfo.InvariantCulture, out dv);
                        if (numerico)
                        {
                            _r.MetricaSoma += dv;
                            if (dv > 0) _r.MetricaPositivos++;
                            else if (dv < 0) _r.MetricaNegativos++;
                        }
                    }
                    string idMun = "";
                    if (p.IndiceMunicipio6 >= 0)
                    {
                        string m6 = valores[p.IndiceMunicipio6];
                        if (brutos) m6 = Normalizacao.Aparar(m6);
                        if (!_cacheMun.TryGetValue(m6, out idMun))
                        {
                            idMun = Normalizacao.Municipio7(m6);
                            _cacheMun[m6] = idMun;
                        }
                    }
                    _linha.Length = 0;
                    for (int j = 0; j < plano.Length; j++)
                    {
                        if (j > 0) _linha.Append(_delimSaida);
                        int k = plano[j];
                        string v;
                        if (k >= 0)
                        {
                            v = k < n ? valores[k] : "";
                            if (decVirgSaida && k < n)
                            {
                                char t = p.TiposOrigem[k];
                                if (t == 'I' || t == 'D') v = Normalizacao.PontoParaVirgula(v);
                            }
                        }
                        else if (k == -1) v = "";
                        else if (k == -2) v = idMun;
                        else
                        {
                            int c = -10 - k;
                            v = (c >= 0 && c < p.Constantes.Length) ? (p.Constantes[c] ?? "") : "";
                        }
                        AnexarCampo(_linha, v);
                    }
                    _w.WriteLine(_linha.ToString());
                    _r.LinhasSaida++;
                }
            }
        }

        public ResultadoEtl Finalizar()
        {
            if (_w != null) { _w.Flush(); _w.Dispose(); _w = null; }
            _r.LinhasLidas = Interlocked.Read(ref _lidas);
            return _r;
        }

        public void Dispose()
        {
            if (_w != null) { try { _w.Dispose(); } catch { } _w = null; }
        }

        private void AnexarCampo(StringBuilder sb, string v)
        {
            bool precisa = false, aspas = false;
            for (int i = 0; i < v.Length; i++)
            {
                char c = v[i];
                if (c == '"') { aspas = true; precisa = true; }
                else if (c == _delimSaida || c == '\r' || c == '\n') precisa = true;
            }
            if (!precisa) { sb.Append(v); return; }
            sb.Append('"');
            sb.Append(aspas ? v.Replace("\"", "\"\"") : v);
            sb.Append('"');
        }

        private static string CampoSimples(string s, int idx, char d)
        {
            if (idx < 0) return null;
            int ini = 0;
            for (int j = 0; j < idx; j++)
            {
                int pos = s.IndexOf(d, ini);
                if (pos < 0) return null;
                ini = pos + 1;
            }
            int fim = s.IndexOf(d, ini);
            if (fim < 0) fim = s.Length;
            return s.Substring(ini, fim - ini);
        }

        private static void DividirSimples(string s, char d, List<string> campos)
        {
            campos.Clear();
            int ini = 0;
            while (true)
            {
                int pos = s.IndexOf(d, ini);
                if (pos < 0) { campos.Add(s.Substring(ini)); break; }
                campos.Add(s.Substring(ini, pos - ini));
                ini = pos + 1;
            }
        }
    }
}
'@

#endregion

#region ============================ 3. FUNÇÕES UTILITÁRIAS ============================

function Write-Log {
    <# Log no console (colorido) e no arquivo de log da execução (UTF-8). #>
    param(
        [Parameter(Mandatory = $true)][AllowEmptyString()][string]$Mensagem,
        [ValidateSet('INFO', 'AVISO', 'ERRO', 'OK', 'DEBUG')][string]$Nivel = 'INFO',
        [string]$Id = ''
    )
    $prefixo = ''
    if ($Id) { $prefixo = '[' + $Id + '] ' }
    $linha = '{0} [{1}] {2}{3}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Nivel.PadRight(5), $prefixo, $Mensagem
    $cor = 'Gray'
    if ($Nivel -eq 'ERRO') { $cor = 'Red' } elseif ($Nivel -eq 'AVISO') { $cor = 'Yellow' } elseif ($Nivel -eq 'OK') { $cor = 'Green' } elseif ($Nivel -eq 'DEBUG') { $cor = 'DarkGray' }
    Write-Host $linha -ForegroundColor $cor
    if ($Script:ArquivoLog) {
        [System.IO.File]::AppendAllText($Script:ArquivoLog, $linha + [Environment]::NewLine, $Script:Utf8ComBom)
    }
}

function Write-LogFila {
    <# Usado nos runspaces: enfileira a mensagem; a thread principal grava no console e no arquivo. #>
    param($Fila, [string]$Nivel, [string]$Id, [string]$Mensagem)
    if ($null -ne $Fila) { $Fila.Enqueue(('{0}|{1}|{2}' -f $Nivel, $Id, $Mensagem)) }
}

function Remove-Acentos {
    param([string]$Texto)
    if ([string]::IsNullOrEmpty($Texto)) { return '' }
    $decomposto = $Texto.Normalize([System.Text.NormalizationForm]::FormD)
    $sb = New-Object System.Text.StringBuilder -ArgumentList $decomposto.Length
    foreach ($c in $decomposto.ToCharArray()) {
        if ([System.Globalization.CharUnicodeInfo]::GetUnicodeCategory($c) -ne [System.Globalization.UnicodeCategory]::NonSpacingMark) {
            [void]$sb.Append($c)
        }
    }
    return $sb.ToString().Normalize([System.Text.NormalizationForm]::FormC)
}

function ConvertTo-ChaveCabecalho {
    <# "Município - Código" -> "municipio"; "Vl Remun Média (SM)" -> "vlremunmediasm". #>
    param([string]$Texto)
    if ($null -eq $Texto) { return '' }
    $s = (Remove-Acentos ($Texto.Replace([string][char]0xFEFF, ''))).Trim().ToLowerInvariant()
    $s = [regex]::Replace($s, '\s*-\s*codigo\s*$', '')
    return [regex]::Replace($s, '[^a-z0-9]', '')
}

function ConvertTo-Slug {
    <# Nome de coluna seguro (snake_case ASCII) para colunas não previstas no dicionário. #>
    param([string]$Texto)
    if ($null -eq $Texto) { $Texto = '' }
    $s = (Remove-Acentos ($Texto.Replace([string][char]0xFEFF, ''))).Trim().ToLowerInvariant()
    $s = [regex]::Replace($s, '[^a-z0-9]+', '_').Trim('_')
    if ($s.Length -eq 0) { $s = 'coluna' }
    if ($s -match '^\d') { $s = 'c_' + $s }
    return $s
}

function ConvertFrom-LinhaCsv {
    <# Parser CSV idêntico a Normalizacao.ParseRegistro (C#). Completa = $false se terminar dentro de aspas. #>
    param([string]$Texto, [char]$Delimitador)
    $campos = New-Object System.Collections.Generic.List[string]
    $sb = New-Object System.Text.StringBuilder
    $emAspas = $false
    $inicio = $true
    $n = $Texto.Length
    $i = 0
    while ($i -lt $n) {
        $c = $Texto[$i]
        if ($emAspas) {
            if ($c -ceq '"') {
                if ((($i + 1) -lt $n) -and ($Texto[$i + 1] -ceq '"')) { [void]$sb.Append('"'); $i++ }
                else { $emAspas = $false }
            }
            else { [void]$sb.Append($c) }
        }
        elseif ($c -ceq $Delimitador) { $campos.Add($sb.ToString()); [void]$sb.Clear(); $inicio = $true }
        elseif (($c -ceq '"') -and $inicio) { $emAspas = $true; $inicio = $false }
        else { [void]$sb.Append($c); $inicio = $false }
        $i++
    }
    $campos.Add($sb.ToString())
    return New-Object psobject -Property @{ Campos = $campos.ToArray(); Completa = (-not $emAspas) }
}

function ConvertTo-LinhaCsvSaida {
    <# Monta uma linha CSV com aspas apenas quando necessário (mesma regra do motor C#). #>
    param([object[]]$Valores, [char]$Delimitador)
    $re = '(?s)^(.*[' + [regex]::Escape([string]$Delimitador) + '"\r\n].*)\z'
    $v = @(@($Valores) -replace '"', '""' -replace $re, '"$1"')
    return [string]::Join([string]$Delimitador, $v)
}

function Get-MunicipioIbge7 {
    <# Código IBGE de 7 dígitos (algoritmo do DV + 9 exceções oficiais). Idêntico a Normalizacao.Municipio7. #>
    param([string]$Codigo6)
    if ($null -eq $Codigo6 -or $Codigo6 -cnotmatch '^[0-9]{6}\z') { return '' }
    $ufs = @('11', '12', '13', '14', '15', '16', '17', '21', '22', '23', '24', '25', '26', '27', '28', '29',
        '31', '32', '33', '35', '41', '42', '43', '50', '51', '52', '53')
    if ($ufs -notcontains $Codigo6.Substring(0, 2)) { return '' }
    $excecoes = @{
        '220191' = '2201919'; '220198' = '2201988'; '220225' = '2202251'; '261153' = '2611533'; '311783' = '3117836'
        '315213' = '3152131'; '430587' = '4305871'; '520393' = '5203939'; '520396' = '5203962'
    }
    if ($excecoes.ContainsKey($Codigo6)) { return $excecoes[$Codigo6] }
    $soma = 0
    for ($i = 0; $i -lt 6; $i++) {
        $p = ([int]$Codigo6[$i] - 48) * (($i % 2) + 1)
        if ($p -gt 9) { $p -= 9 }
        $soma += $p
    }
    return $Codigo6 + [string]((10 - ($soma % 10)) % 10)
}

function Join-FtpUrl {
    param([string]$Base, [string[]]$Segmentos)
    $u = $Base.TrimEnd('/')
    foreach ($s in $Segmentos) { $u += '/' + [System.Uri]::EscapeDataString($s) }
    return $u
}

function Format-Bytes {
    param([double]$Bytes)
    if ($Bytes -ge 1GB) { return ('{0:N2} GB' -f ($Bytes / 1GB)) }
    if ($Bytes -ge 1MB) { return ('{0:N1} MB' -f ($Bytes / 1MB)) }
    return ('{0:N0} KB' -f ($Bytes / 1KB))
}

#endregion

#region ============================ 4. FTP ============================

function New-FtpRequest {
    param([string]$Url, [string]$Metodo, [bool]$UsarProxy)
    $req = [System.Net.FtpWebRequest][System.Net.WebRequest]::Create($Url)
    $req.Method = $Metodo
    $req.UseBinary = $true
    $req.UsePassive = $true
    $req.KeepAlive = $false
    $req.Timeout = 120000
    $req.ReadWriteTimeout = 300000
    $req.Credentials = New-Object System.Net.NetworkCredential -ArgumentList 'anonymous', 'anonymous@'
    if ($UsarProxy) {
        $px = [System.Net.WebRequest]::GetSystemWebProxy()
        $px.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
        $req.Proxy = $px
    }
    else {
        $req.Proxy = $null
    }
    return $req
}

function ConvertFrom-ListagemFtp {
    <# Interpreta LIST nos formatos MS-DOS (IIS, usado pelo MTE) e Unix; HTML quando passa por proxy HTTP. #>
    param([string]$Texto)
    $itens = New-Object System.Collections.Generic.List[object]
    $inv = [System.Globalization.CultureInfo]::InvariantCulture
    $estilo = [System.Globalization.DateTimeStyles]::AllowWhiteSpaces
    $fmtDos = [string[]]@('MM-dd-yy hh:mmtt', 'MM-dd-yy h:mmtt', 'MM-dd-yyyy hh:mmtt', 'MM-dd-yyyy h:mmtt', 'MM-dd-yy HH:mm', 'MM-dd-yyyy HH:mm')
    $fmtUnix = [string[]]@('MMM d HH:mm', 'MMM d H:mm', 'MMM d yyyy')
    if ($Texto -match '(?i)<a\s') {
        foreach ($m in [regex]::Matches($Texto, '(?i)<a\s[^>]*href="(?<h>[^"?#]+)"')) {
            $h = [System.Uri]::UnescapeDataString($m.Groups['h'].Value)
            $ehDir = $h.EndsWith('/')
            $nome = @($h.TrimEnd('/') -split '/')[-1]
            if ($nome -and $nome -ne '.' -and $nome -ne '..') {
                $itens.Add([pscustomobject]@{ Nome = $nome; EhDiretorio = $ehDir; Tamanho = [long]-1; Data = '' })
            }
        }
        return $itens.ToArray()
    }
    foreach ($linha in ($Texto -split "`r?`n")) {
        if ($linha.Trim().Length -eq 0) { continue }
        $m = [regex]::Match($linha, '^(?<d>\d{2}-\d{2}-\d{2,4})\s+(?<h>\d{1,2}:\d{2}\s*(?:[AaPp][Mm])?)\s+(?<t><DIR>|\d+)\s+(?<n>.+?)\s*$')
        if ($m.Success) {
            $dt = [datetime]::MinValue
            $txt = ($m.Groups['d'].Value + ' ' + ($m.Groups['h'].Value -replace '\s', '')).ToUpperInvariant()
            $data = $txt
            if ([datetime]::TryParseExact($txt, $fmtDos, $inv, $estilo, [ref]$dt)) { $data = $dt.ToString('yyyy-MM-dd HH:mm', $inv) }
            $ehDir = ($m.Groups['t'].Value -eq '<DIR>')
            $tam = [long]-1
            if (-not $ehDir) { $tam = [long]$m.Groups['t'].Value }
            $itens.Add([pscustomobject]@{ Nome = $m.Groups['n'].Value; EhDiretorio = $ehDir; Tamanho = $tam; Data = $data })
            continue
        }
        $m = [regex]::Match($linha, '^(?<tp>[-dlbcps])[-rwxsStTlL]{9}\S*\s+\d+\s+\S+\s+\S+\s+(?<t>\d+)\s+(?<d>[A-Za-z]{3}\s+\d{1,2}\s+(?:\d{1,2}:\d{2}|\d{4}))\s+(?<n>.+?)\s*$')
        if ($m.Success) {
            $nome = $m.Groups['n'].Value
            if ($m.Groups['tp'].Value -eq 'l') { $nome = @($nome -split ' -> ')[0] }
            $dt = [datetime]::MinValue
            $txt = ($m.Groups['d'].Value -replace '\s+', ' ')
            $data = $txt
            if ([datetime]::TryParseExact($txt, $fmtUnix, $inv, $estilo, [ref]$dt)) { $data = $dt.ToString('yyyy-MM-dd HH:mm', $inv) }
            $ehDir = ($m.Groups['tp'].Value -eq 'd')
            $tam = [long]-1
            if (-not $ehDir) { $tam = [long]$m.Groups['t'].Value }
            $itens.Add([pscustomobject]@{ Nome = $nome; EhDiretorio = $ehDir; Tamanho = $tam; Data = $data })
        }
    }
    return $itens.ToArray()
}

function Get-FtpListagem {
    param([string]$Url, [bool]$UsarProxy, [int]$Tentativas = 4)
    if (-not $Url.EndsWith('/')) { $Url += '/' }
    $ultimo = $null
    for ($t = 1; $t -le $Tentativas; $t++) {
        $resp = $null
        try {
            $req = New-FtpRequest -Url $Url -Metodo ([System.Net.WebRequestMethods+Ftp]::ListDirectoryDetails) -UsarProxy $UsarProxy
            $resp = $req.GetResponse()
            $sr = New-Object System.IO.StreamReader -ArgumentList $resp.GetResponseStream(), ([System.Text.Encoding]::GetEncoding('iso-8859-1'))
            try { $texto = $sr.ReadToEnd() } finally { $sr.Dispose() }
            $itens = @(ConvertFrom-ListagemFtp -Texto $texto)
            foreach ($i in $itens) {
                $i | Add-Member -NotePropertyName Url -NotePropertyValue ($Url.TrimEnd('/') + '/' + [System.Uri]::EscapeDataString($i.Nome))
            }
            return $itens
        }
        catch {
            $ultimo = $_.Exception
            if ($t -lt $Tentativas) { Start-Sleep -Seconds ([int][math]::Min(30, 3 * [math]::Pow(2, $t - 1))) }
        }
        finally {
            if ($null -ne $resp) { try { $resp.Close() } catch { } }
        }
    }
    throw ('Falha ao listar {0}: {1}' -f $Url, $ultimo.Message)
}

function Invoke-FtpDownload {
    <# Download com retomada (REST), novas tentativas e conferência de tamanho. Grava em .part e renomeia no fim. #>
    param(
        [string]$Url, [string]$Destino, [long]$TamanhoEsperado, [int]$Tentativas, [bool]$UsarProxy,
        $EstadoItem, $Controle, $Fila, [string]$Id
    )
    $part = $Destino + '.part'
    $ultimo = $null
    for ($t = 1; $t -le $Tentativas; $t++) {
        if ($null -ne $Controle -and $Controle.Cancelar) { throw 'Cancelado pelo usuário.' }
        $resp = $null; $rs = $null; $fs = $null
        $offset = [long]0
        try {
            if (Test-Path -LiteralPath $part) { $offset = (Get-Item -LiteralPath $part).Length }
            if ($TamanhoEsperado -gt 0 -and $offset -gt $TamanhoEsperado) { Remove-Item -LiteralPath $part -Force; $offset = 0 }
            if ($TamanhoEsperado -gt 0 -and $offset -eq $TamanhoEsperado) {
                Move-Item -LiteralPath $part -Destination $Destino -Force
                return
            }
            $req = New-FtpRequest -Url $Url -Metodo ([System.Net.WebRequestMethods+Ftp]::DownloadFile) -UsarProxy $UsarProxy
            if ($offset -gt 0) {
                $req.ContentOffset = $offset
                Write-LogFila $Fila 'INFO' $Id ('Retomando download a partir de {0}.' -f (Format-BytesLocal $offset))
            }
            $resp = $req.GetResponse()
            $rs = $resp.GetResponseStream()
            $modo = [System.IO.FileMode]::Create
            if ($offset -gt 0) { $modo = [System.IO.FileMode]::Append }
            $fs = New-Object System.IO.FileStream -ArgumentList $part, $modo, ([System.IO.FileAccess]::Write), ([System.IO.FileShare]::None), 1048576
            $buf = New-Object byte[] 1048576
            $total = $offset
            $marca = [DateTime]::UtcNow
            while (($lidos = $rs.Read($buf, 0, $buf.Length)) -gt 0) {
                $fs.Write($buf, 0, $lidos)
                $total += $lidos
                if (([DateTime]::UtcNow - $marca).TotalMilliseconds -ge 500) {
                    $marca = [DateTime]::UtcNow
                    if ($null -ne $EstadoItem) { $EstadoItem.Bytes = $total }
                    if ($null -ne $Controle -and $Controle.Cancelar) { throw 'Cancelado pelo usuário.' }
                }
            }
            $fs.Dispose(); $fs = $null
            $rs.Dispose(); $rs = $null
            $resp.Close(); $resp = $null
            if ($null -ne $EstadoItem) { $EstadoItem.Bytes = $total }
            $tam = (Get-Item -LiteralPath $part).Length
            if ($TamanhoEsperado -gt 0 -and $tam -ne $TamanhoEsperado) {
                if ($tam -gt $TamanhoEsperado) { Remove-Item -LiteralPath $part -Force }
                throw ('tamanho divergente: {0} bytes recebidos, {1} esperados' -f $tam, $TamanhoEsperado)
            }
            Move-Item -LiteralPath $part -Destination $Destino -Force
            return
        }
        catch {
            $ultimo = $_.Exception
            if ($null -ne $fs) { try { $fs.Dispose() } catch { } }
            if ($null -ne $rs) { try { $rs.Dispose() } catch { } }
            if ($null -ne $resp) { try { $resp.Close() } catch { } }
            if ($null -ne $Controle -and $Controle.Cancelar) { throw }
            # Servidor/proxy sem suporte a retomada: recomeça do zero na próxima tentativa.
            if ($offset -gt 0 -and $ultimo.Message -match '(?i)\b(500|501|502|504)\b|REST|not supported') {
                Remove-Item -LiteralPath $part -Force -ErrorAction SilentlyContinue
            }
            if ($t -lt $Tentativas) {
                $espera = [int][math]::Min(120, 5 * [math]::Pow(2, $t - 1))
                Write-LogFila $Fila 'AVISO' $Id ('Tentativa {0}/{1} de download falhou: {2} Nova tentativa em {3}s.' -f $t, $Tentativas, $ultimo.Message, $espera)
                for ($s = 0; $s -lt $espera; $s++) {
                    if ($null -ne $Controle -and $Controle.Cancelar) { throw 'Cancelado pelo usuário.' }
                    Start-Sleep -Seconds 1
                }
            }
        }
    }
    throw ('Falha no download após {0} tentativas: {1}' -f $Tentativas, $ultimo.Message)
}

#endregion

#region ============================ 5. 7-ZIP E AMOSTRAGEM ============================

function Start-Processo7z {
    param([string]$SeteZip, [string]$Argumentos)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $SeteZip
    $psi.Arguments = $Argumentos
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true
    return [System.Diagnostics.Process]::Start($psi)
}

function Invoke-Processo {
    <# Executa um comando curto capturando stdout/stderr sem risco de deadlock. #>
    param([string]$Executavel, [string]$Argumentos, [int]$TimeoutSegundos = 600)
    $p = Start-Processo7z -SeteZip $Executavel -Argumentos $Argumentos
    try {
        $tErr = $p.StandardError.ReadToEndAsync()
        $out = $p.StandardOutput.ReadToEnd()
        if (-not $p.WaitForExit($TimeoutSegundos * 1000)) {
            try { $p.Kill() } catch { }
            throw ('Tempo esgotado executando {0} {1}' -f $Executavel, $Argumentos)
        }
        $p.WaitForExit()
        return [pscustomobject]@{ ExitCode = $p.ExitCode; Saida = $out; Erro = $tErr.Result }
    }
    finally { $p.Dispose() }
}

function Get-EntradasArquivo {
    <# Lista os arquivos de dados dentro do .7z/.zip (ignora layouts/documentos). #>
    param([string]$Arquivo, [string]$SeteZip)
    $ignorar = '(?i)\.(pdf|docx?|xlsx?|html?|xml|json|md|rtf|odt|ods|png|jpe?g)$'
    if ($Arquivo -match '(?i)\.zip$') {
        $zip = [System.IO.Compression.ZipFile]::OpenRead($Arquivo)
        try {
            $lista = @(foreach ($e in $zip.Entries) {
                    if ($e.Length -gt 0 -and -not $e.FullName.EndsWith('/') -and $e.FullName -notmatch $ignorar) {
                        [pscustomobject]@{ Nome = $e.FullName; Tamanho = [long]$e.Length; Tipo = 'zip' }
                    }
                })
        }
        finally { $zip.Dispose() }
        if ($lista.Count -eq 0) { throw ('ARQUIVO_CORROMPIDO: nenhum arquivo de dados dentro de {0}' -f $Arquivo) }
        return @($lista | Sort-Object Nome)
    }
    $r = Invoke-Processo -Executavel $SeteZip -Argumentos ('l -slt "{0}"' -f $Arquivo)
    if ($r.ExitCode -ge 2) { throw ('ARQUIVO_CORROMPIDO: 7-Zip não abriu {0} (código {1}): {2}' -f $Arquivo, $r.ExitCode, ($r.Erro + ' ' + $r.Saida).Trim()) }
    $entradas = New-Object System.Collections.Generic.List[object]
    $atual = $null
    $dentro = $false
    foreach ($linha in ($r.Saida -split "`r?`n")) {
        if (-not $dentro) { if ($linha -match '^-{5,}\s*$') { $dentro = $true }; continue }
        if ($linha -match '^Path = (.*)$') {
            if ($null -ne $atual) { $entradas.Add($atual) }
            $atual = @{ Nome = $Matches[1]; Tamanho = [long]0; Pasta = $false }
        }
        elseif ($null -ne $atual -and $linha -match '^Size = (\d+)') { $atual.Tamanho = [long]$Matches[1] }
        elseif ($null -ne $atual -and $linha -match '^Folder = \+') { $atual.Pasta = $true }
        elseif ($null -ne $atual -and $linha -cmatch '^Attributes = (\S*)') { if ($Matches[1] -cmatch 'D') { $atual.Pasta = $true } }
    }
    if ($null -ne $atual) { $entradas.Add($atual) }
    $sel = @($entradas | Where-Object { -not $_.Pasta -and $_.Tamanho -gt 0 -and $_.Nome -notmatch $ignorar } |
        ForEach-Object { [pscustomobject]@{ Nome = $_.Nome; Tamanho = $_.Tamanho; Tipo = '7z' } } | Sort-Object Nome)
    if ($sel.Count -eq 0) { throw ('ARQUIVO_CORROMPIDO: nenhum arquivo de dados dentro de {0}' -f $Arquivo) }
    return $sel
}

function Test-ArquivoIntegro {
    <# Teste de integridade (CRC) do arquivo compactado com o 7-Zip. #>
    param([string]$Arquivo, [string]$SeteZip)
    if (-not (Test-Path -LiteralPath $Arquivo)) { return $false }
    try {
        $r = Invoke-Processo -Executavel $SeteZip -Argumentos ('t -bd "{0}"' -f $Arquivo) -TimeoutSegundos 7200
        return ($r.ExitCode -lt 2)
    }
    catch { return $false }
}

function Read-AmostraEntrada {
    <# Lê os primeiros bytes de uma entrada (para detectar encoding e cabeçalho) e encerra o 7-Zip. #>
    param([string]$Arquivo, $Entrada, [string]$SeteZip, [int]$Bytes = 262144)
    $buf = New-Object byte[] $Bytes
    $total = 0
    if ($Entrada.Tipo -eq 'zip') {
        $zip = [System.IO.Compression.ZipFile]::OpenRead($Arquivo)
        try {
            $s = $zip.GetEntry($Entrada.Nome).Open()
            try {
                while ($total -lt $Bytes) { $n = $s.Read($buf, $total, $Bytes - $total); if ($n -le 0) { break }; $total += $n }
            }
            finally { $s.Dispose() }
        }
        finally { $zip.Dispose() }
    }
    else {
        $p = Start-Processo7z -SeteZip $SeteZip -Argumentos ('e -so -bd "{0}" "{1}"' -f $Arquivo, $Entrada.Nome)
        try {
            [void]$p.StandardError.ReadToEndAsync()
            $s = $p.StandardOutput.BaseStream
            while ($total -lt $Bytes) { $n = $s.Read($buf, $total, $Bytes - $total); if ($n -le 0) { break }; $total += $n }
        }
        finally {
            if (-not $p.HasExited) { try { $p.Kill() } catch { } }
            try { [void]$p.WaitForExit(30000) } catch { }
            $p.Dispose()
        }
    }
    if ($total -lt $Bytes) {
        $r = New-Object byte[] $total
        [Array]::Copy($buf, $r, $total)
        return , $r
    }
    return , $buf
}

function Get-EncodingAmostra {
    <#
        Decide o encoding pelo CABEÇALHO (nas duas bases ele tem acentos: "Município", "competênciamov"):
        UTF-8 válido -> utf-8; inválido -> Latin-1. Só se o cabeçalho for ASCII puro a amostra inteira é
        avaliada; sem nenhum byte acima de 0x7F, vale o padrão do grupo.
    #>
    param([byte[]]$Bytes, [string]$Padrao)
    if ($Bytes.Length -ge 3 -and $Bytes[0] -eq 0xEF -and $Bytes[1] -eq 0xBB -and $Bytes[2] -eq 0xBF) { return 'utf-8' }
    $fimCab = [Array]::IndexOf($Bytes, [byte]10)
    if ($fimCab -gt 0) {
        $cab = New-Object byte[] $fimCab
        [Array]::Copy($Bytes, $cab, $fimCab)
        if (@($cab -ge 128).Count -gt 0) {
            try {
                $utf8Estrito = New-Object System.Text.UTF8Encoding -ArgumentList $false, $true
                [void]$utf8Estrito.GetString($cab)
                return 'utf-8'
            }
            catch { return 'iso-8859-1' }
        }
    }
    if (@($Bytes -ge 128).Count -eq 0) { return $Padrao }
    $len = $Bytes.Length
    $k = $len - 1
    $cont = 0
    while ($k -ge 0 -and $cont -lt 3 -and (($Bytes[$k] -band 0xC0) -eq 0x80)) { $k--; $cont++ }
    if ($k -ge 0 -and $Bytes[$k] -ge 0xC0) {
        $precisa = 2
        if ($Bytes[$k] -ge 0xF0) { $precisa = 4 } elseif ($Bytes[$k] -ge 0xE0) { $precisa = 3 }
        if (($len - $k) -lt $precisa) { $len = $k }
    }
    try {
        $utf8 = New-Object System.Text.UTF8Encoding -ArgumentList $false, $true
        [void]$utf8.GetString($Bytes, 0, $len)
        return 'utf-8'
    }
    catch { return 'iso-8859-1' }
}

function Get-CabecalhoAmostra {
    param([byte[]]$Bytes, [string]$NomeEncoding, [bool]$AmostraCompleta)
    $texto = [System.Text.Encoding]::GetEncoding($NomeEncoding).GetString($Bytes).TrimStart([char]0xFEFF)
    $fim = $texto.IndexOf("`n")
    if ($fim -lt 0) {
        if ($AmostraCompleta) { $fim = $texto.Length } else { throw 'Cabeçalho não encontrado nos primeiros 256 KB do arquivo.' }
    }
    return $texto.Substring(0, $fim).TrimEnd("`r")
}

function Get-DelimitadorCabecalho {
    param([string]$Linha)
    $melhor = ';'
    $max = -1
    foreach ($cand in @(';', ',', "`t", '|')) {
        $q = $Linha.Split([char]$cand).Count - 1
        if ($q -gt $max) { $max = $q; $melhor = $cand }
    }
    return [char]$melhor
}

#endregion

#region ============================ 6. PLANO DE TRANSFORMAÇÃO ============================

function Initialize-Definicoes {
    param([System.Collections.IDictionary]$MetaGrupos)
    $textos = @{ RAIS_VINC = $Script:DefRaisVinc; RAIS_ESTAB = $Script:DefRaisEstab; CAGED = $Script:DefCaged }
    $defs = @{}
    foreach ($grupo in $MetaGrupos.Keys) {
        $sufixo = @($grupo -split '_')[1]
        $colunas = New-Object System.Collections.Generic.List[object]
        $mapa = @{}
        foreach ($linhaDef in ($textos[$MetaGrupos[$grupo].Dicionario] -split "`r?`n")) {
            $l = $linhaDef.Trim()
            if ($l.Length -eq 0 -or $l.StartsWith('#')) { continue }
            $p = $l.Split('|')
            if ($p.Count -ne 5) { throw ('Linha inválida no dicionário: {0}' -f $l) }
            $nome = $p[0].Trim(); $tipo = $p[1].Trim(); $somente = $p[2].Trim(); $chaves = $p[3].Trim(); $rotulo = $p[4].Trim()
            if ($somente -and (@($somente.Split(',') | ForEach-Object { $_.Trim() }) -notcontains $sufixo)) { continue }
            $derivada = $null
            $listaChaves = @()
            if ($chaves.StartsWith('=')) { $derivada = $chaves.Substring(1) }
            else { $listaChaves = @($chaves.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }) }
            $colunas.Add([pscustomobject]@{ Nome = $nome; Tipo = $tipo; Rotulo = $rotulo; Derivada = $derivada; Chaves = $listaChaves })
            foreach ($ch in $listaChaves) {
                if (-not $mapa.ContainsKey($ch)) { $mapa[$ch] = New-Object System.Collections.Generic.List[string] }
                $mapa[$ch].Add($nome)
            }
        }
        $defs[$grupo] = @{ Colunas = $colunas.ToArray(); Mapa = $mapa }
    }
    return $defs
}

function New-PlanoEtl {
    <#
        Monta o plano de um arquivo a partir do seu cabeçalho: mapeia colunas de origem para o
        dicionário, define a ordem/nomes de saída, tipos, filtro da UF, métrica e derivadas.
        É compartilhado pelos dois motores (C# e PowerShell).
    #>
    param(
        [string]$Grupo, [hashtable]$Defs, [hashtable]$MetaGrupo, [string[]]$Cabecalho,
        [char]$DelimitadorOrigem, [hashtable]$Constantes, [hashtable]$Cfg
    )
    $def = $Defs[$Grupo]
    $n = $Cabecalho.Count
    $nomesOrigem = New-Object string[] $n
    $canonOrigem = New-Object string[] $n
    $tipos = New-Object char[] $n
    $ocorrencias = @{}
    $usados = @{}
    $tipoPorNome = @{}
    foreach ($c in $def.Colunas) { $tipoPorNome[$c.Nome] = $c.Tipo }
    $avisos = New-Object System.Collections.Generic.List[string]

    for ($i = 0; $i -lt $n; $i++) {
        $nome = ([string]$Cabecalho[$i]).Replace([string][char]0xFEFF, '').Trim()
        $nomesOrigem[$i] = $nome
        $chave = ConvertTo-ChaveCabecalho $nome
        $canon = $null
        if ($chave.Length -gt 0 -and $def.Mapa.ContainsKey($chave)) {
            $k = 0
            if ($ocorrencias.ContainsKey($chave)) { $k = [int]$ocorrencias[$chave] }
            $ocorrencias[$chave] = $k + 1
            $lista = $def.Mapa[$chave]
            if ($k -lt $lista.Count) { $canon = $lista[$k] }
        }
        if ($canon -and $usados.ContainsKey($canon)) {
            $avisos.Add(('Coluna "{0}" repetiria "{1}"; tratada como coluna extra.' -f $nome, $canon))
            $canon = $null
        }
        if ($canon) {
            $usados[$canon] = $i
            $canonOrigem[$i] = $canon
            $tipos[$i] = [char]([string]$tipoPorNome[$canon])
        }
        else {
            $tipos[$i] = [char]'T'
        }
    }

    $cabSaida = New-Object System.Collections.Generic.List[string]
    $plano = New-Object System.Collections.Generic.List[int]
    $constValores = New-Object System.Collections.Generic.List[string]
    $destinoExtra = @{}
    if ($Cfg.ModoCabecalho -eq 'Original') {
        for ($i = 0; $i -lt $n; $i++) { $cabSaida.Add($nomesOrigem[$i]); $plano.Add($i) }
    }
    else {
        $nomesUsados = @{}
        foreach ($c in $def.Colunas) {
            $titulo = $c.Nome
            if ($Cfg.ModoCabecalho -eq 'Descritivo') { $titulo = $c.Rotulo }
            if ($c.Derivada -eq 'ano') {
                $constValores.Add([string]$Constantes['ano'])
                $plano.Add(-10 - ($constValores.Count - 1))
            }
            elseif ($c.Derivada -eq 'id_municipio') { $plano.Add(-2) }
            elseif ($usados.ContainsKey($c.Nome)) { $plano.Add([int]$usados[$c.Nome]) }
            else { $plano.Add(-1) }
            $cabSaida.Add($titulo)
            $nomesUsados[$titulo] = $true
        }
        for ($i = 0; $i -lt $n; $i++) {
            if ($canonOrigem[$i]) { continue }
            $base = ConvertTo-Slug $nomesOrigem[$i]
            if ($Cfg.ModoCabecalho -eq 'Descritivo' -and $nomesOrigem[$i]) { $base = $nomesOrigem[$i] }
            $titulo = $base
            $s = 2
            while ($nomesUsados.ContainsKey($titulo)) { $titulo = '{0}_{1}' -f $base, $s; $s++ }
            $nomesUsados[$titulo] = $true
            $destinoExtra[$i] = $titulo
            $cabSaida.Add($titulo)
            $plano.Add($i)
            $avisos.Add(('Coluna não prevista no dicionário "{0}" anexada ao final como "{1}".' -f $nomesOrigem[$i], $titulo))
        }
    }

    $mapeamento = New-Object System.Collections.Generic.List[object]
    for ($i = 0; $i -lt $n; $i++) {
        $dest = $canonOrigem[$i]
        if (-not $dest) { if ($destinoExtra.ContainsKey($i)) { $dest = $destinoExtra[$i] } else { $dest = $nomesOrigem[$i] } }
        $mapeamento.Add([pscustomobject]@{ Posicao = $i + 1; Origem = $nomesOrigem[$i]; Saida = $dest; Tipo = [string]$tipos[$i] })
    }

    $indiceFiltro = -1
    $porPrefixo = $false
    $colunaFiltro = ''
    foreach ($cand in $MetaGrupo.Filtro) {
        if ($usados.ContainsKey($cand)) {
            $indiceFiltro = [int]$usados[$cand]
            $porPrefixo = ($cand -eq 'id_municipio_6')
            $colunaFiltro = $cand
            break
        }
    }
    if ($indiceFiltro -lt 0) {
        throw ('Coluna para filtrar a UF não encontrada no cabeçalho (procurei {0}). Cabeçalho: {1}' -f ($MetaGrupo.Filtro -join ', '), ($nomesOrigem -join ' | '))
    }

    $fixas = @($def.Colunas | Where-Object { -not $_.Derivada })
    $reconhecidas = @($fixas | Where-Object { $usados.ContainsKey($_.Nome) }).Count
    if ($reconhecidas -lt [math]::Ceiling($fixas.Count * 0.5)) {
        throw ('Layout não reconhecido: só {0} de {1} colunas conhecidas foram encontradas (delimitador "{2}"). Cabeçalho: {3}' -f $reconhecidas, $fixas.Count, $DelimitadorOrigem, ($nomesOrigem -join ' | '))
    }
    $indiceMetrica = -1
    if ($usados.ContainsKey($MetaGrupo.Metrica)) { $indiceMetrica = [int]$usados[$MetaGrupo.Metrica] }
    $indiceMun6 = -1
    if ($usados.ContainsKey('id_municipio_6')) { $indiceMun6 = [int]$usados['id_municipio_6'] }

    return [pscustomobject]@{
        NumCamposOrigem      = $n
        DelimitadorOrigem    = $DelimitadorOrigem
        DecimalVirgulaOrigem = ([string]$DelimitadorOrigem -ne ',')
        IndiceFiltro         = $indiceFiltro
        FiltroPorPrefixo     = $porPrefixo
        ValorFiltro          = [string]$Cfg.CodigoUF
        ColunaFiltro         = $colunaFiltro
        TiposOrigem          = $tipos
        Plano                = $plano.ToArray()
        Constantes           = $constValores.ToArray()
        IndiceMunicipio6     = $indiceMun6
        IndiceMetrica        = $indiceMetrica
        DecimalSaida         = [char]$Cfg.DecimalSaida
        ManterBrutos         = [bool]$Cfg.ManterBrutos
        CabecalhoSaida       = $cabSaida.ToArray()
        Mapeamento           = $mapeamento.ToArray()
        Avisos               = $avisos.ToArray()
        ColunasReconhecidas  = $reconhecidas
        ColunasConhecidas    = $fixas.Count
    }
}

function ConvertTo-PlanoCSharp {
    param($Plano)
    $pc = New-Object EtlMteV1.PlanoEtl
    $pc.NumCamposOrigem = [int]$Plano.NumCamposOrigem
    $pc.DelimitadorOrigem = [char]$Plano.DelimitadorOrigem
    $pc.DecimalVirgulaOrigem = [bool]$Plano.DecimalVirgulaOrigem
    $pc.IndiceFiltro = [int]$Plano.IndiceFiltro
    $pc.FiltroPorPrefixo = [bool]$Plano.FiltroPorPrefixo
    $pc.ValorFiltro = [string]$Plano.ValorFiltro
    $pc.TiposOrigem = [char[]]$Plano.TiposOrigem
    $pc.Plano = [int[]]$Plano.Plano
    $pc.Constantes = [string[]]@($Plano.Constantes)
    $pc.IndiceMunicipio6 = [int]$Plano.IndiceMunicipio6
    $pc.IndiceMetrica = [int]$Plano.IndiceMetrica
    $pc.DecimalSaida = [char]$Plano.DecimalSaida
    $pc.ManterBrutos = [bool]$Plano.ManterBrutos
    return $pc
}

#endregion

#region ============================ 7. MOTOR POWERSHELL (CONTINGÊNCIA) ============================

function New-EscritorCsv {
    param([string]$Caminho, [string[]]$Cabecalho, [hashtable]$Cfg)
    $fs = New-Object System.IO.FileStream -ArgumentList $Caminho, ([System.IO.FileMode]::Create), ([System.IO.FileAccess]::Write), ([System.IO.FileShare]::Read), 1048576
    $destino = $fs
    if ($Cfg.Gzip) { $destino = New-Object System.IO.Compression.GZipStream -ArgumentList $fs, ([System.IO.Compression.CompressionMode]::Compress) }
    $w = New-Object System.IO.StreamWriter -ArgumentList $destino, (New-Object System.Text.UTF8Encoding -ArgumentList ([bool]$Cfg.Bom)), 1048576
    $w.NewLine = "`r`n"
    $w.WriteLine((ConvertTo-LinhaCsvSaida -Valores $Cabecalho -Delimitador ([char]$Cfg.DelimitadorSaida)))
    return $w
}

function Invoke-MotorPowerShell {
    <#
        Mesmo algoritmo do motor C#, vetorizado com operadores de array (-replace, fatiamento por
        índices) para reduzir o custo por campo. Usado quando Add-Type não está disponível.
    #>
    param(
        $Plano, [System.IO.Stream]$Entrada, [string]$NomeEncoding, [System.IO.StreamWriter]$Escritor,
        [hashtable]$Resultado, [char]$DelimitadorSaida, $EstadoItem, $Controle
    )
    # Padrões idênticos à classe Normalizacao (C#).
    $reNA = '(?i)^(?:\{[^\r\n]*|\*+|n/d)\z'
    $reMilhar = '(?<=^[+-]?\d{1,3}(?:\.\d{3})*)\.(?=\d{3}(?:\.\d{3})*\z)'
    $reAspas = '(?s)^(.*[' + [regex]::Escape([string]$DelimitadorSaida) + '"\r\n].*)\z'
    $ds = [string]$DelimitadorSaida
    $estilo = [System.Globalization.NumberStyles]'AllowLeadingSign, AllowDecimalPoint'
    $inv = [System.Globalization.CultureInfo]::InvariantCulture

    $enc = [System.Text.Encoding]::GetEncoding($NomeEncoding)
    $sr = New-Object System.IO.StreamReader -ArgumentList $Entrada, $enc, $false, 1048576
    try {
        [void]$sr.ReadLine()
        $d = [char]$Plano.DelimitadorOrigem
        $sepAspas = [string[]]@('"' + $d + '"')
        $n = [int]$Plano.NumCamposOrigem
        # Atenção: variáveis do PowerShell não diferenciam maiúsculas ($valorUF <> $valorCampoFiltro).
        $iF = [int]$Plano.IndiceFiltro
        $valorUF = [string]$Plano.ValorFiltro
        $prefixo = [bool]$Plano.FiltroPorPrefixo
        $brutos = [bool]$Plano.ManterBrutos
        $decVirgOrig = [bool]$Plano.DecimalVirgulaOrigem
        $decVirgSaida = ([string]$Plano.DecimalSaida -eq ',') -and (-not $brutos)
        $tipos = [char[]]$Plano.TiposOrigem
        $consts = [string[]]@($Plano.Constantes)
        $iM6 = [int]$Plano.IndiceMunicipio6
        $iMet = [int]$Plano.IndiceMetrica

        $listaD = New-Object System.Collections.Generic.List[int]
        $listaI = New-Object System.Collections.Generic.List[int]
        for ($i = 0; $i -lt $n; $i++) {
            if ($tipos[$i] -ceq 'D') { $listaD.Add($i) } elseif ($tipos[$i] -ceq 'I') { $listaI.Add($i) }
        }
        $idxD = $listaD.ToArray(); $idxI = $listaI.ToArray()
        $nD = $idxD.Length; $nI = $idxI.Length
        $posD = @{}; for ($k = 0; $k -lt $nD; $k++) { $posD[$idxD[$k]] = $k }
        $posI = @{}; for ($k = 0; $k -lt $nI; $k++) { $posI[$idxI[$k]] = $k }

        # Array concatenado por registro: origem + decimais + inteiros + constantes + [id_municipio, vazio]
        if ($brutos) { $baseExtra = $n } else { $baseExtra = $n + $nD + $nI }
        $idxIdMun = $baseExtra + $consts.Length
        $idxVazio = $idxIdMun + 1
        $planoOrig = [int[]]$Plano.Plano
        $planoTodos = New-Object int[] ($planoOrig.Length)
        for ($j = 0; $j -lt $planoOrig.Length; $j++) {
            $k = $planoOrig[$j]
            if ($k -ge 0) {
                if ($k -ge $n) { $planoTodos[$j] = $idxVazio }
                elseif ((-not $brutos) -and $posD.ContainsKey($k)) { $planoTodos[$j] = $n + $posD[$k] }
                elseif ((-not $brutos) -and $posI.ContainsKey($k)) { $planoTodos[$j] = $n + $nD + $posI[$k] }
                else { $planoTodos[$j] = $k }
            }
            elseif ($k -eq -1) { $planoTodos[$j] = $idxVazio }
            elseif ($k -eq -2) { $planoTodos[$j] = $idxIdMun }
            else {
                $c = -10 - $k
                if ($c -ge 0 -and $c -lt $consts.Length) { $planoTodos[$j] = $baseExtra + $c } else { $planoTodos[$j] = $idxVazio }
            }
        }
        $metFonte = 'T'; $metPos = -1; $metEhI = $false
        if ($iMet -ge 0) {
            $metEhI = ($tipos[$iMet] -ceq 'I')
            if ($posI.ContainsKey($iMet)) { $metFonte = 'I'; $metPos = $posI[$iMet] }
            elseif ($posD.ContainsKey($iMet)) { $metFonte = 'D'; $metPos = $posD[$iMet] }
        }

        $vD = @(); $vI = @()
        $cacheMun = @{}
        $numLinha = [long]1
        $lidas = [long]0; $saidas = [long]0; $irreg = [long]0
        $soma = [decimal]0; $pos = [long]0; $neg = [long]0
        $dv = [decimal]0
        $baseLidas = [long]$Resultado.LinhasLidas
        $baseIrreg = [long]$Resultado.LinhasIrregulares

        while ($null -ne ($linha = $sr.ReadLine())) {
            $numLinha++
            if ($linha.Length -eq 0) { continue }
            $lidas++
            if (($lidas % 20000) -eq 0) {
                if ($null -ne $EstadoItem) { $EstadoItem.Linhas = $baseLidas + $lidas }
                if ($null -ne $Controle -and $Controle.Cancelar) { throw 'Processamento cancelado.' }
            }
            $linhaInicio = $numLinha

            # ---------- leitura do registro ----------
            if ($linha.IndexOf('"') -lt 0) {
                $f = $linha.Split($d)
            }
            else {
                $f = $null
                if ($linha.Length -ge 2 -and ($linha[0] -ceq '"') -and ($linha[$linha.Length - 1] -ceq '"')) {
                    $f = $linha.Substring(1, $linha.Length - 2).Split($sepAspas, [System.StringSplitOptions]::None)
                    if (@($f -like '*"*').Count -gt 0) { $f = $null }
                }
                if ($null -eq $f) {
                    $reg = $linha
                    while ($true) {
                        $res = ConvertFrom-LinhaCsv -Texto $reg -Delimitador $d
                        if ($res.Completa) { break }
                        $prox = $sr.ReadLine()
                        if ($null -eq $prox) { break }
                        $numLinha++
                        $reg = $reg + "`n" + $prox
                    }
                    $f = $res.Campos
                }
            }

            # ---------- filtro da UF ----------
            if ($iF -ge $f.Length) { continue }
            $valorCampoFiltro = $f[$iF].Trim()
            if ($prefixo) { if (-not $valorCampoFiltro.StartsWith($valorUF, [System.StringComparison]::Ordinal)) { continue } }
            elseif (-not [string]::Equals($valorCampoFiltro, $valorUF, [System.StringComparison]::Ordinal)) { continue }

            if ($f.Length -ne $n) {
                $irreg++
                if (($baseIrreg + $irreg) -le 5) {
                    $Resultado.Avisos.Add(('Linha {0}: {1} campos (esperado {2}); registro ajustado.' -f $linhaInicio, $f.Length, $n))
                }
                if ($f.Length -lt $n) { $f = [string[]](@($f) + @(, '' * ($n - $f.Length))) }
                else { $f = [string[]]@($f[0..($n - 1)]) }
            }

            # ---------- normalização ----------
            $mv = ''
            if ($brutos) {
                if ($iMet -ge 0) {
                    $mv = $f[$iMet].Trim() -replace $reNA, '' -replace '\s', ''
                    if ($decVirgOrig) { $mv = $mv -replace '\.(?=[\d.]*,)', '' -replace $reMilhar, '' -replace ',', '.' }
                    else { $mv = $mv -replace '^([+-]?\d*),(\d*)\z', '$1.$2' }
                    $mv = $mv -replace '^([+-]?)0+(?=\d)', '$1' -replace '^([+-]?)\.(?=\d)', '${1}0.' -replace '(?<=\d)\.\z', '' -replace '^\+(?=[\d.])', ''
                    if ($metEhI) { $mv = $mv -replace '^([+-]?\d+)\.0+\z', '$1' }
                }
            }
            else {
                $f = @($f -replace '^\s+|\s+$', '' -replace $reNA, '')
                if ($nD -gt 0) {
                    $vD = @(@($f[$idxD]) -replace '\s', '')
                    if ($decVirgOrig) { $vD = @($vD -replace '\.(?=[\d.]*,)', '' -replace $reMilhar, '' -replace ',', '.') }
                    else { $vD = @($vD -replace '^([+-]?\d*),(\d*)\z', '$1.$2') }
                    $vD = @($vD -replace '^([+-]?)0+(?=\d)', '$1' -replace '^([+-]?)\.(?=\d)', '${1}0.' -replace '(?<=\d)\.\z', '' -replace '^\+(?=[\d.])', '')
                }
                if ($nI -gt 0) {
                    $vI = @(@($f[$idxI]) -replace '\s', '')
                    if ($decVirgOrig) { $vI = @($vI -replace '\.(?=[\d.]*,)', '' -replace $reMilhar, '' -replace ',', '.') }
                    else { $vI = @($vI -replace '^([+-]?\d*),(\d*)\z', '$1.$2') }
                    $vI = @($vI -replace '^([+-]?)0+(?=\d)', '$1' -replace '^([+-]?)\.(?=\d)', '${1}0.' -replace '(?<=\d)\.\z', '' -replace '^\+(?=[\d.])', '' -replace '^([+-]?\d+)\.0+\z', '$1')
                }
                if ($iMet -ge 0) {
                    if ($metFonte -eq 'I') { $mv = [string]$vI[$metPos] } elseif ($metFonte -eq 'D') { $mv = [string]$vD[$metPos] } else { $mv = [string]$f[$iMet] }
                }
                if ($decVirgSaida) {
                    if ($nD -gt 0) { $vD = @($vD -replace '^([+-]?\d+)\.(\d+)\z', '$1,$2') }
                    if ($nI -gt 0) { $vI = @($vI -replace '^([+-]?\d+)\.(\d+)\z', '$1,$2') }
                }
            }
            if ($iMet -ge 0 -and $mv.Length -gt 0 -and [decimal]::TryParse($mv, $estilo, $inv, [ref]$dv)) {
                $soma += $dv
                if ($dv -gt 0) { $pos++ } elseif ($dv -lt 0) { $neg++ }
            }

            # ---------- derivada: município IBGE 7 dígitos ----------
            $idMun = ''
            if ($iM6 -ge 0) {
                if ($brutos) { $m6 = $f[$iM6].Trim() } else { $m6 = [string]$f[$iM6] }
                $idMun = $cacheMun[$m6]
                if ($null -eq $idMun) { $idMun = Get-MunicipioIbge7 -Codigo6 $m6; $cacheMun[$m6] = $idMun }
            }

            # ---------- montagem e gravação ----------
            if ($brutos) { $todos = @($f) + $consts + @($idMun, '') }
            else { $todos = @($f) + $vD + $vI + $consts + @($idMun, '') }
            $saida = @(@($todos[$planoTodos]) -replace '"', '""' -replace $reAspas, '"$1"')
            $Escritor.WriteLine([string]::Join($ds, $saida))
            $saidas++
        }
        $Resultado.LinhasLidas = $baseLidas + $lidas
        $Resultado.LinhasSaida = [long]$Resultado.LinhasSaida + $saidas
        $Resultado.LinhasIrregulares = $baseIrreg + $irreg
        $Resultado.MetricaSoma = [decimal]$Resultado.MetricaSoma + $soma
        $Resultado.MetricaPositivos = [long]$Resultado.MetricaPositivos + $pos
        $Resultado.MetricaNegativos = [long]$Resultado.MetricaNegativos + $neg
        if ($null -ne $EstadoItem) { $EstadoItem.Linhas = $Resultado.LinhasLidas }
    }
    finally {
        $sr.Dispose()
    }
}

#endregion

#region ============================ 8. PROCESSAMENTO DE UM ARQUIVO (WORKER) ============================

function Invoke-EtlArquivo {
    <# Abre cada entrada do arquivo compactado, monta o plano pelo cabeçalho e executa o motor escolhido. #>
    param([string]$Arquivo, $Item, [hashtable]$Cfg, [hashtable]$Defs, $Est, $Controle, [string]$Tmp, $Fila)
    $meta = $Cfg.Grupos[$Item.Grupo]
    $entradas = @(Get-EntradasArquivo -Arquivo $Arquivo -SeteZip $Cfg.SeteZip)
    $resultado = @{
        LinhasLidas = [long]0; LinhasSaida = [long]0; LinhasIrregulares = [long]0
        MetricaSoma = [decimal]0; MetricaPositivos = [long]0; MetricaNegativos = [long]0
        Avisos = (New-Object System.Collections.Generic.List[string])
    }
    $constantes = @{ ano = [string]$Item.Ano }
    $cabecalhoRef = $null
    $proc = $null
    $w = $null
    $encs = New-Object System.Collections.Generic.List[string]
    $delims = New-Object System.Collections.Generic.List[string]
    $mapeamento = $null
    $colunaFiltro = ''
    try {
        foreach ($ent in $entradas) {
            $amostra = Read-AmostraEntrada -Arquivo $Arquivo -Entrada $ent -SeteZip $Cfg.SeteZip
            if ($amostra.Length -eq 0) { $resultado.Avisos.Add(('Entrada vazia: {0}' -f $ent.Nome)); continue }
            $enc = Get-EncodingAmostra -Bytes $amostra -Padrao $meta.EncodingPadrao
            $linhaCab = Get-CabecalhoAmostra -Bytes $amostra -NomeEncoding $enc -AmostraCompleta ($amostra.Length -lt 262144)
            $delim = Get-DelimitadorCabecalho -Linha $linhaCab
            $campos = (ConvertFrom-LinhaCsv -Texto $linhaCab -Delimitador $delim).Campos
            $plano = New-PlanoEtl -Grupo $Item.Grupo -Defs $Defs -MetaGrupo $meta -Cabecalho $campos -DelimitadorOrigem $delim -Constantes $constantes -Cfg $Cfg
            foreach ($a in $plano.Avisos) { $resultado.Avisos.Add($a) }
            if ($null -eq $cabecalhoRef) {
                $cabecalhoRef = $plano.CabecalhoSaida
                $mapeamento = $plano.Mapeamento
                $colunaFiltro = $plano.ColunaFiltro
                if ($Cfg.Motor -eq 'CSharp') {
                    $proc = New-Object EtlMteV1.Processador -ArgumentList $Tmp, ([string[]]$cabecalhoRef), ([char]$Cfg.DelimitadorSaida), ([bool]$Cfg.Gzip), ([bool]$Cfg.Bom)
                    $Est.Processador = $proc
                }
                else {
                    $w = New-EscritorCsv -Caminho $Tmp -Cabecalho $cabecalhoRef -Cfg $Cfg
                }
            }
            elseif ((@($plano.CabecalhoSaida) -join [string][char]1) -ne (@($cabecalhoRef) -join [string][char]1)) {
                throw ('A entrada {0} tem layout de saída diferente da primeira entrada do arquivo.' -f $ent.Nome)
            }
            $nomeDelim = [string]$delim
            if ($nomeDelim -eq "`t") { $nomeDelim = 'TAB' }
            $encs.Add($enc); $delims.Add($nomeDelim)

            if ($ent.Tipo -eq 'zip') {
                $zip = [System.IO.Compression.ZipFile]::OpenRead($Arquivo)
                try {
                    $s = $zip.GetEntry($ent.Nome).Open()
                    try {
                        if ($null -ne $proc) { $proc.ProcessarStream($s, $enc, (ConvertTo-PlanoCSharp $plano)) }
                        else { Invoke-MotorPowerShell -Plano $plano -Entrada $s -NomeEncoding $enc -Escritor $w -Resultado $resultado -DelimitadorSaida ([char]$Cfg.DelimitadorSaida) -EstadoItem $Est -Controle $Controle }
                    }
                    finally { $s.Dispose() }
                }
                finally { $zip.Dispose() }
            }
            else {
                $p = Start-Processo7z -SeteZip $Cfg.SeteZip -Argumentos ('e -so -bd "{0}" "{1}"' -f $Arquivo, $ent.Nome)
                $Est.Processo = $p
                $tErr = $p.StandardError.ReadToEndAsync()
                $ok = $false
                try {
                    if ($null -ne $proc) { $proc.ProcessarStream($p.StandardOutput.BaseStream, $enc, (ConvertTo-PlanoCSharp $plano)) }
                    else { Invoke-MotorPowerShell -Plano $plano -Entrada $p.StandardOutput.BaseStream -NomeEncoding $enc -Escritor $w -Resultado $resultado -DelimitadorSaida ([char]$Cfg.DelimitadorSaida) -EstadoItem $Est -Controle $Controle }
                    $ok = $true
                }
                finally {
                    if ((-not $ok) -and (-not $p.HasExited)) { try { $p.Kill() } catch { } }
                    $p.WaitForExit()
                    $Est.Processo = $null
                }
                $codigo = $p.ExitCode
                $erro7z = [string]$tErr.Result
                $p.Dispose()
                if ($codigo -ge 2) { throw ('ARQUIVO_CORROMPIDO: 7-Zip retornou código {0} ao extrair {1}: {2}' -f $codigo, $ent.Nome, $erro7z.Trim()) }
                if ($codigo -eq 1) { $resultado.Avisos.Add(('7-Zip emitiu aviso ao extrair {0}: {1}' -f $ent.Nome, $erro7z.Trim())) }
            }
        }
        if ($null -eq $cabecalhoRef) { throw 'O arquivo compactado não contém dados.' }
        if ($null -ne $proc) {
            $r = $proc.Finalizar()
            $resultado.LinhasLidas = $r.LinhasLidas
            $resultado.LinhasSaida = $r.LinhasSaida
            $resultado.LinhasIrregulares = $r.LinhasIrregulares
            $resultado.MetricaSoma = $r.MetricaSoma
            $resultado.MetricaPositivos = $r.MetricaPositivos
            $resultado.MetricaNegativos = $r.MetricaNegativos
            foreach ($a in $r.Avisos) { $resultado.Avisos.Add($a) }
        }
        else {
            $w.Flush(); $w.Dispose(); $w = $null
        }
    }
    finally {
        if ($null -ne $proc) { $proc.Dispose() }
        if ($null -ne $w) { $w.Dispose() }
        $Est.Processador = $null
    }
    $resultado.Encoding = (@($encs | Select-Object -Unique) -join ',')
    $resultado.Delimitador = (@($delims | Select-Object -Unique) -join ',')
    $resultado.Mapeamento = $mapeamento
    $resultado.ColunaFiltro = $colunaFiltro
    return $resultado
}

function Invoke-ProcessarItem {
    <# Unidade de trabalho de um runspace: download + ETL + limpeza de um arquivo do FTP. #>
    param($Item, [hashtable]$Cfg, [hashtable]$Defs, $Fila, $Estado, $Controle)
    $inicio = Get-Date
    $id = $Item.Id
    $est = @{ Fase = 'Aguardando'; Bytes = [long]0; Total = [long]$Item.Tamanho; Linhas = [long]0; Processador = $null; Processo = $null }
    $Estado[$id] = $est
    $meta = $Cfg.Grupos[$Item.Grupo]
    $tmp = $Item.CaminhoSaida + '.tmp'
    $status = 'ERRO'
    $mensagem = ''
    $res = $null
    try {
        [void](New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Item.CaminhoSaida))
        [void](New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Item.CaminhoLocal))
        $tentativa = 0
        while ($true) {
            $tentativa++
            $baixar = $true
            if (Test-Path -LiteralPath $Item.CaminhoLocal) {
                $tamLocal = (Get-Item -LiteralPath $Item.CaminhoLocal).Length
                if ($Item.Tamanho -le 0 -or $tamLocal -eq $Item.Tamanho) {
                    $baixar = $false
                    Write-LogFila $Fila 'INFO' $id ('Reaproveitando arquivo já baixado ({0}).' -f (Format-BytesLocal $tamLocal))
                }
                else { Remove-Item -LiteralPath $Item.CaminhoLocal -Force }
            }
            if ($baixar) {
                $est.Fase = 'Download'
                $t0 = Get-Date
                Write-LogFila $Fila 'INFO' $id ('Baixando {0} ({1}).' -f $Item.NomeArquivo, (Format-BytesLocal $Item.Tamanho))
                Invoke-FtpDownload -Url $Item.Url -Destino $Item.CaminhoLocal -TamanhoEsperado ([long]$Item.Tamanho) -Tentativas $Cfg.Tentativas -UsarProxy ([bool]$Cfg.UsarProxy) -EstadoItem $est -Controle $Controle -Fila $Fila -Id $id
                $seg = [math]::Max(0.1, ((Get-Date) - $t0).TotalSeconds)
                Write-LogFila $Fila 'INFO' $id ('Download concluído em {0:N0}s ({1}/s).' -f $seg, (Format-BytesLocal ((Get-Item -LiteralPath $Item.CaminhoLocal).Length / $seg)))
            }
            $est.Fase = 'Processando'
            try {
                $res = Invoke-EtlArquivo -Arquivo $Item.CaminhoLocal -Item $Item -Cfg $Cfg -Defs $Defs -Est $est -Controle $Controle -Tmp $tmp -Fila $Fila
                break
            }
            catch {
                $msg = $_.Exception.Message
                Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
                if ($null -ne $Controle -and $Controle.Cancelar) { throw }
                $corrompido = $msg.StartsWith('ARQUIVO_CORROMPIDO')
                if (-not $corrompido) {
                    # Um erro de layout/leitura pode vir de um .7z danificado: confirma com o teste do 7-Zip (7z t).
                    $est.Fase = 'Testando'
                    if (-not (Test-ArquivoIntegro -Arquivo $Item.CaminhoLocal -SeteZip $Cfg.SeteZip)) {
                        $corrompido = $true
                        $msg = 'ARQUIVO_CORROMPIDO: o teste de integridade (7z t) falhou. Erro original: ' + $msg
                    }
                }
                if ($corrompido) {
                    Remove-Item -LiteralPath $Item.CaminhoLocal -Force -ErrorAction SilentlyContinue
                    if ($tentativa -eq 1) {
                        Write-LogFila $Fila 'AVISO' $id ('Arquivo compactado danificado; baixando novamente. ({0})' -f $msg)
                        continue
                    }
                }
                throw $msg
            }
        }
        if ($res.LinhasSaida -eq 0) {
            Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
            if ($Item.Regiao -eq 'NI') {
                $status = 'SEM_DADOS'
                $mensagem = 'Nenhum registro da UF neste arquivo (esperado para a região NI).'
            }
            else {
                $status = 'ERRO'
                $mensagem = ('Nenhum registro da UF encontrado em {0:N0} linhas lidas: confira o layout/filtro.' -f $res.LinhasLidas)
            }
        }
        else {
            Move-Item -LiteralPath $tmp -Destination $Item.CaminhoSaida -Force
            $status = 'OK'
            $mensagem = ('{0:N0} de {1:N0} linhas da UF ({2:P1}).' -f $res.LinhasSaida, $res.LinhasLidas, ($res.LinhasSaida / [math]::Max(1, $res.LinhasLidas)))
        }
        if (-not $Cfg.ManterCompactados) { Remove-Item -LiteralPath $Item.CaminhoLocal -Force -ErrorAction SilentlyContinue }
    }
    catch {
        $status = 'ERRO'
        $mensagem = $_.Exception.Message
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
    finally {
        $est.Fase = 'Concluído'
    }
    $fim = Get-Date
    $inv = [System.Globalization.CultureInfo]::InvariantCulture
    $avisos = @()
    $mapeamento = @()
    $linhasLidas = ''; $linhasSaida = ''; $irregulares = ''; $encOrigem = ''; $delimOrigem = ''; $metSoma = ''; $metPos = ''; $metNeg = ''
    if ($null -ne $res) {
        $avisos = @($res.Avisos)
        if ($null -ne $res.Mapeamento) { $mapeamento = @($res.Mapeamento) }
        $linhasLidas = [string]$res.LinhasLidas; $linhasSaida = [string]$res.LinhasSaida; $irregulares = [string]$res.LinhasIrregulares
        $encOrigem = $res.Encoding; $delimOrigem = $res.Delimitador
        $metSoma = ([decimal]$res.MetricaSoma).ToString($inv); $metPos = [string]$res.MetricaPositivos; $metNeg = [string]$res.MetricaNegativos
    }
    $arquivoSaida = ''
    if ($status -eq 'OK') { $arquivoSaida = $Item.CaminhoSaida }
    return [pscustomobject]@{
        IdItem             = $id
        id                 = $id
        grupo              = $Item.Grupo
        periodo            = [string]$Item.Periodo
        regiao             = [string]$Item.Regiao
        arquivo_origem     = $Item.NomeArquivo
        url                = $Item.Url
        tamanho_remoto     = [string]$Item.Tamanho
        data_remota        = [string]$Item.DataRemota
        status             = $status
        arquivo_saida      = $arquivoSaida
        linhas_lidas       = $linhasLidas
        linhas_saida       = $linhasSaida
        linhas_irregulares = $irregulares
        encoding_origem    = $encOrigem
        delimitador_origem = $delimOrigem
        metrica_coluna     = $meta.Metrica
        metrica_soma       = $metSoma
        metrica_positivos  = $metPos
        metrica_negativos  = $metNeg
        motor              = $Cfg.Motor
        assinatura         = $Cfg.Assinatura
        inicio             = $inicio.ToString('yyyy-MM-dd HH:mm:ss', $inv)
        fim                = $fim.ToString('yyyy-MM-dd HH:mm:ss', $inv)
        duracao_s          = ([math]::Round(($fim - $inicio).TotalSeconds, 1)).ToString($inv)
        mensagem           = $mensagem
        Avisos             = $avisos
        Mapeamento         = $mapeamento
    }
}

function Format-BytesLocal {
    <# Cópia de Format-Bytes disponível nos runspaces. #>
    param([double]$Bytes)
    if ($Bytes -ge 1GB) { return ('{0:N2} GB' -f ($Bytes / 1GB)) }
    if ($Bytes -ge 1MB) { return ('{0:N1} MB' -f ($Bytes / 1MB)) }
    if ($Bytes -lt 0) { return 'tamanho desconhecido' }
    return ('{0:N0} KB' -f ($Bytes / 1KB))
}
$Script:FuncoesWorker += 'Format-BytesLocal'

# Script executado em cada runspace do pool.
$Script:ScriptWorker = {
    param($Item, $Cfg, $Defs, $Fila, $Estado, $Controle)
    Set-StrictMode -Version 2.0
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    try {
        Invoke-ProcessarItem -Item $Item -Cfg $Cfg -Defs $Defs -Fila $Fila -Estado $Estado -Controle $Controle
    }
    catch {
        [pscustomobject]@{ IdItem = $Item.Id; id = $Item.Id; status = 'ERRO'; mensagem = ('Falha inesperada no worker: ' + $_.Exception.Message); Avisos = @(); Mapeamento = @() }
    }
}

#endregion

#region ============================ 9. DESCOBERTA E CONTROLE ============================

function Resolve-Grupos {
    param([string[]]$Bases)
    $validos = @('RAIS', 'CAGED') + @($Script:MetaGrupos.Keys)
    $sel = New-Object System.Collections.Generic.List[string]
    foreach ($b in @($Bases | ForEach-Object { ([string]$_).Split(',') } | ForEach-Object { $_.Trim().ToUpperInvariant() } | Where-Object { $_ })) {
        if ($validos -notcontains $b) { throw ('Valor inválido em -Bases: "{0}". Use: {1}.' -f $b, ($validos -join ', ')) }
        if ($b -eq 'RAIS') { $sel.Add('RAIS_VINC'); $sel.Add('RAIS_ESTAB') }
        elseif ($b -eq 'CAGED') { $sel.Add('CAGED_MOV'); $sel.Add('CAGED_FOR'); $sel.Add('CAGED_EXC') }
        else { $sel.Add($b) }
    }
    $grupos = @($Script:MetaGrupos.Keys | Where-Object { $sel -contains $_ })
    if ($grupos.Count -eq 0) { throw 'Nenhuma base selecionada em -Bases.' }
    return $grupos
}

function Get-ItensRemotos {
    <# Percorre o FTP e devolve um item por arquivo compactado do período/grupos selecionados. #>
    param([hashtable]$Cfg, [string[]]$Grupos)
    $itens = New-Object System.Collections.Generic.List[object]
    $querRais = @($Grupos | Where-Object { $_ -like 'RAIS_*' }).Count -gt 0
    $querCaged = @($Grupos | Where-Object { $_ -like 'CAGED_*' }).Count -gt 0
    $regiaoAlvo = ($Cfg.RegiaoUF -replace '[^A-Z]', '')

    if ($querRais) {
        $urlRais = Join-FtpUrl -Base $Cfg.FtpBase -Segmentos @('RAIS')
        Write-Log ('Listando {0}' -f $urlRais)
        $anos = @(Get-FtpListagem -Url $urlRais -UsarProxy $Cfg.UsarProxy -Tentativas $Cfg.Tentativas | Where-Object {
                $_.EhDiretorio -and $_.Nome -match '^\d{4}$' -and [int]$_.Nome -ge $Cfg.AnoInicial -and [int]$_.Nome -le $Cfg.AnoFinal
            } | Sort-Object Nome)
        foreach ($a in $anos) {
            $arquivos = @(Get-FtpListagem -Url $a.Url -UsarProxy $Cfg.UsarProxy -Tentativas $Cfg.Tentativas | Where-Object { -not $_.EhDiretorio })
            foreach ($f in $arquivos) {
                $grupo = $null; $regiao = ''
                if (($Grupos -contains 'RAIS_VINC') -and $f.Nome -match '^RAIS_VINC_PUB_(?<reg>.+?)\.(7z|zip)$') {
                    $regiao = $Matches['reg'].ToUpperInvariant()
                    $regNorm = $regiao -replace '[^A-Z]', ''
                    if ($regNorm -eq $regiaoAlvo -or $regNorm -eq 'NI') { $grupo = 'RAIS_VINC' }
                }
                elseif (($Grupos -contains 'RAIS_ESTAB') -and $f.Nome -match '^RAIS_ESTAB_PUB.*\.(7z|zip)$') {
                    $grupo = 'RAIS_ESTAB'
                }
                if ($null -eq $grupo) { continue }
                $id = '{0}_{1}' -f $grupo, $a.Nome
                if ($grupo -eq 'RAIS_VINC') { $id = '{0}_{1}' -f $id, $regiao }
                $itens.Add([pscustomobject]@{
                        Id = $id; Grupo = $grupo; Periodo = $a.Nome; Ano = $a.Nome; Regiao = $regiao
                        NomeArquivo = $f.Nome; Url = $f.Url; Tamanho = [long]$f.Tamanho; DataRemota = $f.Data
                    })
            }
        }
    }

    if ($querCaged) {
        $urlCaged = Join-FtpUrl -Base $Cfg.FtpBase -Segmentos @('NOVO CAGED')
        Write-Log ('Listando {0}' -f $urlCaged)
        $anos = @(Get-FtpListagem -Url $urlCaged -UsarProxy $Cfg.UsarProxy -Tentativas $Cfg.Tentativas | Where-Object {
                $_.EhDiretorio -and $_.Nome -match '^\d{4}$' -and [int]$_.Nome -ge $Cfg.AnoInicial -and [int]$_.Nome -le $Cfg.AnoFinal
            } | Sort-Object Nome)
        $mapaTipo = @{ MOV = 'CAGED_MOV'; FOR = 'CAGED_FOR'; EXC = 'CAGED_EXC' }
        foreach ($a in $anos) {
            $conteudo = @(Get-FtpListagem -Url $a.Url -UsarProxy $Cfg.UsarProxy -Tentativas $Cfg.Tentativas)
            $arquivos = New-Object System.Collections.Generic.List[object]
            foreach ($e in $conteudo) {
                if (-not $e.EhDiretorio) { $arquivos.Add($e) }
                elseif ($e.Nome -match '^\d{6}$' -and $e.Nome.StartsWith($a.Nome)) {
                    foreach ($x in @(Get-FtpListagem -Url $e.Url -UsarProxy $Cfg.UsarProxy -Tentativas $Cfg.Tentativas | Where-Object { -not $_.EhDiretorio })) { $arquivos.Add($x) }
                }
            }
            foreach ($f in $arquivos) {
                if ($f.Nome -notmatch '^CAGED(?<tipo>MOV|FOR|EXC)(?<per>\d{6})\.(7z|zip)$') { continue }
                $grupo = $mapaTipo[$Matches['tipo'].ToUpperInvariant()]
                $per = $Matches['per']
                $anoPer = [int]$per.Substring(0, 4)
                if ($Grupos -notcontains $grupo -or $anoPer -lt $Cfg.AnoInicial -or $anoPer -gt $Cfg.AnoFinal) { continue }
                $itens.Add([pscustomobject]@{
                        Id = ('{0}_{1}' -f $grupo, $per); Grupo = $grupo; Periodo = $per; Ano = $per.Substring(0, 4); Regiao = ''
                        NomeArquivo = $f.Nome; Url = $f.Url; Tamanho = [long]$f.Tamanho; DataRemota = $f.Data
                    })
            }
        }
    }

    # Um item por Id (se o mesmo arquivo aparecer duas vezes, fica o de data mais recente).
    $unicos = @{}
    foreach ($i in $itens) {
        if (-not $unicos.ContainsKey($i.Id) -or [string]$i.DataRemota -gt [string]$unicos[$i.Id].DataRemota) { $unicos[$i.Id] = $i }
    }
    return @($unicos.Values)
}

$Script:ColunasManifesto = @(
    'id', 'grupo', 'periodo', 'regiao', 'arquivo_origem', 'url', 'tamanho_remoto', 'data_remota', 'status',
    'arquivo_saida', 'linhas_lidas', 'linhas_saida', 'linhas_irregulares', 'encoding_origem', 'delimitador_origem',
    'metrica_coluna', 'metrica_soma', 'metrica_positivos', 'metrica_negativos', 'motor', 'assinatura',
    'inicio', 'fim', 'duracao_s', 'mensagem'
)

function Import-Manifesto {
    param([string]$Caminho)
    $m = @{}
    if (Test-Path -LiteralPath $Caminho) {
        foreach ($l in @(Import-Csv -LiteralPath $Caminho -Delimiter ';' -Encoding UTF8)) {
            if ($l.PSObject.Properties['id'] -and $l.id) { $m[$l.id] = $l }
        }
    }
    return $m
}

function Save-Manifesto {
    param([hashtable]$Manifesto, [string]$Caminho)
    $tmp = $Caminho + '.tmp'
    $w = New-Object System.IO.StreamWriter -ArgumentList $tmp, $false, $Script:Utf8ComBom
    try {
        $w.WriteLine((ConvertTo-LinhaCsvSaida -Valores $Script:ColunasManifesto -Delimitador ';'))
        foreach ($row in @($Manifesto.Values | Sort-Object grupo, periodo, regiao)) {
            $vals = foreach ($c in $Script:ColunasManifesto) {
                $p = $row.PSObject.Properties[$c]
                if ($null -ne $p -and $null -ne $p.Value) { [string]$p.Value } else { '' }
            }
            $w.WriteLine((ConvertTo-LinhaCsvSaida -Valores @($vals) -Delimitador ';'))
        }
    }
    finally { $w.Dispose() }
    Move-Item -LiteralPath $tmp -Destination $Caminho -Force
}

function Get-SituacaoItem {
    <# Motivo para processar o item ou $null se já estiver em dia. #>
    param($Item, [hashtable]$Manifesto, [hashtable]$Cfg)
    if ($Cfg.Reprocessar) { return 'Reprocessamento forçado' }
    if (-not $Manifesto.ContainsKey($Item.Id)) { return 'Novo' }
    $m = $Manifesto[$Item.Id]
    if (@('OK', 'SEM_DADOS') -notcontains [string]$m.status) { return ('Pendente (status anterior: {0})' -f $m.status) }
    if ([string]$m.tamanho_remoto -ne [string]$Item.Tamanho -or [string]$m.data_remota -ne [string]$Item.DataRemota) { return 'Republicado no FTP' }
    if ([string]$m.assinatura -ne $Cfg.Assinatura) { return 'Configuração alterada' }
    if ([string]$m.status -eq 'OK' -and -not (Test-Path -LiteralPath $Item.CaminhoSaida)) { return 'Saída ausente' }
    return $null
}

#endregion

#region ============================ 10. PRÉ-REQUISITOS ============================

function Test-SeteZip {
    param([string]$Caminho)
    try {
        $r = Invoke-Processo -Executavel $Caminho -Argumentos '' -TimeoutSegundos 30
        return (($r.Saida + $r.Erro) -match '7-Zip')
    }
    catch { return $false }
}

function Find-SeteZip {
    param([string]$Informado, [string]$PastaFerramentas, [bool]$Baixar)
    $ehWindows = ([System.Environment]::OSVersion.Platform -eq [System.PlatformID]::Win32NT)
    $candidatos = New-Object System.Collections.Generic.List[string]
    if ($Informado) { $candidatos.Add($Informado) }
    if ($ehWindows) {
        foreach ($base in @($env:ProgramFiles, ${env:ProgramFiles(x86)}, $env:ProgramW6432)) {
            if ($base) { $candidatos.Add((Join-Path $base '7-Zip\7z.exe')) }
        }
        foreach ($chave in @('HKLM:\SOFTWARE\7-Zip', 'HKLM:\SOFTWARE\WOW6432Node\7-Zip', 'HKCU:\SOFTWARE\7-Zip')) {
            try {
                $reg = Get-ItemProperty -LiteralPath $chave -ErrorAction Stop
                foreach ($prop in @('Path64', 'Path')) {
                    $pp = $reg.PSObject.Properties[$prop]
                    if ($null -ne $pp -and $pp.Value) { $candidatos.Add((Join-Path ([string]$pp.Value) '7z.exe')) }
                }
            }
            catch { }
        }
    }
    foreach ($nome in @('7z', '7zz', '7za', '7zr')) {
        $cmd = @(Get-Command -Name $nome -CommandType Application -ErrorAction SilentlyContinue) | Select-Object -First 1
        if ($null -ne $cmd) { $candidatos.Add($cmd.Path) }
    }
    $local = Join-Path $PastaFerramentas '7zr.exe'
    $candidatos.Add($local)
    foreach ($c in $candidatos) {
        if ($c -and (Test-Path -LiteralPath $c -PathType Leaf) -and (Test-SeteZip $c)) { return $c }
    }
    if ($Baixar -and $ehWindows) {
        [void](New-Item -ItemType Directory -Force -Path $PastaFerramentas)
        Write-Log 'Baixando o 7-Zip autônomo oficial (https://www.7-zip.org/a/7zr.exe)...'
        [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor [System.Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri 'https://www.7-zip.org/a/7zr.exe' -OutFile $local -UseBasicParsing
        if (Test-SeteZip $local) { return $local }
        throw 'O 7zr.exe baixado não respondeu como esperado.'
    }
    return $null
}

function Initialize-MotorCSharp {
    <#
        Compila o motor COM otimização: por padrão o Add-Type gera código em modo Debug (o JIT desliga as
        otimizações e o motor fica ~2-3x mais lento). PS 7+: -CompilerOptions; PS 5.1: -CompilerParameters.
    #>
    if ('EtlMteV1.Processador' -as [type]) { return }
    try {
        if ($PSVersionTable.PSVersion.Major -ge 6) {
            Add-Type -TypeDefinition $Script:CodigoCSharp -Language CSharp -CompilerOptions '-optimize+' -ErrorAction Stop
        }
        else {
            $cp = New-Object System.CodeDom.Compiler.CompilerParameters
            $cp.GenerateInMemory = $true
            $cp.IncludeDebugInformation = $false
            $cp.CompilerOptions = '/optimize+'
            [void]$cp.ReferencedAssemblies.Add('System.dll')
            Add-Type -TypeDefinition $Script:CodigoCSharp -Language CSharp -CompilerParameters $cp -ErrorAction Stop
        }
    }
    catch {
        if ('EtlMteV1.Processador' -as [type]) { return }
        Write-Verbose ('Compilação otimizada falhou ({0}); tentando a padrão.' -f $_.Exception.Message)
        Add-Type -TypeDefinition $Script:CodigoCSharp -Language CSharp -ErrorAction Stop
    }
}

#endregion

#region ============================ 11. EXECUÇÃO PARALELA ============================

function Invoke-Execucao {
    param([object[]]$Itens, [hashtable]$Cfg, [hashtable]$Defs, [hashtable]$Manifesto, [string]$CaminhoManifesto)
    $fila = New-Object 'System.Collections.Concurrent.ConcurrentQueue[string]'
    $estado = [hashtable]::Synchronized(@{})
    $controle = [hashtable]::Synchronized(@{ Cancelar = $false })
    $resultados = New-Object System.Collections.Generic.List[object]

    $iss = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
    foreach ($nome in $Script:FuncoesWorker) {
        $definicao = (Get-Command -Name $nome -CommandType Function).Definition
        $iss.Commands.Add((New-Object System.Management.Automation.Runspaces.SessionStateFunctionEntry -ArgumentList $nome, $definicao))
    }
    $pool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(1, $Cfg.Paralelismo, $iss, $Host)
    $pool.Open()
    $tarefas = New-Object System.Collections.Generic.List[object]
    $concluidas = 0
    $total = $Itens.Count
    $ultimoProgresso = [DateTime]::MinValue
    $atividade = 'RAIS/CAGED - UF {0}' -f $Cfg.UF
    try {
        foreach ($item in $Itens) {
            $ps = [System.Management.Automation.PowerShell]::Create()
            $ps.RunspacePool = $pool
            [void]$ps.AddScript($Script:ScriptWorker.ToString()).AddArgument($item).AddArgument($Cfg).AddArgument($Defs).AddArgument($fila).AddArgument($estado).AddArgument($controle)
            $tarefas.Add([pscustomobject]@{ Item = $item; PS = $ps; Handle = $ps.BeginInvoke(); Coletado = $false })
        }
        while ($concluidas -lt $total) {
            $msg = $null
            while ($fila.TryDequeue([ref]$msg)) {
                $partes = $msg.Split([char[]]@('|'), 3)
                Write-Log -Mensagem $partes[2] -Nivel $partes[0] -Id $partes[1]
            }
            foreach ($t in $tarefas) {
                if ($t.Coletado -or -not $t.Handle.IsCompleted) { continue }
                $t.Coletado = $true
                $concluidas++
                $r = $null
                try {
                    $saidaWorker = $t.PS.EndInvoke($t.Handle)
                    $r = @($saidaWorker | Where-Object { $null -ne $_ -and $null -ne $_.PSObject.Properties['IdItem'] }) | Select-Object -Last 1
                    foreach ($e in @($t.PS.Streams.Error)) { Write-Log -Mensagem ('Erro não fatal no worker: ' + $e.ToString()) -Nivel 'AVISO' -Id $t.Item.Id }
                }
                catch { $r = $null; Write-Log -Mensagem ('Worker falhou: ' + $_.Exception.Message) -Nivel 'ERRO' -Id $t.Item.Id }
                finally { $t.PS.Dispose() }
                while ($fila.TryDequeue([ref]$msg)) {
                    $partes = $msg.Split([char[]]@('|'), 3)
                    Write-Log -Mensagem $partes[2] -Nivel $partes[0] -Id $partes[1]
                }
                if ($null -eq $r) {
                    $r = [pscustomobject]@{ IdItem = $t.Item.Id; id = $t.Item.Id; status = 'ERRO'; mensagem = 'O worker não devolveu resultado.'; Avisos = @(); Mapeamento = @() }
                }
                Update-ManifestoItem -Item $t.Item -Resultado $r -Manifesto $Manifesto -Cfg $Cfg
                Save-Manifesto -Manifesto $Manifesto -Caminho $CaminhoManifesto
                $resultados.Add([pscustomobject]@{ Item = $t.Item; Resultado = $r })
                $estado.Remove($t.Item.Id)
                foreach ($a in @($r.Avisos)) { Write-Log -Mensagem $a -Nivel 'AVISO' -Id $t.Item.Id }
                $nivel = 'OK'
                if ($r.status -eq 'ERRO') { $nivel = 'ERRO' } elseif ($r.status -eq 'SEM_DADOS') { $nivel = 'AVISO' }
                Write-Log -Mensagem ('{0} - {1}' -f $r.status, $r.mensagem) -Nivel $nivel -Id $t.Item.Id
            }
            if (([DateTime]::UtcNow - $ultimoProgresso).TotalMilliseconds -ge 1000) {
                $ultimoProgresso = [DateTime]::UtcNow
                $ativos = New-Object System.Collections.Generic.List[string]
                foreach ($chave in @($estado.Keys)) {
                    $e = $estado[$chave]
                    if ($null -eq $e) { continue }
                    if ($e.Fase -eq 'Download') {
                        $pct = 0
                        if ($e.Total -gt 0) { $pct = [int](100 * $e.Bytes / $e.Total) }
                        $ativos.Add(('{0}: baixando {1}%' -f $chave, $pct))
                    }
                    elseif ($e.Fase -eq 'Processando') {
                        $l = $e.Linhas
                        if ($null -ne $e.Processador) { try { $l = $e.Processador.LinhasLidas } catch { } }
                        $ativos.Add(('{0}: {1:N0} linhas' -f $chave, $l))
                    }
                }
                $operacao = ($ativos -join ' | ')
                if (-not $operacao) { $operacao = 'aguardando...' }
                Write-Progress -Activity $atividade -Status ('{0} de {1} arquivos concluídos' -f $concluidas, $total) -CurrentOperation $operacao -PercentComplete ([int](100 * $concluidas / [math]::Max(1, $total)))
            }
            if ($concluidas -lt $total) { Start-Sleep -Milliseconds 300 }
        }
    }
    finally {
        $pendentes = @($tarefas | Where-Object { -not $_.Coletado })
        if ($pendentes.Count -gt 0) {
            $controle.Cancelar = $true
            foreach ($chave in @($estado.Keys)) {
                $e = $estado[$chave]
                if ($null -eq $e) { continue }
                try { if ($null -ne $e.Processador) { $e.Processador.Cancelar = $true } } catch { }
                try { if ($null -ne $e.Processo -and -not $e.Processo.HasExited) { $e.Processo.Kill() } } catch { }
            }
            foreach ($t in $pendentes) { try { [void]$t.PS.BeginStop($null, $null) } catch { } }
            Write-Log ('Execução interrompida: {0} arquivo(s) cancelado(s). Downloads parciais serão retomados na próxima execução.' -f $pendentes.Count) 'AVISO'
            try { [void]$pool.BeginClose($null, $null) } catch { }
        }
        else {
            try { $pool.Close(); $pool.Dispose() } catch { }
        }
        Write-Progress -Activity $atividade -Completed
    }
    return $resultados.ToArray()
}

function Update-ManifestoItem {
    param($Item, $Resultado, [hashtable]$Manifesto, [hashtable]$Cfg)
    $anterior = $null
    if ($Manifesto.ContainsKey($Item.Id)) { $anterior = $Manifesto[$Item.Id] }
    $linha = [ordered]@{}
    foreach ($c in $Script:ColunasManifesto) {
        $p = $Resultado.PSObject.Properties[$c]
        if ($null -ne $p) { $linha[$c] = [string]$p.Value } else { $linha[$c] = '' }
    }
    # Garante a identificação mesmo quando o worker falhou antes de montar o resultado completo.
    $linha['id'] = $Item.Id; $linha['grupo'] = $Item.Grupo; $linha['periodo'] = [string]$Item.Periodo
    $linha['regiao'] = [string]$Item.Regiao; $linha['arquivo_origem'] = $Item.NomeArquivo; $linha['url'] = $Item.Url
    $linha['tamanho_remoto'] = [string]$Item.Tamanho; $linha['data_remota'] = [string]$Item.DataRemota
    if (-not $linha['assinatura']) { $linha['assinatura'] = $Cfg.Assinatura }
    if (-not $linha['motor']) { $linha['motor'] = $Cfg.Motor }
    $Manifesto[$Item.Id] = [pscustomobject]$linha
    # Saída antiga com outro nome/extensão (ex.: .csv -> .csv.gz) não pode ficar duplicando dados.
    if ($null -ne $anterior -and $linha['status'] -eq 'OK') {
        $velho = [string]$anterior.arquivo_saida
        if ($velho -and $velho -ne $linha['arquivo_saida'] -and (Test-Path -LiteralPath $velho)) {
            Remove-Item -LiteralPath $velho -Force -ErrorAction SilentlyContinue
        }
    }
}

#endregion

#region ============================ 12. PÓS-PROCESSAMENTO ============================

function Read-LinhaBytes {
    <# Lê bytes até o primeiro LF (inclusive), descartando BOM UTF-8 inicial. #>
    param([System.IO.Stream]$Stream)
    $ms = New-Object System.IO.MemoryStream
    while (($b = $Stream.ReadByte()) -ge 0) {
        $ms.WriteByte([byte]$b)
        if ($b -eq 10) { break }
    }
    $bytes = $ms.ToArray()
    if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        $semBom = New-Object byte[] ($bytes.Length - 3)
        [Array]::Copy($bytes, 3, $semBom, 0, $semBom.Length)
        return , $semBom
    }
    return , $bytes
}

function Merge-Consolidado {
    <# Concatena as saídas de um grupo num único CSV (cabeçalho uma vez), em streaming de bytes. #>
    param([string]$Grupo, [object[]]$Linhas, [hashtable]$Cfg)
    $arquivos = @($Linhas | Where-Object { $_.status -eq 'OK' -and $_.arquivo_saida -and (Test-Path -LiteralPath $_.arquivo_saida) } | Sort-Object periodo, regiao)
    if ($arquivos.Count -eq 0) { return }
    $meta = $Cfg.Grupos[$Grupo]
    $ext = '.csv'
    if ($Cfg.Gzip) { $ext = '.csv.gz' }
    $pasta = Join-Path $Cfg.Saida 'CONSOLIDADO'
    [void](New-Item -ItemType Directory -Force -Path $pasta)
    $prefixoArq = '{0}_{1}_' -f $meta.Prefixo, $Cfg.UF
    foreach ($velho in @(Get-ChildItem -LiteralPath $pasta -File | Where-Object { $_.Name.StartsWith($prefixoArq) })) { Remove-Item -LiteralPath $velho.FullName -Force }
    $nome = '{0}{1}_{2}{3}' -f $prefixoArq, $arquivos[0].periodo, $arquivos[$arquivos.Count - 1].periodo, $ext
    $destino = Join-Path $pasta $nome
    $tmp = $destino + '.tmp'
    $fsOut = [System.IO.File]::Create($tmp)
    $out = $fsOut
    if ($Cfg.Gzip) { $out = New-Object System.IO.Compression.GZipStream -ArgumentList $fsOut, ([System.IO.Compression.CompressionMode]::Compress) }
    $ok = $false
    try {
        if ($Cfg.Bom) { $out.Write([byte[]](0xEF, 0xBB, 0xBF), 0, 3) }
        $cabRef = $null
        foreach ($a in $arquivos) {
            $fsIn = [System.IO.File]::OpenRead($a.arquivo_saida)
            $in = $fsIn
            if ($a.arquivo_saida -like '*.gz') { $in = New-Object System.IO.Compression.GZipStream -ArgumentList $fsIn, ([System.IO.Compression.CompressionMode]::Decompress) }
            try {
                $cab = Read-LinhaBytes -Stream $in
                if ($null -eq $cabRef) { $cabRef = $cab; $out.Write($cab, 0, $cab.Length) }
                elseif ([Convert]::ToBase64String($cab) -ne [Convert]::ToBase64String($cabRef)) {
                    throw ('o cabeçalho de {0} difere dos demais' -f $a.arquivo_saida)
                }
                $in.CopyTo($out, 1048576)
            }
            finally { $in.Dispose(); if (-not [object]::ReferenceEquals($in, $fsIn)) { $fsIn.Dispose() } }
        }
        $ok = $true
    }
    finally {
        $out.Dispose()
        if (-not [object]::ReferenceEquals($out, $fsOut)) { $fsOut.Dispose() }
        if (-not $ok) { Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue }
    }
    Move-Item -LiteralPath $tmp -Destination $destino -Force
    Write-Log ('Consolidado {0}: {1} arquivo(s) -> {2} ({3})' -f $Grupo, $arquivos.Count, $destino, (Format-Bytes (Get-Item -LiteralPath $destino).Length)) 'OK'
}

function Export-Dicionario {
    param([string]$Caminho, [string[]]$Grupos, [hashtable]$Defs)
    $tipoNome = @{ T = 'texto'; I = 'inteiro'; D = 'decimal' }
    $w = New-Object System.IO.StreamWriter -ArgumentList $Caminho, $false, $Script:Utf8ComBom
    try {
        $w.WriteLine((ConvertTo-LinhaCsvSaida -Valores @('grupo', 'ordem', 'coluna', 'rotulo', 'tipo', 'origem') -Delimitador ';'))
        foreach ($g in $Grupos) {
            $ordem = 0
            foreach ($c in $Defs[$g].Colunas) {
                $ordem++
                $origem = 'derivada: ' + [string]$c.Derivada
                if (-not $c.Derivada) { $origem = 'chaves: ' + (@($c.Chaves) -join ', ') }
                $w.WriteLine((ConvertTo-LinhaCsvSaida -Valores @($g, [string]$ordem, $c.Nome, $c.Rotulo, $tipoNome[$c.Tipo], $origem) -Delimitador ';'))
            }
        }
    }
    finally { $w.Dispose() }
}

function Export-Mapeamento {
    <# Histórico de cabeçalhos de origem -> colunas de saída, por arquivo processado (linhagem). #>
    param([string]$Caminho, [object[]]$Resultados)
    $cols = @('id', 'grupo', 'periodo', 'posicao_origem', 'cabecalho_origem', 'coluna_saida', 'tipo')
    $linhas = @{}
    if (Test-Path -LiteralPath $Caminho) {
        foreach ($l in @(Import-Csv -LiteralPath $Caminho -Delimiter ';' -Encoding UTF8)) {
            if (-not $linhas.ContainsKey($l.id)) { $linhas[$l.id] = New-Object System.Collections.Generic.List[object] }
            $linhas[$l.id].Add($l)
        }
    }
    foreach ($x in $Resultados) {
        $mapa = @($x.Resultado.Mapeamento)
        if ($mapa.Count -eq 0) { continue }
        $lista = New-Object System.Collections.Generic.List[object]
        foreach ($m in $mapa) {
            $lista.Add([pscustomobject]@{ id = $x.Item.Id; grupo = $x.Item.Grupo; periodo = [string]$x.Item.Periodo; posicao_origem = [string]$m.Posicao; cabecalho_origem = $m.Origem; coluna_saida = $m.Saida; tipo = $m.Tipo })
        }
        $linhas[$x.Item.Id] = $lista
    }
    $w = New-Object System.IO.StreamWriter -ArgumentList $Caminho, $false, $Script:Utf8ComBom
    try {
        $w.WriteLine((ConvertTo-LinhaCsvSaida -Valores $cols -Delimitador ';'))
        foreach ($id in @($linhas.Keys | Sort-Object)) {
            foreach ($l in $linhas[$id]) {
                $w.WriteLine((ConvertTo-LinhaCsvSaida -Valores @($l.id, $l.grupo, $l.periodo, $l.posicao_origem, $l.cabecalho_origem, $l.coluna_saida, $l.tipo) -Delimitador ';'))
            }
        }
    }
    finally { $w.Dispose() }
}

function Export-DdlSqlServer {
    param([string]$Caminho, [string[]]$Grupos, [hashtable]$Defs, [hashtable]$Cfg)
    $sb = New-Object System.Text.StringBuilder
    $ufMin = $Cfg.UF.ToLowerInvariant()
    $delimSql = [string]$Cfg.DelimitadorSaida
    if ($delimSql -eq "`t") { $delimSql = '\t' }
    [void]$sb.AppendLine('-- DDL gerada por pipeline_rais_caged.ps1 v' + $Script:VersaoScript + ' em ' + (Get-Date -Format 'yyyy-MM-dd HH:mm'))
    [void]$sb.AppendLine('-- Tabelas de staging no layout padronizado (o mesmo para todos os anos).')
    [void]$sb.AppendLine('-- ATENÇÃO: o script RECRIA as tabelas (DROP + CREATE). Revise o schema/nomes antes de executar.')
    if ([string]$Cfg.DecimalSaida -eq ',') { [void]$sb.AppendLine('-- ATENÇÃO: saída gerada com vírgula decimal; o BULK INSERT em DECIMAL exige -SeparadorDecimal Ponto.') }
    if ($Cfg.Gzip) { [void]$sb.AppendLine('-- ATENÇÃO: saída compactada (.csv.gz); descompacte antes do BULK INSERT.') }
    [void]$sb.AppendLine('IF SCHEMA_ID(''stg'') IS NULL EXEC(''CREATE SCHEMA stg'');')
    [void]$sb.AppendLine('GO')
    foreach ($g in $Grupos) {
        $meta = $Cfg.Grupos[$g]
        $tabela = '[stg].[{0}_{1}]' -f $meta.Tabela, $ufMin
        [void]$sb.AppendLine('')
        [void]$sb.AppendLine(('IF OBJECT_ID(''{0}'') IS NOT NULL DROP TABLE {0};' -f $tabela))
        [void]$sb.AppendLine(('CREATE TABLE {0} (' -f $tabela))
        $defsCols = @($Defs[$g].Colunas)
        for ($i = 0; $i -lt $defsCols.Count; $i++) {
            $c = $defsCols[$i]
            $tipo = 'VARCHAR(20)'
            if ($c.Tipo -eq 'I') { $tipo = 'INT' } elseif ($c.Tipo -eq 'D') { $tipo = 'DECIMAL(18,4)' } elseif ($c.Nome -like '*nome*') { $tipo = 'VARCHAR(100)' }
            $virg = ','
            if ($i -eq $defsCols.Count - 1) { $virg = '' }
            [void]$sb.AppendLine(('    [{0}] {1} NULL{2}' -f $c.Nome, $tipo, $virg))
        }
        [void]$sb.AppendLine(');')
        $exemplo = Join-Path (Join-Path $Cfg.Saida (Join-Path $meta.Pasta[0] $meta.Pasta[1])) ('{0}_{1}_<periodo>.csv' -f $meta.Prefixo, $Cfg.UF)
        [void]$sb.AppendLine('-- Carga (SQL Server 2017+; repita para cada arquivo ou use o consolidado):')
        [void]$sb.AppendLine(('-- BULK INSERT {0} FROM ''{1}''' -f $tabela, $exemplo))
        [void]$sb.AppendLine(("--     WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = '{0}', ROWTERMINATOR = '0x0d0a', CODEPAGE = '65001', TABLOCK);" -f $delimSql))
        [void]$sb.AppendLine('GO')
    }
    [System.IO.File]::WriteAllText($Caminho, $sb.ToString(), $Script:Utf8ComBom)
}

function Write-Resumo {
    param([hashtable]$Manifesto, [string[]]$Grupos)
    $inv = [System.Globalization.CultureInfo]::InvariantCulture
    $linhas = @($Manifesto.Values | Where-Object { $Grupos -contains $_.grupo })
    $tabela = foreach ($g in $Grupos) {
        $doGrupo = @($linhas | Where-Object { $_.grupo -eq $g })
        if ($doGrupo.Count -eq 0) { continue }
        $somaLinhas = [long]0; $somaMetrica = [decimal]0
        foreach ($l in $doGrupo) {
            $v = [long]0; if ([long]::TryParse([string]$l.linhas_saida, [ref]$v)) { $somaLinhas += $v }
            $m = [decimal]0; if ([decimal]::TryParse([string]$l.metrica_soma, [System.Globalization.NumberStyles]::Number, $inv, [ref]$m)) { $somaMetrica += $m }
        }
        [pscustomobject]@{
            Grupo     = $g
            OK        = @($doGrupo | Where-Object { $_.status -eq 'OK' }).Count
            SemDados  = @($doGrupo | Where-Object { $_.status -eq 'SEM_DADOS' }).Count
            Erro      = @($doGrupo | Where-Object { $_.status -eq 'ERRO' }).Count
            Periodos  = ('{0} a {1}' -f (@($doGrupo | Sort-Object periodo)[0].periodo), (@($doGrupo | Sort-Object periodo)[-1].periodo))
            Linhas    = $somaLinhas.ToString('N0')
            Metrica   = $Script:MetaGrupos[$g].Metrica
            SomaMetrica = $somaMetrica.ToString('N0')
        }
    }
    if ($tabela) {
        foreach ($l in (($tabela | Format-Table -AutoSize | Out-String -Width 200) -split "`r?`n")) { if ($l.Trim()) { Write-Log $l } }
    }
    # Conferência do Novo CAGED: saldo por ano = MOV + FOR - EXC (soma de saldo_movimentacao por arquivo).
    $caged = @($linhas | Where-Object { $_.grupo -like 'CAGED_*' -and $_.status -eq 'OK' })
    if ($caged.Count -gt 0) {
        Write-Log 'Conferência Novo CAGED (saldo por ano do arquivo; MOV + FOR - EXC):'
        foreach ($ano in @($caged | ForEach-Object { ([string]$_.periodo).Substring(0, 4) } | Sort-Object -Unique)) {
            $s = @{ CAGED_MOV = [decimal]0; CAGED_FOR = [decimal]0; CAGED_EXC = [decimal]0 }
            foreach ($l in @($caged | Where-Object { ([string]$_.periodo).StartsWith($ano) })) {
                $m = [decimal]0
                if ([decimal]::TryParse([string]$l.metrica_soma, [System.Globalization.NumberStyles]::Number, $inv, [ref]$m)) { $s[$l.grupo] += $m }
            }
            Write-Log ('  {0}: MOV {1:N0} | FOR {2:N0} | EXC {3:N0} | saldo {4:N0}' -f $ano, $s.CAGED_MOV, $s.CAGED_FOR, $s.CAGED_EXC, ($s.CAGED_MOV + $s.CAGED_FOR - $s.CAGED_EXC))
        }
    }
}

#endregion

#region ============================ 13. PRINCIPAL ============================

$codigoSaida = 0
try {
    if ($AnoFinal -lt $AnoInicial) { throw '-AnoFinal deve ser maior ou igual a -AnoInicial.' }
    if ($ExecutionContext.SessionState.LanguageMode -ne 'FullLanguage') {
        throw ('O PowerShell está em {0} (AppLocker/WDAC). O script precisa de FullLanguage para usar tipos .NET.' -f $ExecutionContext.SessionState.LanguageMode)
    }

    $Saida = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Saida)
    $pastaControle = Join-Path $Saida '_controle'
    $pastaLogs = Join-Path $pastaControle 'logs'
    $pastaDownloads = Join-Path $Saida '_downloads'
    $pastaFerramentas = Join-Path $Saida '_ferramentas'
    [void](New-Item -ItemType Directory -Force -Path $pastaLogs)
    [void](New-Item -ItemType Directory -Force -Path $pastaDownloads)
    $Script:ArquivoLog = Join-Path $pastaLogs ('execucao_{0}_{1}.log' -f $UF, $Script:Inicio.ToString('yyyyMMdd_HHmmss'))

    Write-Log ('pipeline_rais_caged.ps1 v{0} | PowerShell {1} ({2}) | {3}' -f $Script:VersaoScript, $PSVersionTable.PSVersion, $PSVersionTable.PSEdition, [System.Environment]::OSVersion.VersionString)
    $grupos = @(Resolve-Grupos -Bases $Bases)
    Write-Log ('UF {0} ({1}) | {2}-{3} | grupos: {4} | saída: {5}' -f $UF, $Script:CodigoUF[$UF], $AnoInicial, $AnoFinal, ($grupos -join ', '), $Saida)

    # FTP: conexões simultâneas por host (o padrão do .NET Framework é 2).
    [System.Net.ServicePointManager]::DefaultConnectionLimit = [math]::Max(16, $Paralelismo * 2)
    # ZipFile (arquivos .zip) não vem carregado no Windows PowerShell 5.1; o assembly vale para os runspaces.
    foreach ($assembly in @('System.IO.Compression', 'System.IO.Compression.FileSystem')) {
        try { Add-Type -AssemblyName $assembly -ErrorAction Stop } catch { Write-Verbose ('Assembly {0} indisponível: {1}' -f $assembly, $_.Exception.Message) }
    }

    # Motor de transformação.
    $motorEfetivo = 'PowerShell'
    if ($Motor -ne 'PowerShell') {
        try {
            Initialize-MotorCSharp
            $motorEfetivo = 'CSharp'
        }
        catch {
            if ($Motor -eq 'CSharp') { throw ('Falha ao compilar o motor C#: {0}' -f $_.Exception.Message) }
            Write-Log ('Motor C# indisponível ({0}). Usando o motor PowerShell (mesmo resultado, bem mais lento).' -f $_.Exception.Message) 'AVISO'
        }
    }
    Write-Log ('Motor de transformação: {0}' -f $motorEfetivo)

    if ($Delimitador -eq 'TAB') { $delimSaida = [char]9 } else { $delimSaida = [char]$Delimitador }
    $decSaida = [char]'.'
    if ($SeparadorDecimal -eq 'Virgula') { $decSaida = [char]',' }
    if ($delimSaida -eq [char]',' -and $decSaida -eq [char]',') { Write-Log 'Delimitador e separador decimal são vírgula: os números decimais sairão entre aspas.' 'AVISO' }
    $assinatura = 'L{0}|UF={1}|Cab={2}|Del={3}|Dec={4}|Brutos={5}|Gz={6}|Bom={7}' -f $Script:VersaoLayout, $UF, $Cabecalho, $Delimitador, $SeparadorDecimal, [int][bool]$ManterValoresBrutos, [int][bool]$CompactarSaida, [int](-not $SemBOM)

    $cfg = @{
        Saida = $Saida; PastaDownloads = $pastaDownloads; FtpBase = $FtpBase.TrimEnd('/')
        UF = $UF; CodigoUF = $Script:CodigoUF[$UF]; RegiaoUF = $Script:RegiaoRais[$UF]
        AnoInicial = $AnoInicial; AnoFinal = $AnoFinal
        ModoCabecalho = $Cabecalho; DelimitadorSaida = $delimSaida; DecimalSaida = $decSaida
        ManterBrutos = [bool]$ManterValoresBrutos; Gzip = [bool]$CompactarSaida; Bom = (-not [bool]$SemBOM)
        Motor = $motorEfetivo; SeteZip = $null; Tentativas = $Tentativas; UsarProxy = [bool]$UsarProxySistema
        ManterCompactados = [bool]$ManterCompactados; Paralelismo = $Paralelismo; Reprocessar = [bool]$Reprocessar
        Grupos = $Script:MetaGrupos; Assinatura = $assinatura
    }
    $defs = Initialize-Definicoes -MetaGrupos $Script:MetaGrupos

    # Manifesto e descoberta.
    $caminhoManifesto = Join-Path $pastaControle ('manifesto_{0}.csv' -f $UF)
    $manifesto = Import-Manifesto -Caminho $caminhoManifesto
    $extensao = '.csv'
    if ($cfg.Gzip) { $extensao = '.csv.gz' }
    $itens = @(Get-ItensRemotos -Cfg $cfg -Grupos $grupos)
    foreach ($it in $itens) {
        $meta = $Script:MetaGrupos[$it.Grupo]
        $nome = '{0}_{1}_{2}' -f $meta.Prefixo, $UF, $it.Periodo
        if ($it.Regiao -and ($it.Regiao -replace '[^A-Z]', '') -ne ($cfg.RegiaoUF -replace '[^A-Z]', '')) { $nome = '{0}_{1}' -f $nome, $it.Regiao }
        $pastaGrupo = Join-Path $Saida (Join-Path $meta.Pasta[0] $meta.Pasta[1])
        $extArq = [System.IO.Path]::GetExtension($it.NomeArquivo)
        $it | Add-Member -NotePropertyName CaminhoSaida -NotePropertyValue (Join-Path $pastaGrupo ($nome + $extensao))
        $it | Add-Member -NotePropertyName CaminhoLocal -NotePropertyValue (Join-Path $pastaDownloads ($it.Id + $extArq))
        $it | Add-Member -NotePropertyName Situacao -NotePropertyValue (Get-SituacaoItem -Item $it -Manifesto $manifesto -Cfg $cfg)
    }
    $aProcessar = @($itens | Where-Object { $null -ne $_.Situacao } | Sort-Object -Property @{ Expression = { [long]$_.Tamanho }; Descending = $true })
    $bytesBaixar = [double]0
    foreach ($it in $aProcessar) { if ($it.Tamanho -gt 0) { $bytesBaixar += $it.Tamanho } }
    Write-Log ('Encontrados {0} arquivo(s) no FTP; {1} a processar ({2} para baixar); {3} já em dia.' -f $itens.Count, $aProcessar.Count, (Format-Bytes $bytesBaixar), ($itens.Count - $aProcessar.Count))
    foreach ($g in $grupos) {
        if (@($itens | Where-Object { $_.Grupo -eq $g }).Count -eq 0) { Write-Log ('Nenhum arquivo de {0} encontrado no período {1}-{2}.' -f $g, $AnoInicial, $AnoFinal) 'AVISO' }
    }

    if ($SomenteListar) {
        $lista = $itens | Sort-Object Grupo, Periodo, Regiao | Select-Object Grupo, Periodo, Regiao, NomeArquivo,
        @{ Name = 'Tamanho'; Expression = { Format-Bytes $_.Tamanho } }, DataRemota,
        @{ Name = 'Situacao'; Expression = { if ($null -eq $_.Situacao) { 'Em dia' } else { $_.Situacao } } }
        foreach ($l in (($lista | Format-Table -AutoSize | Out-String -Width 220) -split "`r?`n")) { if ($l.Trim()) { Write-Log $l } }
        Write-Log 'Simulação (-SomenteListar): nada foi baixado.' 'OK'
    }
    else {
        if ($aProcessar.Count -gt 0) {
            $seteZip = Find-SeteZip -Informado $Caminho7Zip -PastaFerramentas $pastaFerramentas -Baixar ([bool]$Baixar7Zip)
            if (-not $seteZip) {
                throw ('7-Zip não encontrado. Instale (winget install 7zip.7zip), informe -Caminho7Zip ou use -Baixar7Zip.')
            }
            $cfg.SeteZip = $seteZip
            Write-Log ('7-Zip: {0}' -f $seteZip)
            try {
                $raiz = [System.IO.Path]::GetPathRoot($Saida)
                $livre = (New-Object System.IO.DriveInfo -ArgumentList $raiz).AvailableFreeSpace
                $minimo = [long]([math]::Min($bytesBaixar, 4GB) + 3 * $bytesBaixar)
                if ($livre -lt $minimo) { Write-Log ('Pouco espaço livre em {0}: {1} (estimativa necessária: ~{2}).' -f $raiz, (Format-Bytes $livre), (Format-Bytes $minimo)) 'AVISO' }
            }
            catch { }

            $resultados = @(Invoke-Execucao -Itens $aProcessar -Cfg $cfg -Defs $defs -Manifesto $manifesto -CaminhoManifesto $caminhoManifesto)
            Export-Mapeamento -Caminho (Join-Path $pastaControle ('mapeamento_cabecalhos_{0}.csv' -f $UF)) -Resultados $resultados
            if (@($resultados | Where-Object { $_.Resultado.status -eq 'ERRO' }).Count -gt 0) { $codigoSaida = 1 }
        }
        else {
            Write-Log 'Tudo em dia: nenhum arquivo novo, republicado ou pendente.' 'OK'
        }

        Export-Dicionario -Caminho (Join-Path $pastaControle 'dicionario_colunas.csv') -Grupos $grupos -Defs $defs
        if ($Cabecalho -ne 'Original') {
            Export-DdlSqlServer -Caminho (Join-Path $pastaControle ('ddl_sqlserver_{0}.sql' -f $UF)) -Grupos $grupos -Defs $defs -Cfg $cfg
        }
        if ($Consolidar) {
            if ($Cabecalho -eq 'Original') {
                Write-Log 'Consolidação ignorada: com -Cabecalho Original os layouts variam entre anos.' 'AVISO'
            }
            else {
                foreach ($g in $grupos) {
                    try { Merge-Consolidado -Grupo $g -Linhas @($manifesto.Values | Where-Object { $_.grupo -eq $g }) -Cfg $cfg }
                    catch { Write-Log ('Consolidação de {0} falhou: {1}' -f $g, $_.Exception.Message) 'ERRO'; $codigoSaida = 1 }
                }
            }
        }
        Write-Resumo -Manifesto $manifesto -Grupos $grupos
    }
    $duracao = (Get-Date) - $Script:Inicio
    Write-Log ('Fim. Duração: {0:hh\:mm\:ss}. Manifesto: {1}' -f $duracao, $caminhoManifesto) 'OK'
}
catch {
    $codigoSaida = 2
    $mensagemFalha = $_.Exception.Message
    try { Write-Log ('FALHA: {0}' -f $mensagemFalha) 'ERRO' } catch { Write-Host ('FALHA: {0}' -f $mensagemFalha) -ForegroundColor Red }
}
exit $codigoSaida

#endregion
