#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PIPELINE ESTBAN MUNICIPAL - DADOS ESTRATÉGICOS BCB
================================================================================
Repositório : https://github.com/mazoir/dados_publicos
Fonte       : Banco Central do Brasil - Documento 4500 COSIF
Dataset     : Estatística Bancária por Município (ESTBAN)
Período     : 01/2023 a 09/2025 (33 meses)
Responsável : Mazoir Ceará - Sicoob Credicom

Descrição:
    Este script baixa os arquivos ESTBAN municipais do BCB, extrai apenas
    os verbetes estratégicos para análise de crédito, depósitos, provisão,
    inadimplência e market share, calcula KPIs derivados e publica no
    repositório GitHub para consumo direto no Power BI.

Verbetes Estratégicos:
    CRÉDITO (ATIVO):
        160 - Total Operações de Crédito
        161 - Empréstimos e Títulos Descontados (Capital de Giro)
        162 - Financiamentos (Veículos, Bens)
        163 - Financiamentos Rurais - Custeio/Invest. Agrícola
        167 - Financiamentos Agroindustriais
        169 - Financiamentos Imobiliários
        171 - Outras Operações de Crédito (PF)
    PROVISÃO:
        174 - Provisão p/ Créditos de Liquidação Duvidosa
    CAPTAÇÃO (PASSIVO):
        401-419 - Depósitos à Vista (consolidado)
        420 - Depósitos de Poupança
        432 - Depósitos a Prazo (CDB/RDB)
    PATRIMONIAL:
        399 - Total do Ativo
        610 - Patrimônio Líquido

Requisitos:
    pip install pandas requests tqdm

Uso:
    python pipeline_estban.py

    Para executar sem git push:
    python pipeline_estban.py --no-push

    Para definir período customizado:
    python pipeline_estban.py --inicio 2024-01 --fim 2025-09
================================================================================
"""

import os
import sys
import io
import re
import zipfile
import argparse
import subprocess
import warnings
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================

# URL base do BCB para ESTBAN municipal
BCB_BASE_URL = "https://www.bcb.gov.br/content/estatisticas/estatistica_bancaria_estban/municipio"

# Período padrão
PERIODO_INICIO_DEFAULT = "2023-01"
PERIODO_FIM_DEFAULT = "2025-09"

# Caminhos do repositório
REPO_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = REPO_DIR / "dados" / "bcb" / "estban"
OUTPUT_FILE = OUTPUT_DIR / "estban_municipal_estrategico.csv"

# Limite GitHub (aviso se > 90MB)
GITHUB_FILE_LIMIT_MB = 90

# Encoding padrão dos arquivos BCB
BCB_ENCODING = "latin-1"
BCB_SEPARATOR = ";"
BCB_SKIPROWS = 2

# Timeout e retries para download
DOWNLOAD_TIMEOUT = 120
DOWNLOAD_RETRIES = 3

# ==============================================================================
# MAPEAMENTO DE VERBETES ESTRATÉGICOS
# ==============================================================================

# Verbetes individuais a manter (número → nome amigável)
VERBETES_INDIVIDUAIS = {
    160: "OP_CREDITO_TOTAL",
    161: "EMPRESTIMOS_TITULOS",
    162: "FINANCIAMENTOS",
    163: "FIN_RURAIS_AGRICOLA",
    167: "FIN_AGROINDUSTRIAIS",
    169: "FIN_IMOBILIARIOS",
    171: "OUTRAS_OP_CREDITO",
    174: "PROVISAO_CREDITO",
    399: "ATIVO_TOTAL",
    420: "DEP_POUPANCA",
    432: "DEP_PRAZO",
    610: "PATRIMONIO_LIQUIDO",
}

# Verbetes de depósito à vista (401-419) → serão consolidados
VERBETES_DEP_VISTA = list(range(401, 420))  # 401 a 419

# Todos os verbetes necessários (para filtro de colunas)
TODOS_VERBETES = list(VERBETES_INDIVIDUAIS.keys()) + VERBETES_DEP_VISTA

# Colunas de identificação (não-verbete)
COLUNAS_ID_POSSIVEIS = [
    "#DATA_BASE", "DATA_BASE",
    "UF",
    "CODMUN_IBGE", "CODMUN", "COD_MUN", "CODIGO_MUNICIPIO",
    "MUNICIPIO", "NOME_MUNICIPIO",
    "CNPJ",
    "NOME_INSTITUICAO", "INSTITUICAO", "NOME_IF",
    "CODMUN_BCB", "COD_MUN_BCB",
    "AGENCIA",  # pode existir em alguns arquivos
]

# Colunas finais de saída (ordem)
COLUNAS_SAIDA_ID = [
    "DATA_BASE",
    "UF",
    "CODMUN",
    "MUNICIPIO",
    "CNPJ",
    "NOME_INSTITUICAO",
]

COLUNAS_SAIDA_VERBETES = [
    "OP_CREDITO_TOTAL",
    "EMPRESTIMOS_TITULOS",
    "FINANCIAMENTOS",
    "FIN_RURAIS_AGRICOLA",
    "FIN_AGROINDUSTRIAIS",
    "FIN_IMOBILIARIOS",
    "OUTRAS_OP_CREDITO",
    "PROVISAO_CREDITO",
    "ATIVO_TOTAL",
    "DEP_VISTA_TOTAL",
    "DEP_POUPANCA",
    "DEP_PRAZO",
    "PATRIMONIO_LIQUIDO",
]

COLUNAS_SAIDA_KPIS = [
    "IDX_PROVISAO_CREDITO",
    "PENETRACAO_RURAL",
    "MIX_POUPANCA",
]


# ==============================================================================
# FUNÇÕES DE GERAÇÃO DE URLs
# ==============================================================================

def gerar_periodos(inicio: str, fim: str) -> list[dict]:
    """
    Gera lista de períodos (YYYYMM) entre início e fim.
    
    Args:
        inicio: "YYYY-MM" (ex: "2023-01")
        fim: "YYYY-MM" (ex: "2025-09")
    
    Returns:
        Lista de dicts com 'periodo' e 'urls' (tentativas em ordem de prioridade)
    """
    periodos = []
    ano_ini, mes_ini = map(int, inicio.split("-"))
    ano_fim, mes_fim = map(int, fim.split("-"))

    ano, mes = ano_ini, mes_ini
    while (ano < ano_fim) or (ano == ano_fim and mes <= mes_fim):
        yyyymm = f"{ano:04d}{mes:02d}"

        # Padrões de URL conhecidos (ordem de tentativa)
        # 2023-02 em diante: .csv.zip
        # 2023-01: .csv (sem zip)
        # Antes de 2023: .ZIP (maiúsculo)
        urls = []
        if ano >= 2023 and not (ano == 2023 and mes == 1):
            urls.append(f"{BCB_BASE_URL}/{yyyymm}_ESTBAN.csv.zip")
        if ano == 2023 and mes == 1:
            urls.append(f"{BCB_BASE_URL}/{yyyymm}_ESTBAN.csv")
        # Fallbacks
        urls.append(f"{BCB_BASE_URL}/{yyyymm}_ESTBAN.csv.zip")
        urls.append(f"{BCB_BASE_URL}/{yyyymm}_ESTBAN.ZIP")
        urls.append(f"{BCB_BASE_URL}/{yyyymm}_ESTBAN.zip")

        # Remove duplicatas mantendo ordem
        urls_unicos = list(dict.fromkeys(urls))

        periodos.append({
            "periodo": yyyymm,
            "label": f"{mes:02d}/{ano:04d}",
            "urls": urls_unicos,
        })

        mes += 1
        if mes > 12:
            mes = 1
            ano += 1

    return periodos


# ==============================================================================
# FUNÇÕES DE DOWNLOAD
# ==============================================================================

def download_arquivo(urls: list[str], label: str) -> Optional[bytes]:
    """
    Tenta baixar arquivo de uma lista de URLs (fallback).
    
    Args:
        urls: Lista de URLs para tentar
        label: Label para log (ex: "01/2023")
    
    Returns:
        bytes do arquivo ou None se falhar
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/131.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    })

    for url in urls:
        for tentativa in range(DOWNLOAD_RETRIES):
            try:
                resp = session.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
                if resp.status_code == 200:
                    content = resp.content
                    if len(content) > 100:  # Mínimo razoável
                        return content
                elif resp.status_code == 404:
                    break  # Próxima URL, este padrão não existe
                else:
                    continue  # Retry
            except requests.exceptions.RequestException:
                if tentativa < DOWNLOAD_RETRIES - 1:
                    import time
                    time.sleep(2 * (tentativa + 1))
                continue

    return None


def extrair_csv_de_bytes(conteudo: bytes, url: str) -> Optional[pd.DataFrame]:
    """
    Extrai DataFrame de bytes (ZIP ou CSV direto).
    
    Args:
        conteudo: bytes do arquivo baixado
        url: URL original (para detectar formato)
    
    Returns:
        DataFrame ou None
    """
    try:
        # Verifica se é ZIP (magic bytes: PK)
        if conteudo[:2] == b"PK" or url.lower().endswith((".zip", ".csv.zip")):
            with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
                # Encontra o CSV dentro do ZIP
                csv_names = [
                    n for n in zf.namelist()
                    if n.lower().endswith(".csv")
                ]
                if not csv_names:
                    # Tenta qualquer arquivo que não seja diretório
                    csv_names = [
                        n for n in zf.namelist()
                        if not n.endswith("/")
                    ]

                if not csv_names:
                    print(f"    [AVISO] ZIP vazio ou sem CSV")
                    return None

                with zf.open(csv_names[0]) as f:
                    raw = f.read()
                    return _parse_csv_bytes(raw)
        else:
            # CSV direto (sem compressão)
            return _parse_csv_bytes(conteudo)

    except (zipfile.BadZipFile, Exception) as e:
        print(f"    [ERRO] Falha ao extrair: {e}")
        return None


def _parse_csv_bytes(raw: bytes) -> Optional[pd.DataFrame]:
    """
    Faz o parse dos bytes CSV brutos do ESTBAN.
    
    Trata os 2 headers informativos do BCB e detecta encoding.
    """
    # Tenta detectar encoding
    for enc in [BCB_ENCODING, "utf-8", "cp1252"]:
        try:
            texto = raw.decode(enc)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        texto = raw.decode(BCB_ENCODING, errors="replace")

    # O BCB coloca 2 linhas de cabeçalho antes dos dados
    # Linha 1: nome do relatório
    # Linha 2: data de referência
    # Linha 3: cabeçalho real das colunas
    linhas = texto.split("\n")

    # Detecta onde começa o header real (linha com "DATA_BASE" ou "#DATA_BASE")
    header_idx = 0
    for i, linha in enumerate(linhas[:10]):
        if "DATA_BASE" in linha.upper() or "CODMUN" in linha.upper():
            header_idx = i
            break

    # Reconstrói CSV a partir do header real
    csv_limpo = "\n".join(linhas[header_idx:])

    try:
        df = pd.read_csv(
            io.StringIO(csv_limpo),
            sep=BCB_SEPARATOR,
            encoding=enc,
            low_memory=False,
            dtype=str,  # Tudo como string inicialmente
        )
        return df
    except Exception as e:
        print(f"    [ERRO] Falha no parse CSV: {e}")
        return None


# ==============================================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# ==============================================================================

def identificar_colunas_verbetes(colunas: list[str]) -> dict:
    """
    Mapeia colunas do DataFrame para números de verbete.
    
    Os nomes de coluna seguem padrões como:
        VERBETE_160_OPERACOES_DE_CREDITO
        VERBETE_161_EMPRESTIMOS_E_TITULOS_DESCONTADOS
    
    Returns:
        Dict {numero_verbete: nome_coluna_original}
    """
    mapa = {}
    padrao = re.compile(r"VERBETE[_\s]*(\d{3})", re.IGNORECASE)

    for col in colunas:
        match = padrao.search(col)
        if match:
            num = int(match.group(1))
            if num in TODOS_VERBETES:
                mapa[num] = col

    return mapa


def identificar_colunas_id(colunas: list[str]) -> dict:
    """
    Identifica colunas de identificação (não-verbete).
    
    Returns:
        Dict {nome_padrao: nome_coluna_original}
    """
    mapa = {}
    colunas_upper = {c.upper().strip().replace("#", ""): c for c in colunas}

    # DATA_BASE
    for candidato in ["DATA_BASE", "DTBASE", "DT_BASE", "DATA BASE"]:
        if candidato in colunas_upper:
            mapa["DATA_BASE"] = colunas_upper[candidato]
            break
    # Se não encontrou, tenta com #
    if "DATA_BASE" not in mapa:
        for c in colunas:
            if "DATA" in c.upper() and "BASE" in c.upper():
                mapa["DATA_BASE"] = c
                break

    # UF
    for candidato in ["UF", "ESTADO", "SIGLA_UF"]:
        if candidato in colunas_upper:
            mapa["UF"] = colunas_upper[candidato]
            break

    # CODMUN (código do município)
    for candidato in ["CODMUN_IBGE", "CODMUN", "COD_MUN", "CODIGO_MUNICIPIO",
                       "CODMUN_BCB", "COD_MUN_BCB", "CODMUNIC"]:
        if candidato in colunas_upper:
            mapa["CODMUN"] = colunas_upper[candidato]
            break

    # MUNICIPIO
    for candidato in ["MUNICIPIO", "NOME_MUNICIPIO", "NM_MUNICIPIO", "MUNICÍPIO"]:
        if candidato in colunas_upper:
            mapa["MUNICIPIO"] = colunas_upper[candidato]
            break

    # CNPJ
    for candidato in ["CNPJ", "CNPJ_IF", "CNPJ_INSTITUICAO"]:
        if candidato in colunas_upper:
            mapa["CNPJ"] = colunas_upper[candidato]
            break

    # NOME_INSTITUICAO
    for candidato in ["NOME_INSTITUICAO", "INSTITUICAO", "NOME_IF",
                       "NM_INSTITUICAO", "NOME INSTITUICAO"]:
        if candidato in colunas_upper:
            mapa["NOME_INSTITUICAO"] = colunas_upper[candidato]
            break

    return mapa


def transformar_dataframe(df: pd.DataFrame, periodo: str) -> Optional[pd.DataFrame]:
    """
    Transforma um DataFrame ESTBAN bruto no formato estratégico.
    
    1. Identifica e renomeia colunas de ID
    2. Filtra apenas verbetes estratégicos
    3. Consolida depósitos à vista (401-419)
    4. Calcula KPIs derivados
    5. Formata tipos de dados
    
    Args:
        df: DataFrame bruto do CSV ESTBAN
        periodo: "YYYYMM" para fallback de DATA_BASE
    
    Returns:
        DataFrame transformado ou None
    """
    if df is None or df.empty:
        return None

    # Remove colunas totalmente vazias e linhas vazias
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)

    if df.empty:
        return None

    # --- Identificar colunas ---
    colunas_id = identificar_colunas_id(df.columns.tolist())
    colunas_verbete = identificar_colunas_verbetes(df.columns.tolist())

    if not colunas_verbete:
        print(f"    [AVISO] Nenhum verbete estratégico encontrado nas colunas")
        return None

    # --- Montar DataFrame de saída ---
    resultado = pd.DataFrame()

    # Colunas de identificação
    if "DATA_BASE" in colunas_id:
        resultado["DATA_BASE"] = df[colunas_id["DATA_BASE"]].astype(str).str.strip()
    else:
        # Usa o período do arquivo como fallback
        resultado["DATA_BASE"] = periodo

    if "UF" in colunas_id:
        resultado["UF"] = df[colunas_id["UF"]].astype(str).str.strip()

    if "CODMUN" in colunas_id:
        resultado["CODMUN"] = df[colunas_id["CODMUN"]].astype(str).str.strip()

    if "MUNICIPIO" in colunas_id:
        resultado["MUNICIPIO"] = (
            df[colunas_id["MUNICIPIO"]]
            .astype(str)
            .str.strip()
            .str.upper()
        )

    if "CNPJ" in colunas_id:
        resultado["CNPJ"] = (
            df[colunas_id["CNPJ"]]
            .astype(str)
            .str.strip()
            .str.replace(r"\D", "", regex=True)
            .str.zfill(8)
            .str[:8]  # Primeiros 8 dígitos (raiz do CNPJ)
        )

    if "NOME_INSTITUICAO" in colunas_id:
        resultado["NOME_INSTITUICAO"] = (
            df[colunas_id["NOME_INSTITUICAO"]]
            .astype(str)
            .str.strip()
        )

    # --- Verbetes individuais ---
    for num_verbete, nome_amigavel in VERBETES_INDIVIDUAIS.items():
        if num_verbete in colunas_verbete:
            col_original = colunas_verbete[num_verbete]
            resultado[nome_amigavel] = _converter_numerico(df[col_original])
        else:
            resultado[nome_amigavel] = 0.0

    # --- Depósitos à Vista consolidado (401-419) ---
    cols_dep_vista = []
    for num in VERBETES_DEP_VISTA:
        if num in colunas_verbete:
            cols_dep_vista.append(colunas_verbete[num])

    if cols_dep_vista:
        dep_vista_df = df[cols_dep_vista].apply(_converter_numerico)
        resultado["DEP_VISTA_TOTAL"] = dep_vista_df.sum(axis=1)
    else:
        resultado["DEP_VISTA_TOTAL"] = 0.0

    # --- KPIs derivados ---
    # 1. Índice Provisão/Crédito (%)
    #    |PROVISAO_CREDITO| / OP_CREDITO_TOTAL * 100
    #    Provisão é negativa no COSIF, usar valor absoluto
    resultado["IDX_PROVISAO_CREDITO"] = (
        resultado["PROVISAO_CREDITO"].abs()
        / resultado["OP_CREDITO_TOTAL"].replace(0, float("nan"))
        * 100
    ).round(2)

    # 2. Penetração Rural (%)
    #    FIN_RURAIS_AGRICOLA / OP_CREDITO_TOTAL * 100
    resultado["PENETRACAO_RURAL"] = (
        resultado["FIN_RURAIS_AGRICOLA"]
        / resultado["OP_CREDITO_TOTAL"].replace(0, float("nan"))
        * 100
    ).round(2)

    # 3. Mix Poupança (%)
    #    DEP_POUPANCA / (DEP_VISTA_TOTAL + DEP_POUPANCA + DEP_PRAZO) * 100
    total_captacao = (
        resultado["DEP_VISTA_TOTAL"]
        + resultado["DEP_POUPANCA"]
        + resultado["DEP_PRAZO"]
    )
    resultado["MIX_POUPANCA"] = (
        resultado["DEP_POUPANCA"]
        / total_captacao.replace(0, float("nan"))
        * 100
    ).round(2)

    # --- Formatar DATA_BASE como date (YYYY-MM-01) ---
    resultado["DATA_BASE"] = resultado["DATA_BASE"].apply(_formatar_data_base)

    # Preencher NaN nos KPIs com None (será vazio no CSV)
    for col_kpi in COLUNAS_SAIDA_KPIS:
        resultado[col_kpi] = resultado[col_kpi].where(
            resultado[col_kpi].notna() & ~resultado[col_kpi].isin([float("inf"), float("-inf")]),
            other=None,
        )

    # --- Ordenar colunas ---
    colunas_finais = []
    for col in COLUNAS_SAIDA_ID + COLUNAS_SAIDA_VERBETES + COLUNAS_SAIDA_KPIS:
        if col in resultado.columns:
            colunas_finais.append(col)

    resultado = resultado[colunas_finais]

    return resultado


def _converter_numerico(serie: pd.Series) -> pd.Series:
    """Converte série string para numérico, tratando formatação BR."""
    return (
        serie
        .astype(str)
        .str.strip()
        .str.replace(".", "", regex=False)   # Remove separador de milhar
        .str.replace(",", ".", regex=False)   # Troca vírgula decimal
        .str.replace(r"[^\d.\-]", "", regex=True)  # Remove caracteres inválidos
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )


def _formatar_data_base(valor: str) -> str:
    """
    Formata DATA_BASE para YYYY-MM-01.
    
    Aceita formatos: YYYYMM, YYYY-MM, YYYYMMDD, DD/MM/YYYY
    """
    valor = str(valor).strip().replace("#", "")

    # YYYYMM (ex: 202301)
    if re.match(r"^\d{6}$", valor):
        return f"{valor[:4]}-{valor[4:6]}-01"

    # YYYYMMDD (ex: 20230101)
    if re.match(r"^\d{8}$", valor):
        return f"{valor[:4]}-{valor[4:6]}-01"

    # YYYY-MM (ex: 2023-01)
    if re.match(r"^\d{4}-\d{2}$", valor):
        return f"{valor}-01"

    # DD/MM/YYYY
    match = re.match(r"(\d{2})/(\d{2})/(\d{4})", valor)
    if match:
        return f"{match.group(3)}-{match.group(2)}-01"

    # Fallback
    return valor


# ==============================================================================
# PIPELINE PRINCIPAL
# ==============================================================================

def executar_pipeline(inicio: str, fim: str, fazer_push: bool = True):
    """
    Executa o pipeline completo:
    1. Gera lista de períodos/URLs
    2. Baixa cada arquivo
    3. Extrai e transforma
    4. Consolida tudo
    5. Salva CSV otimizado
    6. Atualiza README
    7. Git commit + push
    """
    print("\n" + "=" * 70)
    print("  PIPELINE ESTBAN MUNICIPAL - DADOS ESTRATÉGICOS BCB")
    print("  Sicoob Credicom - Inteligência de Mercado")
    print("=" * 70)
    print(f"  Período    : {inicio} a {fim}")
    print(f"  Repositório: {REPO_DIR}")
    print(f"  Saída      : {OUTPUT_FILE}")
    print(f"  Git Push   : {'Sim' if fazer_push else 'Não'}")
    print("=" * 70)

    # 1. Gerar períodos
    periodos = gerar_periodos(inicio, fim)
    total = len(periodos)
    print(f"\n[1/6] {total} períodos para processar ({inicio} a {fim})")

    # 2. Criar diretório de saída
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 3. Download + Transformação
    print(f"\n[2/6] Baixando e processando arquivos do BCB...")
    dfs = []
    erros = []

    for item in tqdm(periodos, desc="Processando", ncols=80, unit="mês"):
        periodo = item["periodo"]
        label = item["label"]
        urls = item["urls"]

        # Download
        conteudo = download_arquivo(urls, label)
        if conteudo is None:
            erros.append(label)
            tqdm.write(f"  [FALHA] {label} - Arquivo não disponível")
            continue

        # Extrair CSV
        df_bruto = extrair_csv_de_bytes(conteudo, urls[0])
        if df_bruto is None or df_bruto.empty:
            erros.append(label)
            tqdm.write(f"  [FALHA] {label} - Erro na extração")
            continue

        # Transformar
        df_transformado = transformar_dataframe(df_bruto, periodo)
        if df_transformado is None or df_transformado.empty:
            erros.append(label)
            tqdm.write(f"  [FALHA] {label} - Erro na transformação")
            continue

        dfs.append(df_transformado)
        tqdm.write(f"  [OK] {label} → {len(df_transformado):,} registros")

    # 4. Consolidar
    print(f"\n[3/6] Consolidando dados...")
    if not dfs:
        print("[ERRO FATAL] Nenhum arquivo processado com sucesso!")
        sys.exit(1)

    df_final = pd.concat(dfs, ignore_index=True)

    # Ordenar: DATA_BASE, UF, MUNICIPIO, NOME_INSTITUICAO
    df_final = df_final.sort_values(
        ["DATA_BASE", "UF", "MUNICIPIO", "NOME_INSTITUICAO"],
        ignore_index=True,
    )

    # Remove duplicatas exatas
    antes = len(df_final)
    df_final = df_final.drop_duplicates()
    depois = len(df_final)
    if antes != depois:
        print(f"  Duplicatas removidas: {antes - depois:,}")

    print(f"  Total consolidado: {len(df_final):,} registros")
    print(f"  Períodos OK: {len(dfs)}/{total}")
    if erros:
        print(f"  Períodos com falha: {', '.join(erros)}")

    # Estatísticas
    print(f"\n  Resumo dos dados:")
    print(f"    UFs distintas        : {df_final['UF'].nunique()}")
    if "CODMUN" in df_final.columns:
        print(f"    Municípios distintos : {df_final['CODMUN'].nunique():,}")
    print(f"    Instituições distintas: {df_final['NOME_INSTITUICAO'].nunique():,}")
    print(f"    Período              : {df_final['DATA_BASE'].min()} a {df_final['DATA_BASE'].max()}")

    # 5. Salvar CSV
    print(f"\n[4/6] Salvando arquivo...")
    df_final.to_csv(
        OUTPUT_FILE,
        sep=";",
        index=False,
        encoding="utf-8",
        float_format="%.2f",
    )

    tamanho_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"  Arquivo: {OUTPUT_FILE.name}")
    print(f"  Tamanho: {tamanho_mb:.1f} MB")

    if tamanho_mb > GITHUB_FILE_LIMIT_MB:
        print(f"\n  [AVISO] Arquivo excede {GITHUB_FILE_LIMIT_MB}MB!")
        print(f"  Gerando versão compactada (.csv.gz)...")
        output_gz = OUTPUT_FILE.with_suffix(".csv.gz")
        df_final.to_csv(
            output_gz,
            sep=";",
            index=False,
            encoding="utf-8",
            float_format="%.2f",
            compression="gzip",
        )
        tamanho_gz = output_gz.stat().st_size / (1024 * 1024)
        print(f"  Versão gzip: {tamanho_gz:.1f} MB")

        if tamanho_gz < GITHUB_FILE_LIMIT_MB:
            # Remove CSV grande, mantém .gz
            OUTPUT_FILE.unlink()
            print(f"  CSV removido. Use o .csv.gz no Power BI.")

    # 6. Atualizar README
    print(f"\n[5/6] Atualizando README.md...")
    atualizar_readme(df_final, inicio, fim, len(dfs), total, erros, tamanho_mb)

    # 7. Git push
    if fazer_push:
        print(f"\n[6/6] Publicando no GitHub...")
        git_push()
    else:
        print(f"\n[6/6] Git push desabilitado (--no-push)")

    # Relatório final
    print("\n" + "=" * 70)
    print("  PIPELINE CONCLUÍDO COM SUCESSO!")
    print("=" * 70)
    print(f"  Arquivo    : {OUTPUT_FILE.name}")
    print(f"  Registros  : {len(df_final):,}")
    print(f"  Tamanho    : {tamanho_mb:.1f} MB")
    print(f"  Períodos   : {len(dfs)}/{total} OK")
    print(f"  GitHub     : https://github.com/mazoir/dados_publicos")
    print("=" * 70 + "\n")


# ==============================================================================
# README
# ==============================================================================

def atualizar_readme(
    df: pd.DataFrame,
    inicio: str,
    fim: str,
    periodos_ok: int,
    periodos_total: int,
    erros: list,
    tamanho_mb: float,
):
    """Gera/atualiza o README.md do repositório."""

    # Detecta se existe .csv.gz
    arquivo_nome = OUTPUT_FILE.name
    arquivo_gz = OUTPUT_FILE.with_suffix(".csv.gz")
    if arquivo_gz.exists() and not OUTPUT_FILE.exists():
        arquivo_nome = arquivo_gz.name

    agora = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")

    readme = f"""# 📊 Dados Públicos - BCB

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

* **Arquivo:** `dados/bcb/estban/{arquivo_nome}`
* **Fonte:** [BCB - ESTBAN Documento 4500](https://www.bcb.gov.br/estatisticas/estatisticabancariamunicipios)
* **Período:** {inicio} a {fim} ({periodos_ok}/{periodos_total} meses)
* **Separador:** `;`
* **Encoding:** `UTF-8`
* **Tamanho:** ~{tamanho_mb:.0f} MB

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
    Tipagem = Table.TransformColumnTypes(Cabecalho, {{
        {{"CNPJ", type text}},
        {{"Total de Cooperados", Int64.Type}},
        {{"Cooperados PF", Int64.Type}},
        {{"Cooperados PJ", Int64.Type}},
        {{"Sexo Feminino", Int64.Type}},
        {{"Sexo Masculino", Int64.Type}},
        {{"Sexo nao Informado", Int64.Type}},
        {{"Periodo", type date}}
    }})
in
    Tipagem
```

### ESTBAN Municipal Estratégico

```
let
    Url = "https://raw.githubusercontent.com/mazoir/dados_publicos/main/dados/bcb/estban/{arquivo_nome}",
    Fonte = Csv.Document(Web.Contents(Url), [Delimiter=";", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Cabecalho = Table.PromoteHeaders(Fonte, [PromoteAllScalars=true]),
    Tipagem = Table.TransformColumnTypes(Cabecalho, {{
        {{"DATA_BASE", type date}},
        {{"UF", type text}},
        {{"CODMUN", type text}},
        {{"MUNICIPIO", type text}},
        {{"CNPJ", type text}},
        {{"NOME_INSTITUICAO", type text}},
        {{"OP_CREDITO_TOTAL", type number}},
        {{"EMPRESTIMOS_TITULOS", type number}},
        {{"FINANCIAMENTOS", type number}},
        {{"FIN_RURAIS_AGRICOLA", type number}},
        {{"FIN_AGROINDUSTRIAIS", type number}},
        {{"FIN_IMOBILIARIOS", type number}},
        {{"OUTRAS_OP_CREDITO", type number}},
        {{"PROVISAO_CREDITO", type number}},
        {{"ATIVO_TOTAL", type number}},
        {{"DEP_VISTA_TOTAL", type number}},
        {{"DEP_POUPANCA", type number}},
        {{"DEP_PRAZO", type number}},
        {{"PATRIMONIO_LIQUIDO", type number}},
        {{"IDX_PROVISAO_CREDITO", type number}},
        {{"PENETRACAO_RURAL", type number}},
        {{"MIX_POUPANCA", type number}}
    }})
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

{agora}

---

*Gerado automaticamente pelos pipelines BCB.*
"""

    readme_path = REPO_DIR / "README.md"
    readme_path.write_text(readme, encoding="utf-8")
    print(f"  README.md atualizado")


# ==============================================================================
# GIT
# ==============================================================================

def git_push():
    """Executa git add, commit e push."""
    try:
        os.chdir(REPO_DIR)

        # Configura LFS para arquivos grandes (>50MB)
        arquivo_csv = OUTPUT_FILE
        arquivo_gz = OUTPUT_FILE.with_suffix(".csv.gz")

        for arq in [arquivo_csv, arquivo_gz]:
            if arq.exists() and arq.stat().st_size > 50 * 1024 * 1024:
                print(f"  [INFO] Arquivo > 50MB, configurando Git LFS...")
                subprocess.run(["git", "lfs", "install"], capture_output=True)
                subprocess.run(
                    ["git", "lfs", "track", f"dados/bcb/estban/{arq.name}"],
                    capture_output=True,
                )
                subprocess.run(["git", "add", ".gitattributes"], capture_output=True)

        # Git add
        subprocess.run(["git", "add", "dados/bcb/estban/"], check=True, capture_output=True)
        subprocess.run(["git", "add", "README.md"], check=True, capture_output=True)
        subprocess.run(["git", "add", "pipeline_estban.py"], check=True, capture_output=True)

        # Verifica se .gitattributes foi criado
        gitattr = REPO_DIR / ".gitattributes"
        if gitattr.exists():
            subprocess.run(["git", "add", ".gitattributes"], capture_output=True)

        # Commit
        agora = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")
        msg = f"feat: ESTBAN municipal estratégico - atualização {agora}"
        result = subprocess.run(
            ["git", "commit", "-m", msg],
            capture_output=True,
            text=True,
        )

        if "nothing to commit" in result.stdout + result.stderr:
            print("  Nenhuma alteração para commitar.")
            return

        print(f"  Commit: {msg}")

        # Push
        result = subprocess.run(
            ["git", "push", "origin", "main"],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            print("  Push realizado com sucesso!")
        else:
            print(f"  [AVISO] Erro no push: {result.stderr}")
            print("  Tente manualmente: git push origin main")

    except subprocess.CalledProcessError as e:
        print(f"  [ERRO] Git falhou: {e}")
        print("  Execute manualmente:")
        print("    git add .")
        print('    git commit -m "feat: ESTBAN municipal"')
        print("    git push origin main")
    except FileNotFoundError:
        print("  [ERRO] Git não encontrado no PATH.")


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline ESTBAN Municipal - Dados Estratégicos BCB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python pipeline_estban.py
  python pipeline_estban.py --no-push
  python pipeline_estban.py --inicio 2024-01 --fim 2025-09
  python pipeline_estban.py --inicio 2023-01 --fim 2025-09 --no-push
        """,
    )
    parser.add_argument(
        "--inicio",
        type=str,
        default=PERIODO_INICIO_DEFAULT,
        help=f"Período inicial YYYY-MM (padrão: {PERIODO_INICIO_DEFAULT})",
    )
    parser.add_argument(
        "--fim",
        type=str,
        default=PERIODO_FIM_DEFAULT,
        help=f"Período final YYYY-MM (padrão: {PERIODO_FIM_DEFAULT})",
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="Não fazer git push automático",
    )

    args = parser.parse_args()

    # Validação
    padrao = re.compile(r"^\d{4}-\d{2}$")
    if not padrao.match(args.inicio):
        print(f"[ERRO] Formato inválido para --inicio: {args.inicio} (use YYYY-MM)")
        sys.exit(1)
    if not padrao.match(args.fim):
        print(f"[ERRO] Formato inválido para --fim: {args.fim} (use YYYY-MM)")
        sys.exit(1)

    executar_pipeline(
        inicio=args.inicio,
        fim=args.fim,
        fazer_push=not args.no_push,
    )


if __name__ == "__main__":
    main()