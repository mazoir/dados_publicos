#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
=============================================================================
  BCB - Cooperados por Cooperativa
  Pipeline Codespace: Download → Consolidação → Commit → Push
=============================================================================

  Executa dentro do GitHub Codespace do repo "dados_publicos".
  Baixa os dados do BCB, consolida em CSV único e faz push automático.

  Transformações:
    - Remove 6 primeiras linhas de cada CSV (metadados BCB)
    - Cabeçalho único no consolidado
    - CNPJ com zeros à esquerda (8 dígitos)
    - Coluna "Periodo" no formato DD/MM/AAAA (dia fixo = 01)

  Uso:
    pip install requests pandas
    python pipeline_cooperados.py

  Autor: Mazoir / assistido por Claude
  Data: 2026-02-05
=============================================================================
"""

import os
import re
import sys
import time
import zipfile
import logging
import subprocess
import shutil
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

# Detecta raiz do repo automaticamente (Codespace monta em /workspaces/)
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", "/workspaces/dados_publicos"))

# Estrutura de pastas dentro do repo
PASTA_DADOS    = REPO_ROOT / "dados" / "bcb" / "cooperados"
PASTA_BRUTOS   = PASTA_DADOS / "_brutos"
PASTA_TEMP     = PASTA_DADOS / "_temp"

# Arquivo consolidado final (este será versionado no Git)
ARQUIVO_FINAL  = PASTA_DADOS / "cooperados_por_cooperativa.csv"

# Período de coleta
ANO_INICIO, MES_INICIO = 2020, 1
ANO_FIM, MES_FIM = 2025, 12

# Linhas de metadados a pular no início de cada CSV bruto
LINHAS_PULAR = 6

# Configurações HTTP
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_DELAY = 3
PAUSA_ENTRE_DOWNLOADS = 0.5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://www.bcb.gov.br/estabilidadefinanceira/cooperados_cooperativa",
}

# BCB
BCB_BASE = "https://www.bcb.gov.br"
BCB_API  = f"{BCB_BASE}/api/servico/sitebcb/cooperadoscooperativa"
URL_NOVO   = f"{BCB_BASE}/content/estabilidadefinanceira/divulgacaoCCO/cont2/{{yyyymm}}CCOCOOPERATIVA.zip"
URL_ANTIGO = f"{BCB_BASE}/content/estabilidadefinanceira/divulgacaoCCO/cont2/{{yyyymm}}CCOCooperativa.zip"
CORTE_PATTERN = 201904

# Branch
BRANCH = "main"

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("BCB")


# ============================================================================
# UTILITÁRIOS
# ============================================================================

def gerar_periodos() -> list[str]:
    """Gera lista YYYYMM no intervalo configurado."""
    periodos = []
    ano, mes = ANO_INICIO, MES_INICIO
    while (ano < ANO_FIM) or (ano == ANO_FIM and mes <= MES_FIM):
        periodos.append(f"{ano:04d}{mes:02d}")
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return periodos


def criar_estrutura():
    """Cria estrutura de pastas."""
    for pasta in [PASTA_DADOS, PASTA_BRUTOS, PASTA_TEMP]:
        pasta.mkdir(parents=True, exist_ok=True)
    log.info(f"Estrutura criada em: {PASTA_DADOS}")


def run_git(*args) -> subprocess.CompletedProcess:
    """Executa comando git no repo."""
    cmd = ["git"] + list(args)
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)


# ============================================================================
# ETAPA 1: DOWNLOAD
# ============================================================================

def obter_urls_api(session: requests.Session) -> dict[str, str]:
    """Consulta API do BCB para URLs reais."""
    log.info("Consultando API do BCB...")
    urls = {}
    try:
        resp = session.get(BCB_API, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            for item in resp.json().get("conteudo", []):
                url_rel = item.get("Url", "")
                titulo = item.get("Titulo", "")
                if url_rel:
                    periodo = titulo.replace("/", "") if titulo else ""
                    if not periodo:
                        m = re.search(r"(\d{6})", item.get("Nome", ""))
                        if m:
                            periodo = m.group(1)
                    if periodo:
                        urls[periodo] = BCB_BASE + url_rel
            log.info(f"✓ API: {len(urls)} arquivos disponíveis")
        else:
            log.warning(f"API HTTP {resp.status_code}")
    except Exception as e:
        log.warning(f"Erro API: {e}")
    return urls


def url_fallback(periodo: str) -> str:
    """URL direta (fallback se API falhar)."""
    pattern = URL_NOVO if int(periodo) >= CORTE_PATTERN else URL_ANTIGO
    return pattern.replace("{yyyymm}", periodo)


def download_zip(session: requests.Session, url: str, periodo: str) -> Optional[Path]:
    """Baixa ZIP com retry."""
    destino = PASTA_TEMP / f"{periodo}.zip"

    if destino.exists() and destino.stat().st_size > 500:
        return destino

    for t in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
            if resp.status_code == 200:
                total = 0
                with open(destino, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk)
                            total += len(chunk)
                if total > 500:
                    log.info(f"  ✓ {periodo} │ {total:>8,} bytes")
                    return destino
                destino.unlink(missing_ok=True)
            elif resp.status_code == 404:
                log.warning(f"  ✗ {periodo} │ 404")
                return None
            else:
                log.warning(f"  ✗ {periodo} │ HTTP {resp.status_code} (tentativa {t})")
        except Exception as e:
            log.warning(f"  ✗ {periodo} │ {e} (tentativa {t})")
        if t < MAX_RETRIES:
            time.sleep(RETRY_DELAY * t)
    return None


def extrair_csv(zip_path: Path, periodo: str) -> Optional[Path]:
    """Extrai CSV do ZIP."""
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for nome in zf.namelist():
                if nome.lower().endswith(".csv"):
                    dest = PASTA_BRUTOS / f"{periodo}.csv"
                    with zf.open(nome) as src, open(dest, "wb") as tgt:
                        tgt.write(src.read())
                    return dest
    except zipfile.BadZipFile:
        log.error(f"  ✗ ZIP corrompido: {periodo}")
    return None


# ============================================================================
# ETAPA 2: CONSOLIDAÇÃO
# ============================================================================

def consolidar() -> bool:
    """
    Consolida CSVs individuais em arquivo único.

    Regras:
      - Pula 6 primeiras linhas (metadados BCB)
      - Cabeçalho único
      - CNPJ: zfill(8)
      - Coluna Periodo: DD/MM/AAAA
    """
    log.info("")
    log.info("═" * 60)
    log.info("CONSOLIDAÇÃO")
    log.info("═" * 60)

    csvs = sorted(PASTA_BRUTOS.glob("*.csv"))
    if not csvs:
        log.error("Nenhum CSV encontrado!")
        return False

    log.info(f"Arquivos: {len(csvs)}")

    dfs = []
    col_cnpj = None

    for csv_path in csvs:
        periodo = csv_path.stem  # "202001", "202002", ...
        ano = periodo[:4]
        mes = periodo[4:6]
        periodo_fmt = f"01/{mes}/{ano}"

        try:
            df = pd.read_csv(
                csv_path,
                skiprows=LINHAS_PULAR,
                sep=";",
                encoding="latin-1",
                dtype=str,
                on_bad_lines="warn",
                keep_default_na=False,
            )

            if df.empty:
                log.warning(f"  ⚠ {periodo} vazio")
                continue

            # Remove rodapé BCB (se existir)
            val_ultima = str(list(df.iloc[-1])[0]).lower()
            if "fonte" in val_ultima or "banco central" in val_ultima:
                df = df.iloc[:-1]

            # Detecta coluna CNPJ (primeira vez)
            if col_cnpj is None:
                for c in df.columns:
                    if "cnpj" in c.strip().lower():
                        col_cnpj = c
                        break
                if col_cnpj:
                    log.info(f"  Coluna CNPJ: '{col_cnpj}'")
                    log.info(f"  Colunas: {list(df.columns)}")

            # CNPJ: zeros à esquerda (8 dígitos)
            if col_cnpj and col_cnpj in df.columns:
                df[col_cnpj] = (
                    df[col_cnpj].astype(str).str.strip()
                    .str.replace(r"\D", "", regex=True)
                    .str.zfill(8)
                )

            # Coluna Periodo
            df["Periodo"] = periodo_fmt

            dfs.append(df)
            log.info(f"  ✓ {periodo} │ {len(df):>6,} linhas │ {periodo_fmt}")

        except Exception as e:
            log.error(f"  ✗ {periodo} │ {e}")

    if not dfs:
        log.error("Nenhum DataFrame gerado!")
        return False

    # Concatena
    log.info(f"\nConcatenando {len(dfs)} períodos...")
    df_final = pd.concat(dfs, ignore_index=True)

    # ── Transformações pós-concatenação ──────────────────────────

    # 1. Remove coluna Nome
    if "Nome" in df_final.columns:
        df_final.drop(columns=["Nome"], inplace=True)
        log.info("  ✓ Coluna 'Nome' removida")

    # 2. Colunas numéricas → inteiro
    colunas_int = [
        "Total de Cooperados",
        "Cooperados PF",
        "Cooperados PJ",
        "Sexo Feminino",
        "Sexo Masculino",
        "Sexo nao Informado",
    ]
    for col in colunas_int:
        if col in df_final.columns:
            df_final[col] = (
                pd.to_numeric(df_final[col], errors="coerce")
                .fillna(0)
                .astype(int)
            )
    log.info(f"  ✓ Colunas numéricas convertidas para inteiro: {colunas_int}")

    # 3. Periodo → data (YYYY-MM-DD no CSV, o Power BI reconhece direto)
    if "Periodo" in df_final.columns:
        df_final["Periodo"] = pd.to_datetime(
            df_final["Periodo"], format="%d/%m/%Y"
        ).dt.strftime("%Y-%m-%d")
        log.info("  ✓ Coluna 'Periodo' convertida para data (YYYY-MM-DD)")

    # Salva
    log.info(f"Salvando: {ARQUIVO_FINAL}")
    df_final.to_csv(ARQUIVO_FINAL, index=False, sep=";", encoding="utf-8")

    tam_mb = ARQUIVO_FINAL.stat().st_size / (1024 * 1024)

    log.info("")
    log.info("─" * 60)
    log.info(f"  Arquivo:    {ARQUIVO_FINAL.name}")
    log.info(f"  Tamanho:    {tam_mb:.2f} MB")
    log.info(f"  Linhas:     {len(df_final):,}")
    log.info(f"  Colunas:    {list(df_final.columns)}")
    periodos = sorted(df_final["Periodo"].unique(), key=lambda x: x[6:10] + x[3:5])
    log.info(f"  Períodos:   {len(periodos)} ({periodos[0]} → {periodos[-1]})")
    log.info("─" * 60)

    # Amostra
    log.info("\nAmostra (3 primeiras linhas):")
    print(df_final.head(3).to_string(index=False))

    return True


# ============================================================================
# ETAPA 3: GIT COMMIT + PUSH
# ============================================================================

def criar_gitignore():
    """Cria .gitignore para não versionar temporários."""
    gitignore = PASTA_DADOS / ".gitignore"
    gitignore.write_text(
        "# Arquivos temporários do pipeline\n"
        "_brutos/\n"
        "_temp/\n"
    )


def git_push():
    """Commit e push do arquivo consolidado."""
    log.info("")
    log.info("═" * 60)
    log.info("GIT: COMMIT + PUSH")
    log.info("═" * 60)

    criar_gitignore()

    # Configura git (Codespace geralmente já tem, mas por segurança)
    run_git("config", "user.name", "Mazoir")
    run_git("config", "user.email", "mazoir@users.noreply.github.com")

    # Add apenas os arquivos relevantes
    run_git("add", str(ARQUIVO_FINAL))
    run_git("add", str(PASTA_DADOS / ".gitignore"))
    run_git("add", str(REPO_ROOT / "README.md"))

    # Verifica se há mudanças
    result = run_git("status", "--porcelain")
    if not result.stdout.strip():
        log.info("Nenhuma alteração. Dados já estão atualizados no repo.")
        return True

    # Commit
    agora = datetime.now().strftime("%Y-%m-%d %H:%M")
    msg = f"📊 Atualização cooperados BCB - {agora}"
    run_git("commit", "-m", msg)
    log.info(f"  ✓ Commit: {msg}")

    # Push
    result = run_git("push", "origin", BRANCH)
    if result.returncode == 0:
        log.info("  ✓ Push realizado com sucesso!")
        return True
    else:
        log.error(f"  ✗ Erro no push: {result.stderr}")
        return False


def atualizar_readme():
    """Cria/atualiza README.md com instruções de uso."""
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    # Pega o nome do repo da URL de origin
    result = run_git("remote", "get-url", "origin")
    origin_url = result.stdout.strip()

    # Extrai usuario/repo
    match = re.search(r"github\.com[:/](.+?)(?:\.git)?$", origin_url)
    repo_path = match.group(1) if match else "SEU_USUARIO/dados_publicos"

    readme = REPO_ROOT / "README.md"
    conteudo = f"""# 📊 Dados Públicos - BCB

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
| `CNPJ` | Texto, padronizado com zeros à esquerda (8 dígitos) |
| `Total de Cooperados` | Inteiro |
| `Cooperados PF` | Inteiro |
| `Cooperados PJ` | Inteiro |
| `Sexo Feminino` | Inteiro |
| `Sexo Masculino` | Inteiro |
| `Sexo nao Informado` | Inteiro |
| `Periodo` | Data (YYYY-MM-DD, dia fixo = 01) |

> Coluna `Nome` removida para otimizar tamanho.

## 🔌 Uso no Power BI

**Obter Dados → Consulta em Branco → Editor Avançado:**

```powerquery
let
    Url = "https://raw.githubusercontent.com/{repo_path}/main/dados/bcb/cooperados/cooperados_por_cooperativa.csv",
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
{agora}

---
*Gerado automaticamente pelo pipeline BCB.*
"""
    readme.write_text(conteudo, encoding="utf-8")
    log.info("✓ README.md atualizado")


# ============================================================================
# MAIN
# ============================================================================

def main():
    inicio = time.time()

    print()
    print("╔" + "═" * 68 + "╗")
    print("║  BCB - Cooperados por Cooperativa                                ║")
    print("║  Pipeline: Download → Consolidação → Git Push                    ║")
    print(f"║  Período: {ANO_INICIO}/{MES_INICIO:02d} a {ANO_FIM}/{MES_FIM:02d}" + " " * 43 + "║")
    print("╚" + "═" * 68 + "╝")
    print()

    # ── Preparação ────────────────────────────────────────────────
    criar_estrutura()

    # ── Sessão HTTP ───────────────────────────────────────────────
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.urllib3.util.retry.Retry(
            total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
        )
    )
    session.mount("https://", adapter)

    # ── ETAPA 1: Download ─────────────────────────────────────────
    periodos = gerar_periodos()
    urls_api = obter_urls_api(session)

    log.info("")
    log.info("═" * 60)
    log.info(f"DOWNLOAD ({len(periodos)} períodos)")
    log.info("═" * 60)

    sucesso, falha = 0, 0

    for periodo in periodos:
        url = urls_api.get(periodo, url_fallback(periodo))
        zip_path = download_zip(session, url, periodo)

        if zip_path:
            csv_path = extrair_csv(zip_path, periodo)
            if csv_path:
                sucesso += 1
            else:
                falha += 1
        else:
            falha += 1

        time.sleep(PAUSA_ENTRE_DOWNLOADS)

    log.info(f"\nDownload: {sucesso} OK │ {falha} falha(s)")

    # ── ETAPA 2: Consolidação ─────────────────────────────────────
    consolidacao_ok = consolidar()

    # ── ETAPA 3: Git Push ─────────────────────────────────────────
    push_ok = False
    if consolidacao_ok:
        atualizar_readme()
        push_ok = git_push()

    # ── Limpeza ───────────────────────────────────────────────────
    shutil.rmtree(PASTA_BRUTOS, ignore_errors=True)
    shutil.rmtree(PASTA_TEMP, ignore_errors=True)
    log.info("\n✓ Temporários removidos")

    # ── Relatório ─────────────────────────────────────────────────
    duracao = time.time() - inicio

    print()
    print("╔" + "═" * 68 + "╗")
    print("║  RELATÓRIO FINAL" + " " * 51 + "║")
    print("╠" + "═" * 68 + "╣")
    print(f"║  Downloads:       {sucesso:>3} OK │ {falha} falha(s)" + " " * (38 - len(str(falha))) + "║")
    print(f"║  Consolidação:    {'✓ OK' if consolidacao_ok else '✗ FALHOU'}" + " " * (50 if consolidacao_ok else 47) + "║")
    print(f"║  Git Push:        {'✓ OK' if push_ok else '✗ FALHOU'}" + " " * (50 if push_ok else 47) + "║")

    if ARQUIVO_FINAL.exists():
        tam = ARQUIVO_FINAL.stat().st_size / (1024 * 1024)
        print(f"║  Arquivo:         {tam:.1f} MB" + " " * (50 - len(f"{tam:.1f}")) + "║")

    print(f"║  Tempo:           {duracao:.0f}s ({duracao/60:.1f} min)" + " " * (42 - len(f"{duracao:.0f}s ({duracao/60:.1f} min)")) + "║")
    print("╚" + "═" * 68 + "╝")

    if push_ok:
        # Mostra URL raw para o Power BI
        result = run_git("remote", "get-url", "origin")
        origin = result.stdout.strip()
        m = re.search(r"github\.com[:/](.+?)(?:\.git)?$", origin)
        if m:
            repo = m.group(1)
            raw_url = f"https://raw.githubusercontent.com/{repo}/main/dados/bcb/cooperados/cooperados_por_cooperativa.csv"
            print(f"\n  📎 URL raw para Power BI:")
            print(f"  {raw_url}\n")

    return 0 if (consolidacao_ok and push_ok) else 1


if __name__ == "__main__":
    sys.exit(main())