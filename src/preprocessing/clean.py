"""
Módulo de pré-processamento dos microdados eleitorais do TSE.

Etapas por dataset:
    candidatos     → limpa nulos mascarados, normaliza categorias, converte tipos
    votacao_munzona→ converte votos para int, agrega por candidato (soma de zonas)
    bens_candidatos→ converte valor monetário, agrega por candidato (patrimônio total)

Etapa final:
    build_master   → join dos 3 datasets em um único DataFrame analítico
"""

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
MASTER_DIR    = Path("data/processed/master")

# Valores que o TSE usa para indicar "não informado" ou "não aplicável"
VALORES_NULOS_TSE = {"#NE", "#NULO", "#NE#", "NÃO DIVULGÁVEL", "NAO DIVULGAVEL", "-1", ""}


# ── CANDIDATOS ───────────────────────────────────────────────────────────────

def _limpar_nulos_tse(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui valores mascarados do TSE por NaN em colunas de texto."""
    return df.replace(VALORES_NULOS_TSE, pd.NA)


def _normalizar_resultado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria coluna binária ELEITO e agrupamento simplificado de resultado.

    DS_SIT_TOT_TURNO original → RESULTADO_SIMPLIFICADO
        ELEITO / ELEITO POR QP / ELEITO POR MÉDIA  → Eleito
        NÃO ELEITO / SUPLENTE                       → Não Eleito
        2º TURNO                                    → 2o Turno
        demais                                      → Indefinido
    """
    mapa = {
        "ELEITO":              "Eleito",
        "ELEITO POR QP":       "Eleito",
        "ELEITO POR MÉDIA":    "Eleito",
        "NÃO ELEITO":          "Não Eleito",
        "SUPLENTE":            "Não Eleito",
        "2º TURNO":            "2o Turno",
    }
    df["RESULTADO_SIMPLIFICADO"] = df["DS_SIT_TOT_TURNO"].map(mapa).fillna("Indefinido")
    df["ELEITO"] = (df["RESULTADO_SIMPLIFICADO"] == "Eleito").astype("Int8")
    return df


def _normalizar_grau_instrucao(df: pd.DataFrame) -> pd.DataFrame:
    """Converte grau de instrução em valor ordinal para uso em modelos."""
    ordem = {
        "ANALFABETO":                   0,
        "LÊ E ESCREVE":                 1,
        "ENSINO FUNDAMENTAL INCOMPLETO":2,
        "ENSINO FUNDAMENTAL COMPLETO":  3,
        "ENSINO MÉDIO INCOMPLETO":      4,
        "ENSINO MÉDIO COMPLETO":        5,
        "SUPERIOR INCOMPLETO":          6,
        "SUPERIOR COMPLETO":            7,
        "PÓS-GRADUAÇÃO":                8,
    }
    df["GRAU_INSTRUCAO_ORD"] = df["DS_GRAU_INSTRUCAO"].map(ordem).astype("Int8")
    return df


def preprocessar_candidatos(ano: int) -> pd.DataFrame:
    """
    Pipeline de limpeza do dataset de candidatos.

    Transformações:
    - Remove valores nulos mascarados (NÃO DIVULGÁVEL, #NE, etc.)
    - Converte ANO_ELEICAO e CD_CARGO para inteiro
    - Cria RESULTADO_SIMPLIFICADO e ELEITO
    - Cria GRAU_INSTRUCAO_ORD (ordinal)
    - Padroniza DS_GENERO para M/F/Outro
    """
    path = PROCESSED_DIR / "candidatos" / f"candidatos_{ano}.parquet"
    log.info("[candidatos/%d] Carregando %s", ano, path)
    df = pd.read_parquet(path)

    df = _limpar_nulos_tse(df)

    df["ANO_ELEICAO"] = df["ANO_ELEICAO"].astype(int)
    df["CD_CARGO"]    = df["CD_CARGO"].astype(int)

    df = _normalizar_resultado(df)
    df = _normalizar_grau_instrucao(df)

    # Gênero simplificado
    mapa_genero = {"MASCULINO": "M", "FEMININO": "F"}
    df["GENERO"] = df["DS_GENERO"].map(mapa_genero).fillna("Outro")

    # Deduplicação: candidatos do 2º turno aparecem com 2 registros (um por turno).
    # Mantém apenas o resultado final — prioridade: Eleito > Não Eleito > 2o Turno > demais.
    prioridade = {"Eleito": 0, "Não Eleito": 1, "2o Turno": 2, "Indefinido": 3}
    antes = len(df)
    df["_prio"] = df["RESULTADO_SIMPLIFICADO"].map(prioridade).fillna(9)
    df = (df.sort_values("_prio")
            .drop_duplicates(subset="SQ_CANDIDATO", keep="first")
            .drop(columns="_prio")
            .reset_index(drop=True))
    removidos = antes - len(df)
    if removidos:
        log.info("[candidatos/%d] Deduplicação 2º turno: %d duplicatas removidas", ano, removidos)

    log.info("[candidatos/%d] %d linhas | nulos por coluna-chave:", ano, len(df))
    for col in ["DS_SIT_TOT_TURNO", "DS_GENERO", "DS_GRAU_INSTRUCAO"]:
        if col in df.columns:
            n = df[col].isna().sum()
            log.info("  %-35s %d nulos", col, n)

    return df


# ── VOTAÇÃO ──────────────────────────────────────────────────────────────────

def preprocessar_votacao(ano: int) -> pd.DataFrame:
    """
    Pipeline de limpeza e agregação do dataset de votação.

    Transformações:
    - Converte QT_VOTOS_NOMINAIS e QT_VOTOS_NOMINAIS_VALIDOS para int
    - Agrega por SQ_CANDIDATO: soma de votos em todas as zonas
    - Mantém colunas de identificação (UF, município, cargo, partido)
    - Calcula MUNICIPIOS_COM_VOTOS por candidato (detecta candidatos multi-zona)
    """
    path = PROCESSED_DIR / "votacao_munzona" / f"votacao_munzona_{ano}.parquet"
    log.info("[votacao/%d] Carregando %s", ano, path)
    df = pd.read_parquet(path)

    df["QT_VOTOS_NOMINAIS"]        = pd.to_numeric(df["QT_VOTOS_NOMINAIS"], errors="coerce").fillna(0).astype(int)
    df["QT_VOTOS_NOMINAIS_VALIDOS"] = pd.to_numeric(df["QT_VOTOS_NOMINAIS_VALIDOS"], errors="coerce").fillna(0).astype(int)

    # Agrega zonas → um registro por candidato
    agg = (
        df.groupby("SQ_CANDIDATO", as_index=False)
        .agg(
            TOTAL_VOTOS        =("QT_VOTOS_NOMINAIS",        "sum"),
            TOTAL_VOTOS_VALIDOS=("QT_VOTOS_NOMINAIS_VALIDOS","sum"),
            N_ZONAS            =("NR_ZONA",                  "nunique"),
            N_MUNICIPIOS       =("CD_MUNICIPIO",              "nunique"),
            # pega o primeiro valor de colunas de identificação (são iguais dentro do candidato)
            CD_MUNICIPIO       =("CD_MUNICIPIO",             "first"),
            NM_MUNICIPIO       =("NM_MUNICIPIO",             "first"),
            SG_UF              =("SG_UF",                    "first"),
            CD_CARGO           =("CD_CARGO",                 "first"),
            DS_CARGO           =("DS_CARGO",                 "first"),
            SG_PARTIDO         =("SG_PARTIDO",               "first"),
            NM_PARTIDO         =("NM_PARTIDO",               "first"),
            ANO_ELEICAO        =("ANO_ELEICAO",              "first"),
        )
    )

    log.info("[votacao/%d] %d candidatos após agregação por zona", ano, len(agg))
    log.info("  votos totais: %d", agg["TOTAL_VOTOS"].sum())
    log.info("  votos máximos (1 candidato): %d", agg["TOTAL_VOTOS"].max())
    return agg


# ── BENS ─────────────────────────────────────────────────────────────────────

def preprocessar_bens(ano: int) -> pd.DataFrame:
    """
    Pipeline de limpeza e agregação do dataset de bens declarados.

    Transformações:
    - Converte VR_BEM_CANDIDATO de string BR (vírgula decimal) para float
    - Agrega por SQ_CANDIDATO: patrimônio total, número de bens, maior bem
    """
    path = PROCESSED_DIR / "bens_candidatos" / f"bens_candidatos_{ano}.parquet"
    log.info("[bens/%d] Carregando %s", ano, path)
    df = pd.read_parquet(path)

    df["VR_BEM_CANDIDATO"] = (
        df["VR_BEM_CANDIDATO"]
        .str.replace(".", "", regex=False)   # remove separador de milhar
        .str.replace(",", ".", regex=False)  # vírgula → ponto decimal
    )
    df["VR_BEM_CANDIDATO"] = pd.to_numeric(df["VR_BEM_CANDIDATO"], errors="coerce").fillna(0.0)

    agg = (
        df.groupby("SQ_CANDIDATO", as_index=False)
        .agg(
            PATRIMONIO_TOTAL =("VR_BEM_CANDIDATO", "sum"),
            N_BENS           =("VR_BEM_CANDIDATO", "count"),
            MAIOR_BEM        =("VR_BEM_CANDIDATO", "max"),
        )
    )

    log.info("[bens/%d] %d candidatos com bens declarados", ano, len(agg))
    log.info("  patrimônio total: R$ %s", f"{agg['PATRIMONIO_TOTAL'].sum():,.2f}")
    log.info("  maior patrimônio individual: R$ %s", f"{agg['PATRIMONIO_TOTAL'].max():,.2f}")
    return agg


# ── MASTER ───────────────────────────────────────────────────────────────────

def build_master(ano: int) -> pd.DataFrame:
    """
    Constrói o dataset analítico unificado (um registro por candidato).

    Joins:
        candidatos  ← LEFT JOIN votacao_agg  ON SQ_CANDIDATO
        resultado   ← LEFT JOIN bens_agg     ON SQ_CANDIDATO

    Colunas adicionadas:
        TOTAL_VOTOS, TOTAL_VOTOS_VALIDOS, N_ZONAS, N_MUNICIPIOS (da votação)
        PATRIMONIO_TOTAL, N_BENS, MAIOR_BEM                    (dos bens)
        CD_MUNICIPIO, NM_MUNICIPIO                             (da votação, mais preciso)
    """
    log.info("══ BUILD MASTER %d ══", ano)

    cand  = preprocessar_candidatos(ano)
    votos = preprocessar_votacao(ano)
    bens  = preprocessar_bens(ano)

    # candidatos é a base — left join garante que todos os candidatos ficam
    master = cand.merge(
        votos[["SQ_CANDIDATO","CD_MUNICIPIO","NM_MUNICIPIO",
               "TOTAL_VOTOS","TOTAL_VOTOS_VALIDOS","N_ZONAS","N_MUNICIPIOS"]],
        on="SQ_CANDIDATO",
        how="left",
    )

    master = master.merge(
        bens[["SQ_CANDIDATO","PATRIMONIO_TOTAL","N_BENS","MAIOR_BEM"]],
        on="SQ_CANDIDATO",
        how="left",
    )

    # Candidatos sem votos registrados → preenche com 0
    for col in ["TOTAL_VOTOS","TOTAL_VOTOS_VALIDOS","N_ZONAS","N_MUNICIPIOS"]:
        master[col] = master[col].fillna(0).astype(int)

    # Candidatos sem bens declarados → preenche com 0
    for col in ["PATRIMONIO_TOTAL","MAIOR_BEM"]:
        master[col] = master[col].fillna(0.0)
    master["N_BENS"] = master["N_BENS"].fillna(0).astype(int)

    log.info("Master %d: %d linhas | %d colunas", ano, len(master), len(master.columns))

    # Salvar
    MASTER_DIR.mkdir(parents=True, exist_ok=True)
    saida = MASTER_DIR / f"master_{ano}.parquet"
    master.to_parquet(saida, index=False, engine="pyarrow")
    log.info("Salvo: %s (%.1f MB)", saida, saida.stat().st_size / 1e6)

    return master
