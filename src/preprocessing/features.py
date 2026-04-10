"""
Engenharia de features para detecção de anomalias eleitorais.

Produz um DataFrame com variáveis numéricas derivadas do dataset mestre,
prontas para os algoritmos de detecção de anomalias.

Uso:
    from src.preprocessing.features import build_features
    df_feat = build_features(ano=2024)
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
FEATURES_DIR  = Path("data/processed/features")


# ── HELPERS ──────────────────────────────────────────────────────────────────

def _zscore_grupo(series: pd.Series, grupo: pd.Series) -> pd.Series:
    """Z-score de `series` calculado dentro de cada grupo."""
    mean = series.groupby(grupo).transform("mean")
    std  = series.groupby(grupo).transform("std").replace(0, np.nan)
    return (series - mean) / std


def _log1p_safe(series: pd.Series) -> pd.Series:
    """log1p com proteção contra negativos."""
    return np.log1p(series.clip(lower=0))


# ── FEATURES DE VOTOS ────────────────────────────────────────────────────────

def _feat_votos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Features derivadas da votação do candidato.

    - VOTOS_LOG               : log1p dos votos totais
    - RAZAO_VOTOS_MUN         : votos / total de votos do cargo no município
    - VOTOS_POR_ZONA          : votos / n_zonas (média por zona)
    - ZSCORE_VOTOS_MUN        : z-score dos votos dentro de município+cargo
    - FLAG_ZERO_VOTOS         : 1 se candidato participou mas teve 0 votos
    """
    chave = ["CD_MUNICIPIO", "CD_CARGO"]

    total_mun = df.groupby(chave)["TOTAL_VOTOS"].transform("sum")

    df["VOTOS_LOG"]       = _log1p_safe(df["TOTAL_VOTOS"])
    df["RAZAO_VOTOS_MUN"] = (df["TOTAL_VOTOS"] / total_mun.replace(0, np.nan)).fillna(0)
    df["VOTOS_POR_ZONA"]  = (
        df["TOTAL_VOTOS"] / df["N_ZONAS"].replace(0, np.nan)
    ).fillna(0)
    df["ZSCORE_VOTOS_MUN"] = _zscore_grupo(
        df["TOTAL_VOTOS"],
        df["CD_MUNICIPIO"].astype(str) + "_" + df["CD_CARGO"].astype(str),
    ).fillna(0)

    # Candidatos que participaram mas não tiveram votos computados
    # (excluímos quem teve candidatura cancelada — N_ZONAS == 0)
    df["FLAG_ZERO_VOTOS"] = (
        (df["TOTAL_VOTOS"] == 0) & (df["N_ZONAS"] > 0)
    ).astype("Int8")

    return df


# ── FEATURES DE PATRIMÔNIO ───────────────────────────────────────────────────

def _feat_patrimonio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Features derivadas do patrimônio declarado.

    - PATRIMONIO_LOG          : log1p do patrimônio total
    - PATRIMONIO_VS_MEDIANA   : patrimônio / mediana do cargo no estado
    - ZSCORE_PAT_CARGO_UF     : z-score do patrimônio dentro de cargo+UF
    - FLAG_PATRIMONIO_ZERO    : 1 se candidato não declarou nenhum bem
    - FLAG_PATRIMONIO_ALTO    : 1 se patrimônio > percentil 99 do cargo
    """
    chave_uf_cargo = df["SG_UF"] + "_" + df["CD_CARGO"].astype(str)

    mediana_uf_cargo = df.groupby(["SG_UF", "CD_CARGO"])["PATRIMONIO_TOTAL"].transform("median")
    p99_cargo        = df.groupby("CD_CARGO")["PATRIMONIO_TOTAL"].transform(lambda x: x.quantile(0.99))

    df["PATRIMONIO_LOG"]        = _log1p_safe(df["PATRIMONIO_TOTAL"])
    df["PATRIMONIO_VS_MEDIANA"] = (
        df["PATRIMONIO_TOTAL"] / mediana_uf_cargo.replace(0, np.nan)
    ).fillna(0).clip(upper=1e4)  # limita outliers extremos para não distorcer o modelo
    df["ZSCORE_PAT_CARGO_UF"]  = _zscore_grupo(
        df["PATRIMONIO_TOTAL"], chave_uf_cargo
    ).fillna(0)

    df["FLAG_PATRIMONIO_ZERO"] = (df["PATRIMONIO_TOTAL"] == 0).astype("Int8")
    df["FLAG_PATRIMONIO_ALTO"] = (df["PATRIMONIO_TOTAL"] > p99_cargo).astype("Int8")

    return df


# ── FEATURES DE COMPETIÇÃO ───────────────────────────────────────────────────

def _feat_competicao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Features que capturam o contexto competitivo do candidato.

    - CANDIDATOS_POR_VAGA     : n_candidatos / n_eleitos no município+cargo
                                (quanto maior, mais disputado o pleito)
    - RAZAO_PARTIDO_ELEITOS   : eleitos do partido / candidatos do partido no município
    - ZSCORE_PAT_VS_CARGO_MUN : z-score do patrimônio dentro de município+cargo
    """
    chave = ["CD_MUNICIPIO", "CD_CARGO"]

    n_cand  = df.groupby(chave)["SQ_CANDIDATO"].transform("count")
    n_el    = df.groupby(chave)["ELEITO"].transform("sum").replace(0, np.nan)

    df["CANDIDATOS_POR_VAGA"] = (n_cand / n_el).fillna(0).clip(upper=500)

    chave_part = ["CD_MUNICIPIO", "CD_CARGO", "SG_PARTIDO"]
    n_cand_p   = df.groupby(chave_part)["SQ_CANDIDATO"].transform("count")
    n_el_p     = df.groupby(chave_part)["ELEITO"].transform("sum")
    df["RAZAO_PARTIDO_ELEITOS"] = (n_el_p / n_cand_p).fillna(0)

    chave_mun_cargo = df["CD_MUNICIPIO"].astype(str) + "_" + df["CD_CARGO"].astype(str)
    df["ZSCORE_PAT_VS_CARGO_MUN"] = _zscore_grupo(
        df["PATRIMONIO_TOTAL"], chave_mun_cargo
    ).fillna(0)

    return df


# ── FEATURES DE PERFIL ───────────────────────────────────────────────────────

def _feat_perfil(df: pd.DataFrame) -> pd.DataFrame:
    """
    Features categóricas convertidas para numéricas.

    - GENERO_BIN              : 1 = Masculino, 0 = Feminino/Outro
    - REELEICAO               : 1 se ST_REELEICAO == 'S' (quando disponível)
    """
    df["GENERO_BIN"] = (df["GENERO"] == "M").astype("Int8")

    if "ST_REELEICAO" in df.columns:
        df["REELEICAO"] = (df["ST_REELEICAO"] == "S").astype("Int8")
    else:
        df["REELEICAO"] = 0

    return df


# ── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

# Features que entram nos modelos de anomalia
FEATURES_MODELO = [
    # Votos
    "VOTOS_LOG",
    "RAZAO_VOTOS_MUN",
    "VOTOS_POR_ZONA",
    "ZSCORE_VOTOS_MUN",
    # Patrimônio
    "PATRIMONIO_LOG",
    "PATRIMONIO_VS_MEDIANA",
    "ZSCORE_PAT_CARGO_UF",
    "ZSCORE_PAT_VS_CARGO_MUN",
    # Competição
    "CANDIDATOS_POR_VAGA",
    "RAZAO_PARTIDO_ELEITOS",
    # Perfil
    "GRAU_INSTRUCAO_ORD",
    "GENERO_BIN",
    "N_BENS",
]

# Colunas de identificação mantidas no output (não entram nos modelos)
COLUNAS_ID = [
    "SQ_CANDIDATO", "ANO_ELEICAO", "NM_CANDIDATO", "DS_CARGO",
    "SG_UF", "CD_MUNICIPIO", "NM_MUNICIPIO", "SG_PARTIDO",
    "RESULTADO_SIMPLIFICADO", "ELEITO", "TOTAL_VOTOS", "PATRIMONIO_TOTAL",
    "FLAG_ZERO_VOTOS", "FLAG_PATRIMONIO_ZERO", "FLAG_PATRIMONIO_ALTO",
]


def build_features(ano: int, apenas_com_votos: bool = True) -> pd.DataFrame:
    """
    Constrói o dataset de features a partir do dataset mestre.

    Args:
        ano:              ano eleitoral
        apenas_com_votos: se True, remove candidatos com candidatura cancelada
                          (N_ZONAS == 0) — reduz ruído nos modelos

    Returns:
        DataFrame com colunas de ID + features para o modelo
    """
    path = PROCESSED_DIR / "master" / f"master_{ano}.parquet"
    log.info("[features/%d] Carregando %s", ano, path)
    df = pd.read_parquet(path)

    if apenas_com_votos:
        antes = len(df)
        df = df[df["N_ZONAS"] > 0].copy()
        log.info("[features/%d] Filtro N_ZONAS > 0: %d → %d linhas", ano, antes, len(df))

    # Garantir tipos corretos antes de calcular
    df["TOTAL_VOTOS"]      = df["TOTAL_VOTOS"].astype(float)
    df["PATRIMONIO_TOTAL"] = df["PATRIMONIO_TOTAL"].astype(float)
    df["N_BENS"]           = df["N_BENS"].astype(float)
    df["GRAU_INSTRUCAO_ORD"] = df["GRAU_INSTRUCAO_ORD"].astype(float)
    df["N_ZONAS"]          = df["N_ZONAS"].astype(float)

    log.info("[features/%d] Calculando features de votos...", ano)
    df = _feat_votos(df)

    log.info("[features/%d] Calculando features de patrimônio...", ano)
    df = _feat_patrimonio(df)

    log.info("[features/%d] Calculando features de competição...", ano)
    df = _feat_competicao(df)

    log.info("[features/%d] Calculando features de perfil...", ano)
    df = _feat_perfil(df)

    # Selecionar apenas colunas relevantes
    colunas_out = [c for c in COLUNAS_ID if c in df.columns] + FEATURES_MODELO
    df_out = df[colunas_out].copy()

    # Preencher NaN residuais com 0 nas features do modelo
    df_out[FEATURES_MODELO] = df_out[FEATURES_MODELO].fillna(0)

    # Salvar
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    saida = FEATURES_DIR / f"features_{ano}.parquet"
    df_out.to_parquet(saida, index=False, engine="pyarrow")
    log.info("[features/%d] Salvo: %s (%.1f MB) | %d linhas | %d features",
             ano, saida, saida.stat().st_size / 1e6, len(df_out), len(FEATURES_MODELO))

    return df_out
