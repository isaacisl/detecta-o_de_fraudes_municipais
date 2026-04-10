"""
Detecção de anomalias eleitorais — Isolation Forest, LOF e Outlier Estatístico.

Algoritmos:
  - Isolation Forest  : isola pontos com poucos cortes aleatórios (rápido, escala bem)
  - LOF               : compara densidade local com a dos vizinhos (mais preciso localmente)
  - Outlier Estatístico: z-score do score combinado IF+LOF (interpretável, sem parâmetros críticos)

Cada algoritmo gera um score normalizado [0,1]. O score consolidado é a média dos três.
Anomalia confirmada = flagged por pelo menos 2 dos 3 algoritmos.

Uso:
    from src.models.anomaly_detection import detectar_anomalias
    df_result = detectar_anomalias(ano=2024)
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler

from src.preprocessing.features import FEATURES_MODELO

log = logging.getLogger(__name__)

FEATURES_DIR = Path("data/processed/features")
RESULTS_DIR  = Path("data/processed/anomalias")


# ── NORMALIZAÇÃO ─────────────────────────────────────────────────────────────

def _normalizar_score(scores: np.ndarray, inverter: bool = True) -> np.ndarray:
    """Normaliza para [0,1] onde 1 = mais anômalo."""
    s = -scores if inverter else scores
    s_min, s_max = s.min(), s.max()
    if s_max == s_min:
        return np.zeros_like(s)
    return (s - s_min) / (s_max - s_min)


# ── ISOLATION FOREST ─────────────────────────────────────────────────────────

def _isolation_forest(X: np.ndarray, contamination: float = 0.05) -> tuple:
    """
    Isolation Forest: isola anomalias com árvores de decisão aleatórias.
    Rápido e escalável — funciona bem em alta dimensão.
    """
    log.info("  → Isolation Forest (contamination=%.2f)...", contamination)
    model = IsolationForest(n_estimators=200, contamination=contamination,
                            random_state=42, n_jobs=-1)
    model.fit(X)
    scores = model.decision_function(X)   # negativo = mais anômalo
    labels = model.predict(X)             # -1 = anomalia
    n_anom = (labels == -1).sum()
    log.info("    %d anomalias (%.1f%%)", n_anom, n_anom / len(X) * 100)
    return scores, labels


# ── LOF ──────────────────────────────────────────────────────────────────────

def _lof(X: np.ndarray, n_neighbors: int = 20, contamination: float = 0.05) -> tuple:
    """
    Local Outlier Factor: detecta pontos com densidade local muito menor
    do que a de seus vizinhos. Mais preciso em regiões densas.
    """
    log.info("  → LOF (n_neighbors=%d, contamination=%.2f)...", n_neighbors, contamination)
    model = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination,
                               novelty=False, n_jobs=-1)
    labels = model.fit_predict(X)
    scores = model.negative_outlier_factor_
    n_anom = (labels == -1).sum()
    log.info("    %d anomalias (%.1f%%)", n_anom, n_anom / len(X) * 100)
    return scores, labels


# ── OUTLIER ESTATÍSTICO ───────────────────────────────────────────────────────

def _stat_outlier(score_if_norm: np.ndarray, score_lof_norm: np.ndarray,
                  threshold_z: float = 2.5) -> tuple:
    """
    Terceiro detector: usa o score combinado IF+LOF e marca como anomalia
    os pontos cujo z-score ultrapassa `threshold_z`.

    Vantagens:
    - Sem parâmetros críticos de distância (eps) que falham em alta dimensão
    - Complementa IF e LOF ao capturar extremos na distribuição conjunta dos scores
    - Interpretável: um z-score alto significa que o candidato é atípico
      segundo AMBOS os algoritmos principais
    """
    score_combinado = (score_if_norm + score_lof_norm) / 2
    media = score_combinado.mean()
    std   = score_combinado.std()

    if std == 0:
        return score_combinado, np.zeros(len(score_combinado), dtype=int)

    z = (score_combinado - media) / std
    labels = np.where(z >= threshold_z, -1, 1)
    n_anom = (labels == -1).sum()
    log.info("  → Outlier Estatístico (z≥%.1f): %d anomalias (%.1f%%)",
             threshold_z, n_anom, n_anom / len(labels) * 100)
    return score_combinado, labels


# ── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

def detectar_anomalias(
    ano: int,
    cargo: str | None = None,
    contamination: float = 0.05,
    stat_z: float = 2.5,
) -> pd.DataFrame:
    """
    Executa os três detectores e consolida os resultados.

    Args:
        ano:           ano eleitoral
        cargo:         'PREFEITO', 'VEREADOR' ou None (processa ambos)
        contamination: proporção de anomalias esperada (IF e LOF)
        stat_z:        threshold z-score para o detector estatístico

    Returns:
        DataFrame com scores, flags individuais e score/flag consolidados
    """
    path = FEATURES_DIR / f"features_{ano}.parquet"
    log.info("[anomalia/%d] Carregando: %s", ano, path)
    df = pd.read_parquet(path)

    resultados = []
    cargos = [cargo] if cargo else df["DS_CARGO"].unique().tolist()

    for c in cargos:
        log.info("\n══ Cargo: %s ══", c)
        sub = df[df["DS_CARGO"] == c].copy()
        X_raw = sub[FEATURES_MODELO].astype(float).values

        scaler = StandardScaler()
        X = scaler.fit_transform(X_raw)

        # ── Algoritmos
        if_scores, if_labels   = _isolation_forest(X, contamination)
        lof_scores, lof_labels = _lof(X, contamination=contamination)

        # Normaliza IF e LOF antes de passar ao detector estatístico
        if_norm  = _normalizar_score(if_scores,  inverter=True)
        lof_norm = _normalizar_score(lof_scores, inverter=True)

        stat_scores, stat_labels = _stat_outlier(if_norm, lof_norm, threshold_z=stat_z)

        # ── Scores normalizados
        sub["SCORE_IF"]   = if_norm
        sub["SCORE_LOF"]  = lof_norm
        sub["SCORE_STAT"] = _normalizar_score(stat_scores, inverter=False)

        # ── Flags individuais (-1 → 1, normal → 0)
        sub["ANOM_IF"]   = (if_labels   == -1).astype("int8")
        sub["ANOM_LOF"]  = (lof_labels  == -1).astype("int8")
        sub["ANOM_STAT"] = (stat_labels == -1).astype("int8")

        # ── Score consolidado (média dos 3)
        sub["SCORE_CONSOLIDADO"] = (sub["SCORE_IF"] + sub["SCORE_LOF"] + sub["SCORE_STAT"]) / 3

        # ── Consenso
        sub["N_ALGORITMOS_ANOM"] = (
            sub["ANOM_IF"].astype(int) +
            sub["ANOM_LOF"].astype(int) +
            sub["ANOM_STAT"].astype(int)
        )

        # ── Confirmada: ≥2 algoritmos concordam
        sub["ANOMALIA_CONFIRMADA"] = (sub["N_ALGORITMOS_ANOM"] >= 2).astype("int8")

        n_conf = sub["ANOMALIA_CONFIRMADA"].sum()
        log.info("  Anomalias confirmadas (≥2/3): %d (%.1f%%)",
                 n_conf, n_conf / len(sub) * 100)

        resultados.append(sub)

    df_result = pd.concat(resultados, ignore_index=True)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    sufixo = f"_{cargo.lower()}" if cargo else ""
    saida  = RESULTS_DIR / f"anomalias_{ano}{sufixo}.parquet"
    df_result.to_parquet(saida, index=False, engine="pyarrow")
    log.info("\nSalvo: %s (%.1f MB)", saida, saida.stat().st_size / 1e6)

    return df_result
