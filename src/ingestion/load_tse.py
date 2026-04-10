"""
Módulo de leitura e filtragem dos CSVs brutos do TSE.

Responsável por:
    - Ler os CSVs extraídos de data/raw/
    - Filtrar apenas eleições municipais (CD_CARGO in {11, 13})
    - Selecionar apenas as colunas de interesse
    - Tratar encoding e separador corretos por ano
    - Salvar o resultado em data/processed/ como Parquet

Uso:
    python -m src.ingestion.load_tse --dataset candidatos --anos 2020 2024
    python -m src.ingestion.load_tse --dataset todos
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from src.ingestion.config import (
    ANOS_MUNICIPAIS,
    CARGOS_MUNICIPAIS,
    COLUNAS_BENS,
    COLUNAS_CANDIDATOS,
    COLUNAS_VOTACAO,
    DATASETS,
    ENCODING_POR_ANO,
    SEPARADOR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

# Colunas por dataset
COLUNAS_POR_DATASET = {
    "candidatos":     COLUNAS_CANDIDATOS,
    "votacao_munzona": COLUNAS_VOTACAO,
    "bens_candidatos": COLUNAS_BENS,
}


def listar_csvs(dataset: str, ano: int) -> list[Path]:
    """
    Retorna os CSVs a processar para um dataset/ano.

    O ZIP do TSE contém um arquivo _BRASIL.csv (consolidado) e 26 arquivos
    por UF com o mesmo conteúdo. Usamos apenas o _BRASIL quando disponível
    para evitar duplicação. Caso não exista, usamos todos os arquivos por UF.
    """
    pasta = RAW_DIR / dataset / str(ano)

    brasil = sorted(pasta.glob("*BRASIL*.csv"))
    if brasil:
        log.info("Usando arquivo consolidado: %s", brasil[0].name)
        return brasil

    csvs = sorted(pasta.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(
            f"Nenhum CSV encontrado em {pasta}. Execute download_tse.py primeiro."
        )
    log.info("Arquivo BRASIL não encontrado — lendo %d arquivos por UF.", len(csvs))
    return csvs


def _colunas_existentes(df: pd.DataFrame, colunas_desejadas: list[str]) -> list[str]:
    """Retorna apenas as colunas desejadas que existem no DataFrame."""
    existentes = [c for c in colunas_desejadas if c in df.columns]
    ausentes = set(colunas_desejadas) - set(existentes)
    if ausentes:
        log.debug("Colunas ausentes neste arquivo (podem variar entre anos): %s", ausentes)
    return existentes


def ler_csv_tse(caminho: Path, ano: int, colunas: list[str]) -> pd.DataFrame:
    """
    Lê um CSV do TSE com encoding e separador corretos.
    Aplica seleção de colunas e tratamento básico de tipos.
    """
    encoding = ENCODING_POR_ANO.get(ano, "utf-8")
    log.info("Lendo: %s (encoding=%s)", caminho.name, encoding)

    df = pd.read_csv(
        caminho,
        sep=SEPARADOR,
        encoding=encoding,
        dtype=str,           # ler tudo como string primeiro — evita erros de tipo
        on_bad_lines="skip", # linhas malformadas são ignoradas
        low_memory=False,
    )

    # Normalizar nomes de colunas (remover espaços e aspas que aparecem em alguns anos)
    df.columns = df.columns.str.strip().str.replace('"', "", regex=False)

    colunas_ok = _colunas_existentes(df, colunas)
    df = df[colunas_ok].copy()

    log.info("  → %d linhas | %d colunas carregadas", len(df), len(df.columns))
    return df


def filtrar_municipais(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém apenas registros de eleições municipais.
    Filtra por CD_CARGO (11=Prefeito, 13=Vereador) quando disponível.
    """
    if "CD_CARGO" not in df.columns:
        log.warning("Coluna CD_CARGO ausente — não foi possível filtrar por cargo.")
        return df

    antes = len(df)
    df = df[df["CD_CARGO"].isin([str(c) for c in CARGOS_MUNICIPAIS])].copy()
    log.info("  → Filtro municipal: %d → %d linhas (%d removidas)",
             antes, len(df), antes - len(df))
    return df


def processar(dataset: str, ano: int) -> pd.DataFrame:
    """
    Pipeline completo para um dataset/ano:
    leitura → seleção de colunas → filtro municipal → retorno do DataFrame.
    """
    csvs = listar_csvs(dataset, ano)
    colunas = COLUNAS_POR_DATASET[dataset]
    partes = []

    for csv in csvs:
        df = ler_csv_tse(csv, ano, colunas)
        df = filtrar_municipais(df)
        partes.append(df)

    resultado = pd.concat(partes, ignore_index=True) if len(partes) > 1 else partes[0]
    resultado["ANO_ELEICAO"] = str(ano)   # garantir que o ano esteja presente
    return resultado


def salvar_parquet(df: pd.DataFrame, dataset: str, ano: int) -> Path:
    """Salva o DataFrame processado como Parquet particionado por ano."""
    pasta = PROCESSED_DIR / dataset
    pasta.mkdir(parents=True, exist_ok=True)
    saida = pasta / f"{dataset}_{ano}.parquet"
    df.to_parquet(saida, index=False, engine="pyarrow")
    log.info("Salvo: %s (%.1f MB)", saida, saida.stat().st_size / 1e6)
    return saida


def carregar_todos_anos(dataset: str, anos: list[int] = ANOS_MUNICIPAIS) -> pd.DataFrame:
    """
    Lê os Parquets já processados de todos os anos e retorna um DataFrame unificado.
    Útil para análise exploratória e modelagem.
    """
    pasta = PROCESSED_DIR / dataset
    dfs = []
    for ano in anos:
        caminho = pasta / f"{dataset}_{ano}.parquet"
        if not caminho.exists():
            log.warning("Parquet não encontrado para %s/%d — execute load_tse.py primeiro.", dataset, ano)
            continue
        dfs.append(pd.read_parquet(caminho))

    if not dfs:
        raise FileNotFoundError(f"Nenhum Parquet encontrado em {pasta}.")

    df = pd.concat(dfs, ignore_index=True)
    log.info("Dataset '%s' carregado: %d linhas | %d colunas", dataset, len(df), len(df.columns))
    return df


def pipeline(datasets: list[str], anos: list[int]) -> None:
    """Executa leitura + filtro + salvamento para todas as combinações."""
    for ds in datasets:
        for ano in anos:
            log.info("══ Processando: %s / %d ══", ds, ano)
            try:
                df = processar(ds, ano)
                salvar_parquet(df, ds, ano)
            except FileNotFoundError as e:
                log.error("%s", e)
            except Exception as e:
                log.error("Erro inesperado em %s/%d: %s", ds, ano, e)


# ── CLI ──────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lê, filtra e converte microdados do TSE para Parquet"
    )
    parser.add_argument(
        "--dataset",
        choices=list(DATASETS.keys()) + ["todos"],
        default="todos",
    )
    parser.add_argument(
        "--anos",
        nargs="+",
        type=int,
        default=ANOS_MUNICIPAIS,
        choices=ANOS_MUNICIPAIS,
        metavar="ANO",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    datasets = list(DATASETS.keys()) if args.dataset == "todos" else [args.dataset]
    pipeline(datasets, args.anos)
