"""
Módulo de download e extração dos microdados do TSE.

Fluxo:
    1. Monta a URL do arquivo .zip a partir do dataset e ano
    2. Baixa o arquivo com barra de progresso
    3. Extrai os CSVs para data/raw/{dataset}/{ano}/
    4. Verifica se o download já existe (evita redownload)

Uso:
    python -m src.ingestion.download_tse --dataset candidatos --anos 2020 2024
    python -m src.ingestion.download_tse --dataset todos --anos 2004 2008 2012 2016 2020 2024
"""

import argparse
import logging
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm

from src.ingestion.config import ANOS_MUNICIPAIS, DATASETS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")


def montar_url(dataset: str, ano: int) -> str:
    """Retorna a URL de download do CDN do TSE para um dataset e ano."""
    template = DATASETS[dataset]["url"]
    return template.format(ano=ano)


def destino_zip(dataset: str, ano: int) -> Path:
    """Retorna o caminho local onde o .zip será salvo."""
    pasta = RAW_DIR / dataset / str(ano)
    pasta.mkdir(parents=True, exist_ok=True)
    nome = montar_url(dataset, ano).split("/")[-1]
    return pasta / nome


def ja_extraido(dataset: str, ano: int) -> bool:
    """Verifica se os CSVs já foram extraídos para este dataset/ano."""
    pasta = RAW_DIR / dataset / str(ano)
    csvs = list(pasta.glob("*.csv"))
    return len(csvs) > 0


def baixar_zip(url: str, destino: Path, chunk_size: int = 8192) -> bool:
    """
    Faz o download de um arquivo ZIP com barra de progresso.
    Retorna True se bem-sucedido, False em caso de erro.
    """
    if destino.exists():
        log.info("Arquivo já existe, pulando download: %s", destino.name)
        return True

    log.info("Baixando: %s", url)
    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error("Falha ao baixar %s: %s", url, e)
        return False

    total = int(resp.headers.get("content-length", 0))
    with (
        open(destino, "wb") as f,
        tqdm(total=total, unit="B", unit_scale=True, desc=destino.name) as barra,
    ):
        for chunk in resp.iter_content(chunk_size=chunk_size):
            f.write(chunk)
            barra.update(len(chunk))

    log.info("Download concluído: %s (%.1f MB)", destino.name, destino.stat().st_size / 1e6)
    return True


def extrair_zip(caminho_zip: Path) -> None:
    """Extrai o conteúdo do .zip no mesmo diretório."""
    log.info("Extraindo: %s", caminho_zip.name)
    with zipfile.ZipFile(caminho_zip, "r") as zf:
        membros = zf.namelist()
        log.info("  → %d arquivo(s) encontrado(s): %s", len(membros), membros[:3])
        zf.extractall(caminho_zip.parent)
    log.info("Extração concluída em: %s", caminho_zip.parent)


def ingerir(dataset: str, ano: int, forcar: bool = False) -> bool:
    """
    Orquestra download + extração para um dataset/ano.

    Args:
        dataset: chave do dict DATASETS (ex: 'candidatos')
        ano:     ano eleitoral (ex: 2024)
        forcar:  se True, baixa mesmo que já exista
    Returns:
        True se concluído com sucesso
    """
    if not forcar and ja_extraido(dataset, ano):
        log.info("[%s/%d] Dados já extraídos. Use --forcar para reprocessar.", dataset, ano)
        return True

    url = montar_url(dataset, ano)
    destino = destino_zip(dataset, ano)

    ok = baixar_zip(url, destino)
    if not ok:
        return False

    extrair_zip(destino)
    return True


def ingerir_todos(datasets: list[str], anos: list[int], forcar: bool = False) -> None:
    """Executa a ingestão para todas as combinações dataset × ano."""
    total = len(datasets) * len(anos)
    erros = []

    log.info("Iniciando ingestão: %d dataset(s) × %d ano(s) = %d downloads", len(datasets), len(anos), total)

    for ds in datasets:
        for ano in anos:
            log.info("── [%s | %d] ──", ds, ano)
            ok = ingerir(ds, ano, forcar=forcar)
            if not ok:
                erros.append((ds, ano))

    if erros:
        log.warning("Falhas na ingestão (%d):", len(erros))
        for ds, ano in erros:
            log.warning("  ✗ %s / %d", ds, ano)
    else:
        log.info("Ingestão concluída sem erros.")


# ── CLI ──────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download e extração dos microdados do TSE (eleições municipais)"
    )
    parser.add_argument(
        "--dataset",
        choices=list(DATASETS.keys()) + ["todos"],
        default="todos",
        help="Dataset a baixar. 'todos' baixa todos os datasets configurados.",
    )
    parser.add_argument(
        "--anos",
        nargs="+",
        type=int,
        default=ANOS_MUNICIPAIS,
        choices=ANOS_MUNICIPAIS,
        metavar="ANO",
        help=f"Anos eleitorais. Padrão: {ANOS_MUNICIPAIS}",
    )
    parser.add_argument(
        "--forcar",
        action="store_true",
        help="Força re-download mesmo que os arquivos já existam.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    datasets = list(DATASETS.keys()) if args.dataset == "todos" else [args.dataset]
    ingerir_todos(datasets, args.anos, forcar=args.forcar)
