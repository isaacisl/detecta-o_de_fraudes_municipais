"""
Módulo de ingestão dos microdados do TSE.
Responsável por baixar e organizar os arquivos CSV por ano eleitoral.
"""

ANOS_ELEICOES_MUNICIPAIS = [2004, 2008, 2012, 2016, 2020, 2024]

BASE_URL_TSE = "https://dadosabertos.tse.jus.br/dataset"

DATASETS = {
    "candidatos": "candidatos",
    "resultados": "resultados",
    "partidos":   "partidos",
}


def listar_anos():
    """Retorna a lista de anos com eleições municipais no período estudado."""
    return ANOS_ELEICOES_MUNICIPAIS


def construir_url(dataset: str, ano: int) -> str:
    """Monta a URL de download para um dataset e ano específicos."""
    return f"{BASE_URL_TSE}/{DATASETS[dataset]}/{ano}"


# TODO: implementar download automatizado via requests/httpx
# TODO: implementar descompactação dos arquivos .zip do TSE
# TODO: implementar validação de integridade (checksum)
