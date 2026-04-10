"""
Módulo de pré-processamento e limpeza dos microdados eleitorais.
"""

# Colunas de interesse nos microdados de candidatos (TSE)
COLUNAS_CANDIDATOS = [
    "ANO_ELEICAO",
    "NM_UE",           # Nome do município
    "SG_UF",           # Estado
    "DS_CARGO",        # Cargo (Prefeito, Vereador)
    "NM_CANDIDATO",
    "SG_PARTIDO",
    "DS_GENERO",
    "DS_GRAU_INSTRUCAO",
    "DS_SITUACAO_CANDIDATURA",
    "DS_SIT_TOT_TURNO",  # Resultado: Eleito, Não Eleito, etc.
]

# Mapeamento de encoding histórico dos arquivos TSE
ENCODING_TSE = {
    range(2004, 2014): "latin-1",
    range(2014, 2025): "utf-8",
}


def inferir_encoding(ano: int) -> str:
    """Retorna o encoding correto para o ano eleitoral informado."""
    for intervalo, enc in ENCODING_TSE.items():
        if ano in intervalo:
            return enc
    return "utf-8"


# TODO: implementar leitura com Pandas/Dask e seleção de colunas
# TODO: implementar tratamento de valores ausentes e padronização
# TODO: implementar harmonização de esquemas entre anos distintos
