"""
Configurações centrais da ingestão — URLs, datasets, cargos e encoding.
"""

# Anos de eleições municipais no Brasil (ciclos de 4 anos)
ANOS_MUNICIPAIS = [2004, 2008, 2012, 2016, 2020, 2024]

# Cargos municipais no TSE
# 11 = Prefeito | 13 = Vereador
CARGOS_MUNICIPAIS = {11, 13}

# Base CDN do TSE
CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele"

# Datasets que serão baixados e sua URL template
# {ano} é substituído dinamicamente
DATASETS = {
    "candidatos": {
        "url": f"{CDN}/consulta_cand/consulta_cand_{{ano}}.zip",
        "descricao": "Perfil dos candidatos (gênero, escolaridade, partido, cargo, resultado)",
    },
    "votacao_munzona": {
        "url": f"{CDN}/votacao_candidato_munzona/votacao_candidato_munzona_{{ano}}.zip",
        "descricao": "Votação nominal por candidato, município e zona eleitoral",
    },
    "bens_candidatos": {
        "url": f"{CDN}/bem_candidato/bem_candidato_{{ano}}.zip",
        "descricao": "Bens declarados pelos candidatos (útil para detecção de anomalias patrimoniais)",
    },
}

# Encoding histórico dos arquivos TSE
# Arquivos anteriores a 2014 usam latin-1
ENCODING_POR_ANO = {
    2004: "latin-1",
    2008: "latin-1",
    2012: "latin-1",
    2016: "latin-1",
    2020: "latin-1",
    2024: "latin-1",
}

# Separador dos CSVs do TSE
SEPARADOR = ";"

# Colunas de interesse por dataset (subconjunto para reduzir memória)
COLUNAS_CANDIDATOS = [
    "ANO_ELEICAO",
    "CD_TIPO_ELEICAO",
    "NM_TIPO_ELEICAO",
    "CD_CARGO",
    "DS_CARGO",
    "SG_UF",
    "CD_MUNICIPIO",
    "NM_UE",
    "SQ_CANDIDATO",
    "NM_CANDIDATO",
    "NM_URNA_CANDIDATO",
    "SG_PARTIDO",
    "NM_PARTIDO",
    "DS_GENERO",
    "DS_GRAU_INSTRUCAO",
    "DS_ESTADO_CIVIL",
    "NR_IDADE_DATA_POSSE",
    "DS_OCUPACAO",
    "DS_SITUACAO_CANDIDATURA",
    "DS_DETALHE_SITUACAO_CAND",
    "ST_REELEICAO",
    "DS_SIT_TOT_TURNO",
]

COLUNAS_VOTACAO = [
    "ANO_ELEICAO",
    "CD_TIPO_ELEICAO",
    "NM_TIPO_ELEICAO",
    "CD_CARGO",
    "DS_CARGO",
    "SG_UF",
    "CD_MUNICIPIO",
    "NM_MUNICIPIO",
    "NR_ZONA",
    "SQ_CANDIDATO",
    "NM_CANDIDATO",
    "NM_URNA_CANDIDATO",
    "SG_PARTIDO",
    "NM_PARTIDO",
    "QT_VOTOS_NOMINAIS",
    "QT_VOTOS_NOMINAIS_VALIDOS",
]

COLUNAS_BENS = [
    "ANO_ELEICAO",
    "CD_TIPO_ELEICAO",
    "SG_UF",
    "SQ_CANDIDATO",
    "NM_CANDIDATO",
    "DS_TIPO_BEM_CANDIDATO",
    "DS_BEM_CANDIDATO",
    "VR_BEM_CANDIDATO",
]
