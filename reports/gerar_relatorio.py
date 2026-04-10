"""
Gerador do relatório consolidado do projeto em PDF.
Executa com: python reports/gerar_relatorio.py
"""

from fpdf import FPDF
from pathlib import Path
import pandas as pd
import numpy as np

FONT_DIR = "/usr/share/fonts/truetype/dejavu/"
OUT_PATH = "reports/relatorio_projeto.pdf"
FIG_DIR  = Path("reports/figuras_2024")


class Relatorio(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("DV",  "", FONT_DIR + "DejaVuSans.ttf")
        self.add_font("DV",  "B", FONT_DIR + "DejaVuSans-Bold.ttf")
        self._pagina_atual = ""

    def header(self):
        self.set_font("DV", "B", 8)
        self.set_fill_color(30, 60, 120)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, "Detec\u00e7\u00e3o de Padr\u00f5es At\u00edpicos em Elei\u00e7\u00f5es Municipais Brasileiras  \u2014  PPCA / UnB  \u2014  2026",
                  fill=True, align="C", new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def footer(self):
        self.set_y(-13)
        self.set_font("DV", "", 7.5)
        self.set_text_color(130, 130, 130)
        self.cell(0, 8, f"P\u00e1g. {self.page_no()}", align="C")

    def chapter(self, numero, titulo):
        self.set_font("DV", "B", 13)
        self.set_fill_color(220, 230, 245)
        self.set_text_color(20, 40, 100)
        self.cell(0, 9, f"{numero}. {titulo}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def sub(self, texto):
        self.set_font("DV", "B", 10.5)
        self.set_text_color(40, 60, 120)
        self.multi_cell(0, 6, texto)
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def body(self, texto):
        self.set_font("DV", "", 9.5)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, texto)
        self.ln(2)

    def bullet(self, itens: list[str]):
        self.set_font("DV", "", 9.5)
        self.set_text_color(30, 30, 30)
        larg = self.w - self.l_margin - self.r_margin - 8
        for item in itens:
            x0 = self.get_x() + 8
            self.set_x(x0)
            self.multi_cell(larg, 5.5, f"\u2022  {item}")
        self.ln(1)

    def kv(self, label, valor):
        self.set_font("DV", "B", 9.5)
        self.cell(65, 6, label + ":", new_x="RIGHT")
        self.set_font("DV", "", 9.5)
        self.multi_cell(0, 6, str(valor))

    def figura(self, caminho, largura=170, legenda=""):
        if Path(caminho).exists():
            x = (self.w - largura) / 2
            self.image(str(caminho), x=x, w=largura)
            if legenda:
                self.set_font("DV", "", 8)
                self.set_text_color(100, 100, 100)
                self.cell(0, 5, legenda, align="C", new_x="LMARGIN", new_y="NEXT")
                self.set_text_color(0, 0, 0)
            self.ln(3)

    def tabela(self, headers, rows, cols_w, header_color=(30, 60, 120)):
        self.set_font("DV", "B", 8.5)
        self.set_fill_color(*header_color)
        self.set_text_color(255, 255, 255)
        for w, h in zip(cols_w, headers):
            self.cell(w, 7, h, border=1, fill=True, align="C")
        self.ln()
        self.set_text_color(20, 20, 20)
        for i, row in enumerate(rows):
            fill = (240, 245, 255) if i % 2 == 0 else (255, 255, 255)
            self.set_fill_color(*fill)
            self.set_font("DV", "", 8.5)
            for w, cell in zip(cols_w, row):
                self.cell(w, 6.5, str(cell), border=1, fill=True)
            self.ln()
        self.ln(2)


# ═══════════════════════════════════════════════════════════════════════════════
pdf = Relatorio()
pdf.set_margins(18, 18, 18)

# ── CAPA ───────────────────────────────────────────────────────────────────────
pdf.add_page()
pdf.ln(20)
pdf.set_font("DV", "B", 17)
pdf.set_text_color(20, 40, 100)
pdf.multi_cell(0, 10,
    "Detec\u00e7\u00e3o de Padr\u00f5es At\u00edpicos em\n"
    "Elei\u00e7\u00f5es Municipais Brasileiras\n"
    "Utilizando Processamento Paralelo",
    align="C")
pdf.ln(4)
pdf.set_font("DV", "", 10)
pdf.set_text_color(100, 100, 100)
pdf.cell(0, 7, "Relat\u00f3rio de Progresso do Projeto  \u2014  Ano 2026", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 7, "PPCA \u2013 Programa de P\u00f3s-Gradua\u00e7\u00e3o em Computa\u00e7\u00e3o Aplicada  |  UnB", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(10)
pdf.set_font("DV", "B", 10)
pdf.set_text_color(20, 40, 100)
pdf.cell(0, 7, "\u00cdndice de Conte\u00fado", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(3)
pdf.set_font("DV", "", 9.5)
pdf.set_text_color(30, 30, 30)
indice = [
    "1.  Contextualiza\u00e7\u00e3o do Problema",
    "2.  Objetivo Geral e Espec\u00edficos",
    "3.  Descri\u00e7\u00e3o dos Dados",
    "4.  Etapa 1 \u2014 Coleta e Ingest\u00e3o dos Microdados (TSE)",
    "5.  Etapa 2 \u2014 Pr\u00e9-processamento e Constru\u00e7\u00e3o do Dataset Mestre",
    "6.  Etapa 3 \u2014 An\u00e1lise Explorat\u00f3ria (EDA)",
    "7.  Pr\u00f3ximas Etapas",
]
for item in indice:
    pdf.cell(10)
    pdf.cell(0, 7, item, new_x="LMARGIN", new_y="NEXT")

# ── CAP 1: CONTEXTUALIZAÇÃO ────────────────────────────────────────────────────
pdf.add_page()
pdf.chapter("1", "Contextualiza\u00e7\u00e3o do Problema")
pdf.body(
    "As elei\u00e7\u00f5es municipais brasileiras constituem um dos processos democr\u00e1ticos de maior "
    "abrang\u00eancia territorial e volumetria de dados no pa\u00eds. Realizadas a cada quatro anos, abrangem "
    "mais de 5.500 munic\u00edpios e envolvem milh\u00f5es de candidatos, eleitores e registros acumulados "
    "ao longo de d\u00e9cadas. Esses dados, disponibilizados publicamente pelo Tribunal Superior Eleitoral (TSE), "
    "representam um reposit\u00f3rio de elevado valor anal\u00edtico para a compreens\u00e3o do comportamento "
    "pol\u00edtico-eleitoral brasileiro."
)
pdf.body(
    "A identifica\u00e7\u00e3o de padr\u00f5es at\u00edpicos \u2014 comportamentos estatisticamente discrepantes "
    "em rela\u00e7\u00e3o ao conjunto observado \u2014 constitui uma linha de pesquisa relevante tanto "
    "cient\u00edfica quanto praticamente. Anomalias em resultados eleitorais, concentra\u00e7\u00f5es incomuns "
    "de votos, varia\u00e7\u00f5es abruptas no desempenho de candidatos ou inconsist\u00eancias nos perfis "
    "sociopolíticos dos eleitos s\u00e3o exemplos de fen\u00f4menos que merecem investiga\u00e7\u00e3o sistem\u00e1tica."
)
pdf.body(
    "O desafio t\u00e9cnico central reside no volume e na heterogeneidade dos dados. O conjunto de informa\u00e7\u00f5es "
    "eleitorais do TSE, acumulado entre 2004 e 2024, compreende dezenas de milh\u00f5es de registros "
    "distribu\u00eddos em m\u00faltiplas tabelas, justificando a ado\u00e7\u00e3o de t\u00e9cnicas de Big Data "
    "e computa\u00e7\u00e3o paralela."
)

pdf.chapter("2", "Objetivo Geral e Espec\u00edficos")
pdf.sub("Objetivo Geral")
pdf.body(
    "Detectar padr\u00f5es at\u00edpicos nas elei\u00e7\u00f5es municipais brasileiras (2004\u20132024) por meio "
    "de minera\u00e7\u00e3o de dados em larga escala e processamento paralelo, visando identificar "
    "comportamentos eleitorais discrepantes que possam subsidiar an\u00e1lises de integridade eleitoral."
)
pdf.sub("Objetivos Espec\u00edficos")
pdf.bullet([
    "Consolidar e estruturar a base de dados eleitorais do TSE (2004\u20132024).",
    "Enriquecer os dados com informa\u00e7\u00f5es do IBGE (popula\u00e7\u00e3o, IDHM).",
    "Realizar an\u00e1lise explorat\u00f3ria multidimensional dos dados eleitorais.",
    "Aplicar t\u00e9cnicas de detec\u00e7\u00e3o de anomalias (Isolation Forest, LOF, DBSCAN).",
    "Implementar pipeline de processamento paralelo (PySpark / Dask).",
    "Interpretar e discutir os padr\u00f5es at\u00edpicos identificados.",
])

pdf.chapter("3", "Descri\u00e7\u00e3o dos Dados")
pdf.sub("Fontes Utilizadas")
pdf.tabela(
    ["Fonte", "Dataset", "Per\u00edodo", "Formato", "Volume estimado"],
    [
        ["TSE", "Candidatos", "2004\u20132024", "CSV/ZIP", "~3M registros"],
        ["TSE", "Vota\u00e7\u00e3o por mun./zona", "2004\u20132024", "CSV/ZIP", "~4M registros"],
        ["TSE", "Bens declarados", "2004\u20132024", "CSV/ZIP", "~5M registros"],
        ["IBGE", "Popula\u00e7\u00e3o/IDHM", "2000\u20132022", "CSV/XLSX", "~60k registros"],
    ],
    [38, 52, 24, 20, 36],
)
pdf.body(
    "Os dados do TSE est\u00e3o organizados por ano eleitoral e unidade federativa, em formato CSV "
    "com encoding latin-1 e separador ponto-e-v\u00edrgula. Foram selecionadas as vari\u00e1veis de "
    "maior relev\u00e2ncia para o projeto: identifica\u00e7\u00e3o do candidato, partido, cargo, "
    "munic\u00edpio, resultado eleitoral, g\u00eanero, escolaridade, estado civil, ocupa\u00e7\u00e3o "
    "e patrimônio declarado."
)

# ── CAP 4: INGESTÃO ────────────────────────────────────────────────────────────
pdf.add_page()
pdf.chapter("4", "Etapa 1 \u2014 Coleta e Ingest\u00e3o dos Microdados")
pdf.sub("M\u00f3dulos implementados")
pdf.bullet([
    "config.py \u2014 URLs reais do CDN do TSE, anos municipais, cargos, encoding, colunas de interesse.",
    "download_tse.py \u2014 download com barra de progresso, extra\u00e7\u00e3o de ZIP, cache local e CLI.",
    "load_tse.py \u2014 leitura dos CSVs, filtro de elei\u00e7\u00f5es municipais (CD_CARGO \u2208 {11, 13}), convers\u00e3o para Parquet.",
])
pdf.sub("Resultados da valida\u00e7\u00e3o \u2014 Ano 2024")
pdf.tabela(
    ["Dataset", "Arquivo ZIP", "Linhas brutas", "Ap\u00f3s filtro", "Parquet"],
    [
        ["candidatos",      "63,5 MB", "463.802", "447.792", "18,7 MB"],
        ["votacao_munzona", "48,1 MB", "717.310", "717.310", "22,0 MB"],
        ["bens_candidatos", "45,6 MB", "910.861", "910.861", "24,6 MB"],
    ],
    [42, 28, 30, 30, 26],
)
pdf.sub("Problemas identificados e corrigidos")
pdf.bullet([
    "Encoding: arquivos de todos os anos usam latin-1 (n\u00e3o utf-8 como inicialmente suposto).",
    "Duplica\u00e7\u00e3o: o ZIP cont\u00e9m um arquivo _BRASIL.csv consolidado e 26 arquivos por UF com o "
    "mesmo conte\u00fado. Solu\u00e7\u00e3o: priorizar o arquivo BRASIL e ignorar os por UF.",
])

# ── CAP 5: PRÉ-PROCESSAMENTO ───────────────────────────────────────────────────
pdf.chapter("5", "Etapa 2 \u2014 Pr\u00e9-processamento e Dataset Mestre")
pdf.sub("Transforma\u00e7\u00f5es aplicadas")
pdf.bullet([
    "Candidatos: substitui\u00e7\u00e3o de valores mascarados do TSE (\u201cN\u00c3O DIVULG\u00c1VEL\u201d, "
    "\u201c#NE\u201d, \u201c#NULO\u201d) por NaN; cria\u00e7\u00e3o de RESULTADO_SIMPLIFICADO, "
    "ELEITO (bin\u00e1rio), GRAU_INSTRUCAO_ORD (ordinal 0\u20138) e GENERO (M/F/Outro).",
    "Vota\u00e7\u00e3o: convers\u00e3o de votos para inteiro; agrega\u00e7\u00e3o por zona eleitoral "
    "\u2192 TOTAL_VOTOS, N_ZONAS e N_MUNICIPIOS por candidato.",
    "Bens: convers\u00e3o de formato monet\u00e1rio brasileiro (v\u00edrgula decimal) para float; "
    "agrega\u00e7\u00e3o por candidato \u2192 PATRIMONIO_TOTAL, N_BENS, MAIOR_BEM.",
    "Dataset mestre: LEFT JOIN dos 3 datasets por SQ_CANDIDATO \u2192 1 registro por candidato, 31 colunas.",
])
pdf.sub("Estat\u00edsticas do dataset mestre \u2014 2024")
pdf.tabela(
    ["M\u00e9trica", "Valor"],
    [
        ["Total de candidatos",          "447.792"],
        ["Prefeitos",                    "15.761"],
        ["Vereadores",                   "432.031"],
        ["Candidatos eleitos",           "63.713 (14,2%)"],
        ["Com votos registrados",        "427.099 (95,4%)"],
        ["Com patrim\u00f4nio declarado","283.009 (63,2%)"],
        ["Patrim\u00f4nio m\u00e1ximo",  "R$ 12,1 bilh\u00f5es"],
        ["Votos m\u00e1ximos",           "5.194.249"],
        ["Mediana de votos",             "88"],
    ],
    [90, 80],
)

# ── CAP 6: EDA ─────────────────────────────────────────────────────────────────
pdf.add_page()
pdf.chapter("6", "Etapa 3 \u2014 An\u00e1lise Explorat\u00f3ria (EDA)")

pdf.sub("6.1  Distribui\u00e7\u00e3o de votos por cargo")
pdf.body(
    "A distribui\u00e7\u00e3o de votos \u00e9 fortemente assim\u00e9trica (cauda longa) em ambos os cargos. "
    "Para vereadores, a mediana \u00e9 de apenas 88 votos, enquanto o m\u00e1ximo chega a 161.386. "
    "Para prefeitos, a mediana \u00e9 de 2.558 votos e o m\u00e1ximo de 5,19 milh\u00f5es. "
    "Essa assimetria \u00e9 um indicativo natural de outliers que merecem aten\u00e7\u00e3o na fase de detec\u00e7\u00e3o de anomalias."
)
pdf.figura(FIG_DIR / "fig1_distribuicao_votos.png", largura=168,
           legenda="Fig. 1 \u2014 Distribui\u00e7\u00e3o de votos em escala logar\u00edtmica por cargo (2024)")

pdf.sub("6.2  Taxa de elei\u00e7\u00e3o por g\u00eanero")
pdf.body(
    "Candidatos masculinos apresentam taxa de elei\u00e7\u00e3o consistentemente superior: 36,1% vs 30,5% "
    "para prefeito e 17,0% vs 6,9% para vereador. A diferen\u00e7a entre g\u00eaneros \u00e9 especialmente "
    "acentuada entre vereadores, sugerindo barreiras estruturais \u00e0 elei\u00e7\u00e3o feminina."
)
pdf.figura(FIG_DIR / "fig2_taxa_eleicao_genero.png", largura=140,
           legenda="Fig. 2 \u2014 Taxa de elei\u00e7\u00e3o por g\u00eanero e cargo (2024)")

pdf.add_page()
pdf.sub("6.3  Partidos com maior n\u00famero de eleitos")
pdf.body(
    "MDB, PP e PSD lideram em n\u00famero de vereadores eleitos, refletindo a capilaridade "
    "dessas siglas no interior do pa\u00eds. Entre os prefeitos, MDB e PP tamb\u00e9m dominam, "
    "consolidando o perfil das grandes m\u00e1quinas partid\u00e1rias municipais brasileiras."
)
pdf.figura(FIG_DIR / "fig3_top_partidos_eleitos.png", largura=168,
           legenda="Fig. 3 \u2014 Top 10 partidos por candidatos eleitos, por cargo (2024)")

pdf.sub("6.4  Patrim\u00f4nio declarado: eleitos vs n\u00e3o eleitos")
pdf.body(
    "Candidatos eleitos apresentam patrim\u00f4nio mediano superior ao dos n\u00e3o eleitos em ambos os cargos, "
    "embora a diferen\u00e7a n\u00e3o seja determinante isoladamente. Nota-se tamb\u00e9m a exist\u00eancia de "
    "candidatos n\u00e3o eleitos com patrim\u00f4nio declarado muito elevado, o que configura um padr\u00e3o "
    "at\u00edpico a ser investigado."
)
pdf.figura(FIG_DIR / "fig4_patrimonio_eleito.png", largura=145,
           legenda="Fig. 4 \u2014 Patrim\u00f4nio mediano declarado por resultado e cargo (2024, R$ mil)")

pdf.add_page()
pdf.sub("6.5  Taxa de elei\u00e7\u00e3o de vereadores por UF e g\u00eanero")
pdf.body(
    "O mapa de calor revela heterogeneidade regional significativa. Estados como RR e RO apresentam "
    "taxas de elei\u00e7\u00e3o masculina acima de 20%, enquanto estados maiores (SP, MG) t\u00eam taxas "
    "menores devido ao maior n\u00famero de candidatos por vaga. A desigualdade de g\u00eanero \u00e9 "
    "sistem\u00e1tica em todas as UFs."
)
pdf.figura(FIG_DIR / "fig5_heatmap_uf_genero.png", largura=168,
           legenda="Fig. 5 \u2014 Taxa de elei\u00e7\u00e3o de vereadores por UF e g\u00eanero (2024, %)")

pdf.sub("6.6  Grau de instru\u00e7\u00e3o e taxa de elei\u00e7\u00e3o")
pdf.body(
    "Candidatos com ensino superior completo e p\u00f3s-gradua\u00e7\u00e3o apresentam as maiores taxas de "
    "elei\u00e7\u00e3o relativas ao seu grupo. Contudo, candidatos analfabetos tamb\u00e9m apresentam taxa "
    "n\u00e3o nula, evidenciando que lideran\u00e7a local e reconhecimento comunit\u00e1rio podem superar "
    "barreiras educacionais em munic\u00edpios menores."
)
pdf.figura(FIG_DIR / "fig6_instrucao_eleitos.png", largura=162,
           legenda="Fig. 6 \u2014 Taxa de elei\u00e7\u00e3o por grau de instru\u00e7\u00e3o (2024)")

# ── CAP 7: PRÓXIMAS ETAPAS ─────────────────────────────────────────────────────
pdf.add_page()
pdf.chapter("7", "Pr\u00f3ximas Etapas")
pdf.tabela(
    ["#", "Etapa", "Descri\u00e7\u00e3o", "Status"],
    [
        ["1", "Ingest\u00e3o", "Download e extra\u00e7\u00e3o dos dados TSE 2004\u20132024", "Validado (2024)"],
        ["2", "Pr\u00e9-processamento", "Limpeza, normaliza\u00e7\u00e3o e join dos datasets", "Validado (2024)"],
        ["3", "EDA", "An\u00e1lise explorat\u00f3ria multidimensional", "Em andamento"],
        ["4", "Escala (todos anos)", "Executar pipeline para 2004\u20132020", "Pendente"],
        ["5", "Integra\u00e7\u00e3o IBGE", "Enriquecimento com dados de popula\u00e7\u00e3o e IDHM", "Pendente"],
        ["6", "Processamento Paralelo", "Refatorar pipeline com PySpark ou Dask", "Pendente"],
        ["7", "Detec\u00e7\u00e3o de Anomalias", "Isolation Forest, LOF, DBSCAN", "Pendente"],
        ["8", "Interpreta\u00e7\u00e3o", "An\u00e1lise dos padr\u00f5es at\u00edpicos identificados", "Pendente"],
        ["9", "Relat\u00f3rio Final", "Artigo e apresenta\u00e7\u00e3o para o PPCA", "Pendente"],
    ],
    [8, 40, 90, 30],
)

pdf.ln(4)
pdf.set_font("DV", "", 8)
pdf.set_text_color(100, 100, 100)
pdf.cell(0, 5,
    "Relat\u00f3rio gerado automaticamente pelo pipeline do projeto  \u2014  PPCA / UnB  \u2014  Abril 2026",
    align="C")

pdf.output(OUT_PATH)
print(f"Relat\u00f3rio gerado: {OUT_PATH}")
