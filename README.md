# Detecção de Padrões Atípicos em Eleições Municipais Brasileiras

> Projeto de mestrado — PPCA (Computação Aplicada)  
> Disciplina: Mineração de Dados Massivos  
> Ano: 2026

---

## Descrição

Este projeto investiga padrões atípicos nas eleições municipais brasileiras (2004–2024) utilizando técnicas de mineração de dados em larga escala e processamento paralelo. Os dados são provenientes do **TSE (Tribunal Superior Eleitoral)** e do **IBGE**, com mais de dezenas de milhões de registros históricos.

---

## Estrutura do Repositório

```
eleicoes-anomalias/
├── data/
│   ├── raw/          # Microdados brutos do TSE e IBGE (não versionados)
│   └── processed/    # Dados limpos e integrados
├── notebooks/        # Análises exploratórias em Jupyter
├── src/
│   ├── ingestion/    # Scripts de coleta e carga dos dados
│   ├── preprocessing/# Limpeza, padronização e integração
│   ├── analysis/     # Análise exploratória e estatísticas
│   └── models/       # Modelos de detecção de anomalias
├── reports/          # Relatórios e documentos de entrega
├── docs/             # Documentação do projeto
├── requirements.txt
└── README.md
```

---

## Fontes de Dados

| Fonte | Descrição | Formato |
|-------|-----------|---------|
| [TSE – Dados Abertos](https://dadosabertos.tse.jus.br) | Microdados eleitorais (candidatos, resultados, partidos) 2004–2024 | CSV |
| [IBGE](https://www.ibge.gov.br) | População municipal, IDHM | CSV / XLSX |

---

## Tecnologias

| Ferramenta | Uso |
|------------|-----|
| Python 3.x | Linguagem principal |
| PySpark / Dask | Processamento paralelo e distribuído |
| Pandas | Prototipação e pré-processamento |
| Scikit-learn | Modelos de detecção de anomalias |
| PostgreSQL | Armazenamento estruturado |
| Jupyter Notebook | Análise exploratória |
| Apache Parquet | Formato de armazenamento eficiente |
| Git + GitHub | Versionamento |

---

## Instalação

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd eleicoes-anomalias

# Crie e ative o ambiente virtual
python3 -m venv .venv
source .venv/bin/activate

# Instale as dependências
pip install -r requirements.txt
```

---

## Etapas do Projeto

- [x] Proposta inicial — Contextualização, objetivos e descrição dos dados
- [ ] Coleta e ingestão dos microdados do TSE
- [ ] Pré-processamento e integração com dados do IBGE
- [ ] Análise exploratória (EDA)
- [ ] Implementação do pipeline paralelo (PySpark/Dask)
- [ ] Aplicação de modelos de detecção de anomalias
- [ ] Interpretação e relatório final

---

## Licença

Uso acadêmico. Dados públicos conforme legislação brasileira de acesso à informação (Lei nº 12.527/2011).
