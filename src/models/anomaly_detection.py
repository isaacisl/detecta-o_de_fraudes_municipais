"""
Módulo de detecção de anomalias nos dados eleitorais.
Algoritmos candidatos: Isolation Forest, LOF, DBSCAN.
"""

# TODO: implementar pipeline de detecção com Scikit-learn
# TODO: avaliar adaptação para PySpark MLlib (escala)
# TODO: definir features relevantes para cada modelo

MODELOS_DISPONIVEIS = [
    "isolation_forest",
    "local_outlier_factor",
    "dbscan",
]
