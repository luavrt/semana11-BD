"""Referência para Airflow real (Airflow 3.x / airflow.sdk).
Este arquivo é material de leitura. A prática principal usa mini_orquestrador.py para não depender de Docker/WSL.
"""
from airflow.sdk import dag, task
from datetime import datetime

@dag(dag_id='pipeline_lakehouse_referencia', schedule='0 2 * * *', start_date=datetime(2025,1,1), catchup=False)
def pipeline_lakehouse_referencia():
    @task
    def extrair():
        return 360
    @task
    def transformar(n_raw: int):
        return n_raw - 5
    @task
    def validar(n_silver: int):
        assert n_silver > 0
    validar(transformar(extrair()))

pipeline_lakehouse_referencia()
