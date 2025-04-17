"""
# Data Governance - DG_01_fetch_from_fiadmin

DAG responsável pela coleta de dados do sistema FI-Admin e armazenamento no MongoDB.

## Fluxo Principal
1. **Coleta Paginada**: Utiliza API do FI-Admin com paginação (limit/offset)
2. **Persistência**: Armazena dados no MongoDB com tratamento de duplicidades
3. **Monitoramento**: Registra métricas de performance usando contexto de timer
"""

import requests
import pymongo
import logging
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base import BaseHook


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self  # permite acessar atributos depois
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.perf_counter()
        self.interval = self.end - self.start


def get_requests_session():
    session = requests.Session()
    retries = Retry(
        total=5,  # Number of total attempts
        backoff_factor=2,  # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 503, 504],  # Retry on these status codes
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


# Função para obter JSON da API de forma paginada e enviar para o MongoDB
def get_and_send_json_from_api(**kwargs):
    logger = logging.getLogger(__name__)

    fiadmin_conn = BaseHook.get_connection('fiadmin')

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    mongo_db = mongo_client["data_governance"]
    mongo_collection = mongo_db["01_landing_zone"]

    url = fiadmin_conn.host
    limit = 100
    offset = 0
    status = 1
    headers = {'apikey': fiadmin_conn.password}
    session = get_requests_session()
    
    has_more_data = True
    while has_more_data:
        logger.info(f"Coletando offset {offset} no FI-Admin")

        params = {
            "limit": limit,
            "offset": offset,
            "status": status,
            "format": "json"
        }
        with Timer() as t:
            response = session.get(url, params=params, headers=headers)
        logger.info(f"Tempo de coleta de dados no FI-Admin: {t.interval:.4f} segundos")

        if response.status_code == 200:
            json_data = response.json()
            
            # Se a resposta estiver vazia, não há mais dados
            if not json_data:
                has_more_data = False
            else:
                results = json_data['objects']
                with Timer() as t:
                    send_json_to_mongodb(results, mongo_collection)
                logger.info(f"Tempo de inserção de dados no MongoDB: {t.interval:.4f} segundos")
                
                offset += limit
        elif response.status_code == 502:
            logger.error(f"Erro {response.status_code} na coleta do batch de offset {offset} e limit {limit}. Pulando para o próximo batch.")
            offset += limit
        else:
            raise Exception(f"Erro ao obter dados do FI-Admin: Status code {response.status_code}")
            has_more_data = False


# Função para enviar JSON para o MongoDB
def send_json_to_mongodb(json_data, collection):
    # Extrai todos os IDs do lote atual
    batch_ids = [doc['id'] for doc in json_data if 'id' in doc]
    
    # Consulta os IDs existentes no MongoDB
    existing_ids = collection.distinct('id', {'id': {'$in': batch_ids}})
    
    # Filtra documentos novos
    new_docs = [
        doc for doc in json_data 
        if doc.get('id') not in existing_ids
    ]

    if new_docs:
        try:
            collection.insert_many(new_docs, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            # Check if the error is a duplicate key error
            for err in e.details['writeErrors']:
                if err['code'] == 11000:  # Duplicate key error code
                    # Ignore this error
                    continue
                else:
                    # Handle other errors
                    raise Exception(f"Error code {err['code']}: {err['errmsg']}")


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'DG_01_fetch_from_fiadmin',
    default_args=default_args,
    description='Data Governance - Obtém JSON paginado do FI-Admin e envia para o MongoDB',
    tags=["data_governance", "fi-admin", "mongodb"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    fetch_and_send_json_task = PythonOperator(
        task_id='fetch_and_send_json',
        python_callable=get_and_send_json_from_api
    )
