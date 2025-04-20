"""
# Data Governance - Tasks for 01 DAG

Task reutilizável para coleta e armazenamento de dados do FI-Admin no MongoDB.

## Objetivo Principal
Coordenar a ingestão segura e eficiente de dados do sistema FI-Admin para a camada de landing zone no MongoDB, com tratamento de dados e monitoramento integrado.

## 🔑 Funcionalidades-Chave
1. **Coleta Resiliente**
   - Paginação automática (limit/offset)
   - Retry inteligente para falhas HTTP (5 tentativas com backoff)
   - Tratamento especial para erro 502 (continuação automática)

2. **Gestão de Dados**
   - Detecção e prevenção de duplicidades
   - Modos de atualização configuráveis (FULL/INCREMENTAL)
   - Inserção em lote

## ⚙️ Parâmetros de Configuração
| Parâmetro        | Tipo    | Default  | Descrição                                                                 |
|------------------|---------|----------|---------------------------------------------------------------------------|
| `update_mode`    | string  | REQUIRED | Modo de operação: `FULL` (carga completa) ou `INCREMENTAL` (5 dias atrás) |
| `mongo_conn_id`  | string  | 'mongo'  | Conexão Airflow para MongoDB                                             |
| `fiadmin_conn_id`| string  | 'fiadmin'| Conexão Airflow com host da API e apikey no password        
"""


import requests
import pymongo
import logging
import time
import math
from datetime import datetime
from datetime import timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task


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
@task
def harvest_fiadmin_and_store_in_mongodb(update_mode):
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
    extra_params = {}
    headers = {'apikey': fiadmin_conn.password}
    session = get_requests_session()

    if update_mode == "INCREMENTAL":
        incremental_update_date = (datetime.today() - timedelta(days=5)).strftime('%Y-%m-%d')
        extra_params = {'updated_time__gte': incremental_update_date}
    elif update_mode == "FULL":
        total_records = mongo_collection.count_documents({})
        offset = math.floor(total_records / limit) * limit
        logger.info(f"Há {total_records} registros na coleção. Ativando offset {offset}.")
    
    has_more_data = True
    while has_more_data:
        params = {
            "limit": limit,
            "offset": offset,
            "status": status,
            "format": "json",
            **extra_params
        }
        logger.info(f"Coletando offset {offset} no FI-Admin com os parametros {params}")
        with Timer() as t:
            response = session.get(url, params=params, headers=headers)
        logger.info(f"Tempo de coleta de dados no FI-Admin: {t.interval:.4f} segundos")

        if response.status_code == 200:
            json_data = response.json()
            
            # Se a resposta estiver vazia, não há mais dados
            if not json_data or (json_data and not json_data['objects']):
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