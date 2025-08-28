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
from pymongo import UpdateOne


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


def retry_error_records(error_collection, lz_collection, session, url, headers):
    logger = logging.getLogger(__name__)

    errors = list(error_collection.find({}))
    for error in errors:
        params = error.get("params")
        try:
            response = session.get(url, params=params, headers=headers)
            response.raise_for_status()

            json_data = response.json()
            results = json_data.get('objects', [])

            # Persistir novamente os registros obtidos
            send_json_to_mongodb(results, lz_collection)

            # Remover o erro da coleção após sucesso
            error_collection.delete_one({'_id': error['_id']})
            logger.info(f"Registro de erro com params {params} reprocessado com sucesso.")
        except Exception as e:
            logger.error(f"Falha ao reprocessar erro com params {params}: {str(e)}")


# Função para obter JSON da API de forma paginada e enviar para o MongoDB
@task
def harvest_fiadmin_and_store_in_mongodb(update_mode):
    logger = logging.getLogger(__name__)

    fiadmin_conn = BaseHook.get_connection('fiadmin')

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    mongo_db = mongo_client["data_governance"]
    lz_collection = mongo_db["01_landing_zone"]
    error_collection = mongo_db["01_fiadmin_error_tracking"]

    url = fiadmin_conn.host
    limit = 1
    offset = 0
    extra_params = {}
    headers = {'apikey': fiadmin_conn.password}
    session = get_requests_session()

    total_records = None
    max_erronous_loops = 50
    erronous_loops = 0

    if update_mode == "INCREMENTAL":
        incremental_update_date = (datetime.today() - timedelta(days=10)).strftime('%Y-%m-%d')
        extra_params = {'updated_time__gte': incremental_update_date}
    
    while total_records is None or offset < total_records:
        params = {
            "limit": limit,
            "offset": offset,
            "format": "json",
            **extra_params
        }
        logger.info(f"Coletando offset {offset} no FI-Admin com os parametros {params}")
        with Timer() as t:
            try:
                response = session.get(url, params=params, headers=headers)
                response.raise_for_status()
            except Exception as e:
                error_collection.insert_one({
                    "error": str(e),
                    "url": url,
                    "params": params,
                    "error_type": "SKIP",
                    "timestamp": datetime.now()
                })
                logger.error(f"Erro {response.status_code} na coleta do batch de offset {offset} e limit {limit}. Pulando para o próximo batch.")

                # Limitar o número de loops errôneos caso nao saibamos o total de registros (condicao de loop infinito)
                erronous_loops += 1
                if erronous_loops >= max_erronous_loops and total_records is None:
                    logger.error(f"Máximo de loops errôneos ({max_erronous_loops}) atingido. Encerrando coleta.")
                    break

        logger.info(f"Tempo de coleta de dados no FI-Admin: {t.interval:.4f} segundos")

        if response.status_code == 200:
            json_data = response.json()
            
            # Se a resposta estiver vazia, não há mais dados
            if not json_data or (json_data and not json_data['objects']):
                logger.info(f"Batch vazio recebido para offset {offset}. Encerrando coleta.")
                break
            else:
                if total_records is None:
                    total_records = json_data['meta']['total_count']
                    logger.info(f"Total de registros encontrados: {total_records}")

                results = json_data.get('objects', [])
                send_json_to_mongodb(results, lz_collection)
                
        offset += limit

    if update_mode == "INCREMENTAL":
        retry_error_records(error_collection, lz_collection, session, url, headers)


def send_json_to_mongodb(json_data, collection):
    batch = []
    for doc in json_data:
        if 'id' in doc:
            batch.append(
                UpdateOne(
                    {'id': doc['id']},    # Filtro para identificar o documento
                    {'$set': doc},        # Atualiza todos os campos do documento
                    upsert=True           # Insere se não existir
                )
            )
    if batch:
        collection.bulk_write(batch, ordered=False)
