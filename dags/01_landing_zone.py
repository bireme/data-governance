import requests
import json
import pymongo
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base import BaseHook


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

    has_more_data = True
    while has_more_data:
        logger.info("Coletando offset %i no FI-Admin" % offset)

        params = {
            "limit": limit,
            "offset": offset,
            "status": status,
            "format": "json"
        }
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            json_data = response.json()
            
            # Se a resposta estiver vazia, não há mais dados
            if not json_data:
                has_more_data = False
            else:
                results = json_data['objects']
                send_json_to_mongodb(results, mongo_collection)
                
                offset += limit
        else:
            raise Exception(f"Erro ao obter dados do FI-Admin: Status code {response.status_code}")
            has_more_data = False


# Função para enviar JSON para o MongoDB
def send_json_to_mongodb(json_data, collection):
    try:
        collection.insert_many(json_data, ordered=False)
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
    '01_fetch_from_fiadmin',
    default_args=default_args,
    description='A DAG que obtém JSON paginado do FI-Admin e envia para o MongoDB',
    start_date=datetime(2025, 3, 19),
    schedule=None,
    catchup=False,
) as dag:
    fetch_and_send_json_task = PythonOperator(
        task_id='fetch_and_send_json',
        python_callable=get_and_send_json_from_api
    )
