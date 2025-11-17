import logging
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.data_governance.tasks_for_01 import send_json_to_mongodb
from data_governance.dags.data_governance.tasks_for_01 import get_requests_session
from data_governance.dags.data_governance.tasks_for_01 import Timer


def ids_update():
    logger = logging.getLogger(__name__)

    fiadmin_conn = BaseHook.get_connection('fiadmin')

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    lz_collection = mongo_hook.get_collection('01_landing_zone', 'data_governance')

    url = fiadmin_conn.host
    headers = {'apikey': fiadmin_conn.password}
    session = get_requests_session()

    fs_hook = FSHook(fs_conn_id='DG_IDS_LIST_FILE_PATH_FOR_UPDATE')
    ids_list_path = fs_hook.get_path()
    ids_arr = open(ids_list_path, 'r').readlines()
    ids_list = [id_str.strip() for id_str in ids_arr if id_str.strip()]
    
    for id_str in ids_list:
        params = {
            "format": "json",
            "id": id_str
        }
        logger.info(f"Coletando dados do FI-Admin com os parametros {params}")
        with Timer() as t:
            try:
                response = session.get(url, params=params, headers=headers)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Erro {response.status_code} na coleta. Pulando para o próximo ID.")

        logger.info(f"Tempo de coleta de dados no FI-Admin: {t.interval:.4f} segundos")

        if response.status_code == 200:
            json_data = response.json()
            
            # Se a resposta estiver vazia, não há mais dados
            if not json_data or (json_data and not json_data['objects']):
                logger.info(f"Batch vazio recebido para ID {id_str}.")
            else:
                results = json_data.get('objects', [])
                send_json_to_mongodb(results, lz_collection)


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'DG_01_ids_update',
    default_args=default_args,
    description='Data Governance - Inicia o processamento em modo IDS, ou seja, coletará e processará os registros do Fi-Admin com uma lista de IDs armazenada em variável do Airflow',
    tags=["data_governance", "fi-admin", "mongodb", "ids_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    ids_update_task = PythonOperator(
        task_id='ids_update',
        python_callable=ids_update
    )