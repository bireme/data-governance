"""
# Data Governance - DG_01_full_update

DAG wrapper para execução de atualização completa dos dados do FI-Admin.

## Objetivo Principal
Executar carga completa de todos os registros disponíveis no FI-Admin para o MongoDB com tratamento de duplicidades.

## Características Principais
- **Modo Full**: Coleta total com resiliência a falhas intermediárias
- **Estratégia de Offset**: Retoma coleta do último ponto em caso de interrupção
- **Herança de Funcionalidades**:
  - Paginação automática
  - Duplicidade controlada por índice único
  - Retry inteligente para falhas transitórias
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task
from data_governance.dags.data_governance.tasks_for_01 import harvest_fiadmin_and_store_in_mongodb


@task
def setup_error_tracking_collection():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    db = mongo_hook.get_conn()
    collection_name = '01_fiadmin_error_tracking'
    db_name = 'data_governance'

    # Deleta a coleção se existir
    if collection_name in db[db_name].list_collection_names():
        db[db_name].drop_collection(collection_name)

    # Cria a coleção
    new_collection = db[db_name][collection_name]
    new_collection.create_index([('error_type', 1)])


@task
def get_total_records(update_mode):
    total_records = 3000  
    return total_records


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'DG_01_full_update',
    default_args=default_args,
    description='Data Governance - Inicia o processamento em modo Full update, ou seja, coletará e processará todos os registros do Fi-Admin',
    tags=["data_governance", "fi-admin", "mongodb", "full_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    setup_error_tracking_collection_task = setup_error_tracking_collection()

    # Criar 3 tasks paralelas, cada uma com sua fatia de offset
    workers = 3
    limit = 100
    step = limit * workers  # pula 3 em 3 para cada worker
    for i in range(workers):
        offset = i * limit
        harvest_task = harvest_fiadmin_and_store_in_mongodb.override(
            task_id=f'harvest_fiadmin_and_store_in_mongodb_part_{i}'
        )(
            update_mode='FULL',
            offset=offset,
            limit=limit,
            step = step
        )
        setup_error_tracking_collection_task >> harvest_task