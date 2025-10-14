from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def setup_collections():
    """Configura coleção e índices no MongoDB"""
    mongo_hook = MongoHook(mongo_conn_id='mongo')

    # Deleta coleções a serem atualizadas
    db = mongo_hook.get_conn()['tmgl_charts']
    db['02_metrics'].drop()

    # Cria coleção de métricas
    collection_metrics = mongo_hook.get_collection('02_metrics', 'tmgl_charts')
    collection_metrics.create_index([('type', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_metrics.create_index([('region', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_metrics.create_index([('year', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_metrics.create_index([('type', 1), ('region', 1), ('name', 1), ('year', 1)])


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'TMGL_REGION_01_setup',
    default_args=default_args,
    description='TMGL REGION - Setup das coleções',
    tags=["tmgl", "xml", "mongodb", "full_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    setup_task = PythonOperator(
        task_id='setup_collections',
        python_callable=setup_collections
    )