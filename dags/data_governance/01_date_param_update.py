from airflow import DAG
from airflow.models.param import Param
from data_governance.dags.data_governance.tasks_for_01 import harvest_fiadmin_and_store_in_mongodb


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'DG_01_date_param_update',
    params={
        "start_date": Param(
            type="string",
            minLength=10,
            maxLength=10,
        ),
        "end_date": Param(
            type="string",
            minLength=10,
            maxLength=10,
        ),
    },
    default_args=default_args,
    description='Data Governance - Inicia o processamento em modo Incremental, ou seja, coletará e processará os registros do Fi-Admin com Update Time definido via param',
    tags=["data_governance", "fi-admin", "mongodb", "incremental_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    harvest_fiadmin_and_store_in_mongodb_task = harvest_fiadmin_and_store_in_mongodb(
        update_mode='DATE_PARAM'
    )