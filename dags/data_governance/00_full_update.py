"""
TBD
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'DG_00_full_update',
    default_args=default_args,
    description='Data Governance - Inicia o processamento em modo Full update, ou seja, coletará e processará todos os registros do Fi-Admin',
    tags=["data_governance", "fi-admin", "mongodb", "full_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    trigger = TriggerDagRunOperator(
        task_id='full_update',
        trigger_dag_id='DG_01_fetch_from_fiadmin',  # The DAG ID of the DAG you want to trigger
        conf={"update_mode": "FULL"}
    )
