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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    'DG_01_full_update',
    default_args=default_args,
    description='Data Governance - Inicia o processamento em modo Full update, ou seja, coletará e processará todos os registros do Fi-Admin',
    tags=["data_governance", "fi-admin", "mongodb", "full_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    harvest_fiadmin_and_store_in_mongodb_task = harvest_fiadmin_and_store_in_mongodb(
        update_mode='FULL'
    )

    harvest_fiadmin_and_store_in_mongodb_task