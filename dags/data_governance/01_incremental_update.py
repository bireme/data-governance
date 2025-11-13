"""
# Data Governance - DG_01_incremental_update

DAG wrapper para execução de atualização incremental dos dados do FI-Admin.

## Objetivo Principal
Executar coleta seletiva de registros atualizados recentemente no FI-Admin e armazenamento no MongoDB.

## Características Principais
- **Modo Incremental**: Coleta apenas registros modificados nos últimos 5 dias
- **Herança de Funcionalidades**:
  - Paginação automática
  - Tratamento de duplicidades
  - Retry inteligente para falhas de rede
"""

from airflow import DAG
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
    'DG_01_incremental_update',
    default_args=default_args,
    description='Data Governance - Inicia o processamento em modo Incremental, ou seja, coletará e processará os registros do Fi-Admin com Update Time recente',
    tags=["data_governance", "fi-admin", "mongodb", "incremental_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    harvest_fiadmin_and_store_in_mongodb_task = harvest_fiadmin_and_store_in_mongodb(
        update_mode='INCREMENTAL'
    )