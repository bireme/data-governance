"""
# DAG DG_00_run_all_incremental

Este módulo define uma DAG responsável por orquestrar a execução
sequencial de outras DAGs do pipeline de Data Governance, relacionadas ao processo
de atualização incremental das coleções MongoDB e geração de arquivos XML para o IAHx.

Objetivo:
    Orquestrar a execução encadeada das seguintes DAGs:
        1. DG_01_incremental_update
        2. DG_02_create_iahx_xml_collection
        3. DG_03_enrich_xml
        4. DG_04_export_xml

    Cada etapa depende da anterior e será executada apenas após o sucesso da execução
    anterior. Isso garante que os dados foram atualizados e processados corretamente
    antes do próximo passo iniciar.
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 14),
    'retries': 0,
}

with DAG(
    'DG_00_run_all_incremental',
    default_args=default_args,
    description='Data Governance - Orquestra todos os DAGs em ordem',
    schedule="0 3 * * *",
    catchup=False,
    tags=["data_governance", "fi-admin", "mongodb", "incremental_update"],
    doc_md=__doc__
) as dag:

    run_incremental_update = TriggerDagRunOperator(
        task_id='run_DG_01_incremental_update',
        trigger_dag_id='DG_01_incremental_update',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_iahx_xml_collection = TriggerDagRunOperator(
        task_id='run_DG_02_create_iahx_xml_collection',
        trigger_dag_id='DG_02_create_iahx_xml_collection',
        wait_for_completion=True,
        deferrable=True,
    )

    run_enrich_xml = TriggerDagRunOperator(
        task_id='run_DG_03_enrich_xml',
        trigger_dag_id='DG_03_enrich_xml',
        wait_for_completion=True,
        deferrable=True,
    )

    run_x01_create_iahx_xml_collection = TriggerDagRunOperator(
        task_id='run_DG_02_x01_create_iahx_xml_collection',
        trigger_dag_id='DG_02_x01_create_iahx_xml_collection',
        wait_for_completion=True,
        deferrable=True,
    )

    run_export_xml = TriggerDagRunOperator(
        task_id='run_DG_04_export_xml',
        trigger_dag_id='DG_04_export_xml',
        wait_for_completion=True,
        deferrable=True,
    )

    run_incremental_update >> run_create_iahx_xml_collection >> run_x01_create_iahx_xml_collection >> run_enrich_xml >> run_export_xml
