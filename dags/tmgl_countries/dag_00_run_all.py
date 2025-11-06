from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 18),
    'retries': 0,
}

with DAG(
    'TMGL_COUNTRIES_00_run_all',
    default_args=default_args,
    description='TMGL - Orquestra todos os DAGs TMGL COUNTRIES em ordem',
    schedule="30 4 * * 7",
    catchup=False,
    tags=["tmgl", "mongodb", "country"],
    doc_md=__doc__
) as dag:

    run_setup = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_01_setup',
        trigger_dag_id='TMGL_COUNTRIES_01_setup',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_metric_complementary = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_metric_complementary',
        trigger_dag_id='TMGL_COUNTRIES_02_create_metric_complementary',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_metric_dimentions = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_metric_dimentions',
        trigger_dag_id='TMGL_COUNTRIES_02_create_metric_dimentions',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_metric_doctype = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_metric_doctype',
        trigger_dag_id='TMGL_COUNTRIES_02_create_metric_doctype',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_metric_regions = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_metric_regions',
        trigger_dag_id='TMGL_COUNTRIES_02_create_metric_regions',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_metric_studytype = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_metric_studytype',
        trigger_dag_id='TMGL_COUNTRIES_02_create_metric_studytype',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_metric_subjects = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_metric_subjects',
        trigger_dag_id='TMGL_COUNTRIES_02_create_metric_subjects',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_metric_therapies = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_metric_therapies',
        trigger_dag_id='TMGL_COUNTRIES_02_create_metric_therapies',
        wait_for_completion=True,
        deferrable=True,
    )

    run_create_timeline = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_02_create_timeline',
        trigger_dag_id='TMGL_COUNTRIES_02_create_timeline',
        wait_for_completion=True,
        deferrable=True,
    )

    """run_create_metric_traditional = TriggerDagRunOperator(
        task_id='run_TMGL_REGION_02_create_metric_traditional',
        trigger_dag_id='TMGL_REGION_02_create_metric_traditional',
        wait_for_completion=True,
        deferrable=True,
    )"""

    run_export_html = TriggerDagRunOperator(
        task_id='run_TMGL_COUNTRIES_03_export_html',
        trigger_dag_id='TMGL_COUNTRIES_03_export_html',
        wait_for_completion=True,
        deferrable=True,
    )

    run_setup >> run_create_metric_complementary >> run_export_html
    run_setup >> run_create_metric_dimentions >> run_export_html
    run_setup >> run_create_metric_doctype >> run_export_html
    run_setup >> run_create_metric_regions >> run_export_html
    run_setup >> run_create_metric_studytype >> run_export_html
    run_setup >> run_create_metric_subjects >> run_export_html
    run_setup >> run_create_metric_therapies >> run_export_html
    run_setup >> run_create_timeline >> run_export_html
    #run_setup >> run_create_metric_traditional >> run_export_html
