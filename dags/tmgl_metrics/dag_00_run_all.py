"""
# DAG TMGL_00_run_all

Este módulo define um DAG do Apache Airflow responsável por orquestrar a execução sequencial e automatizada de todos os DAGs do pipeline TMGL, 
garantindo que as etapas de ingestão, processamento, cálculo de métricas e exportação de relatórios ocorram na ordem correta e de forma coordenada.

## Funcionalidades Principais

- **Orquestração de DAGs dependentes:**  
  Utiliza o operador `TriggerDagRunOperator` para acionar outros DAGs do TMGL em sequência, aguardando a conclusão de cada etapa antes de iniciar a próxima, o que garante integridade e consistência dos dados em todo o pipeline.
- **Execução agendada:**  
  O DAG é configurado para rodar automaticamente toda sexta-feira às 22h (UTC-3), permitindo a atualização periódica e automatizada de todo o fluxo de dados e métricas TMGL.
- **Monitoramento centralizado:**  
  Permite o acompanhamento do status de todas as etapas do pipeline a partir de um único DAG, facilitando o monitoramento, troubleshooting e reprocessamento em caso de falhas.

## Estrutura do Pipeline

1. **run_TMGL_01_full_update**  
   - Inicia o processamento completo dos arquivos XML e a carga dos dados brutos para o MongoDB.
2. **run_TMGL_02_create_metric_total_docs**  
   - Calcula o total de documentos por país.
3. **run_TMGL_03_create_metric_doc_type**  
   - Calcula métricas por tipo de documento.
4. **run_TMGL_04_create_metric_study_type**  
   - Calcula métricas por tipo de estudo.
5. **run_TMGL_05_dimentions**  
   - Calcula métricas por dimensão temática.
6. **run_TMGL_06_subjects**  
   - Calcula métricas dos principais assuntos (descritores).
7. **run_TMGL_07_export_html_reports**  
   - Gera e exporta relatórios HTML com as métricas agregadas por país.

As dependências são organizadas para garantir que todas as métricas sejam calculadas após a atualização dos dados brutos e antes da exportação dos relatórios.

## Exemplo de Uso

1. Certifique-se de que todos os DAGs dependentes estejam corretamente configurados e habilitados no Airflow.
2. O DAG `TMGL_00_run_all` pode ser executado manualmente ou será disparado automaticamente conforme o agendamento.
3. O monitoramento pode ser feito pela interface do Airflow, acompanhando o status de cada etapa do pipeline.

## Observações

- O uso do `TriggerDagRunOperator` com `wait_for_completion=True` garante que cada etapa só inicie após a conclusão da anterior, evitando inconsistências.
- A estrutura modular permite fácil manutenção e expansão do pipeline, bastando adicionar novos DAGs e dependências conforme necessário.

"""



from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 18),
    'retries': 0,
}

with DAG(
    'TMGL_00_run_all',
    default_args=default_args,
    description='TMGL - Orquestra todos os DAGs TMGL em ordem',
    schedule="0 22 * * FRI",
    catchup=False,
    tags=["tmgl", "mongodb", "metrics"],
    doc_md=__doc__
) as dag:

    run_full_update = TriggerDagRunOperator(
        task_id='run_TMGL_01_full_update',
        trigger_dag_id='TMGL_01_full_update',
        wait_for_completion=True,
    )

    run_metric_total_docs = TriggerDagRunOperator(
        task_id='run_TMGL_02_create_metric_total_docs',
        trigger_dag_id='TMGL_02_create_metric_total_docs',
        wait_for_completion=True,
    )

    run_metric_doc_type = TriggerDagRunOperator(
        task_id='run_TMGL_03_create_metric_doc_type',
        trigger_dag_id='TMGL_03_create_metric_doc_type',
        wait_for_completion=True,
    )

    run_metric_study_type = TriggerDagRunOperator(
        task_id='run_TMGL_04_create_metric_study_type',
        trigger_dag_id='TMGL_04_create_metric_study_type',
        wait_for_completion=True,
    )

    run_dimentions = TriggerDagRunOperator(
        task_id='run_TMGL_05_dimentions',
        trigger_dag_id='TMGL_05_dimentions',
        wait_for_completion=True,
    )

    run_subjects = TriggerDagRunOperator(
        task_id='run_TMGL_06_subjects',
        trigger_dag_id='TMGL_06_subjects',
        wait_for_completion=True,
    )

    run_export_html = TriggerDagRunOperator(
        task_id='run_TMGL_07_export_html_reports',
        trigger_dag_id='TMGL_07_export_html_reports',
        wait_for_completion=True,
    )

    run_full_update >> run_metric_total_docs >> run_export_html
    run_full_update >> run_metric_doc_type >> run_export_html
    run_full_update >> run_metric_study_type >> run_export_html
    run_full_update >> run_dimentions >> run_export_html 
    run_full_update >> run_subjects >> run_export_html