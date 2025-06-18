"""
# DAG TMGL_07_export_html_reports

Este módulo define um DAG do Apache Airflow responsável pela geração e exportação de relatórios HTML com métricas TMGL por país, utilizando dados armazenados no MongoDB. 
O pipeline coleta métricas agregadas de documentos, tipos, dimensões, estudos e assuntos principais, e gera arquivos HTML formatados para cada país elegível.

## Funcionalidades Principais

- **Coleta de métricas agregadas:**  
  Para cada país com pelo menos um documento (`total_docs > 0`), o DAG coleta métricas de diferentes tipos (total de documentos, fulltext, tipos de documento, tipos de estudo, dimensões e assuntos principais) 
  a partir da coleção `02_countries_metrics` do banco `tmgl_metrics`.
- **Geração de relatórios HTML:**  
  Utiliza templates HTML customizados para criar relatórios visuais, organizando as métricas em tabelas e seções temáticas, com design responsivo e estilização baseada em Bootstrap.
- **Exportação automatizada:**  
  Os arquivos HTML são salvos em um diretório configurado via conexão Airflow (`TMGL_HTML_OUTPUT`), prontos para publicação ou distribuição.

## Estrutura do Pipeline

1. **generate_html_reports**  
   - Identifica países elegíveis (com pelo menos um documento).
   - Coleta métricas agregadas para cada país: total de documentos, total com fulltext, tipos de documento, tipos de estudo, dimensões e assuntos principais.
   - Gera o conteúdo HTML usando templates e salva o arquivo no diretório de saída configurado.

## Parâmetros e Conexões

- **MongoDB:**  
  Conexão definida via Airflow Connection `mongo`.
- **Diretório de saída:**  
  Definido via Airflow Connection `TMGL_HTML_OUTPUT`.
- **Coleção utilizada:**  
  - `02_countries_metrics` (métricas agregadas por país)

## Exemplo de Uso

1. Certifique-se de que as conexões e coleções do MongoDB estejam corretamente configuradas.
2. Execute o DAG `TMGL_07_export_html_reports` via interface do Airflow para gerar e exportar os relatórios HTML por país.

## Observações

- As funções dependem da estrutura padronizada das métricas na coleção `02_countries_metrics`.

## Dependências

- Apache Airflow
- pymongo
- MongoDB

"""


import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TMGL Metrics</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Roboto+Serif:ital,opsz,wght@0,8..144,100..900;1,8..144,100..900&family=Roboto:ital,wght@0,100..900;1,100..900&display=swap" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
*{{
font-family: "Roboto";
}}
.title{{
font-family: "Roboto Serif", serif;
font-size: 1.8rem;
text-align: center;
font-weight: bold;
}}
.hn{{
font-size: 3rem;
font-weight: bold;
}}
.label-1, table{{
font-size: 1.4rem;
}}
thead th{{
background: #0093D5!important;
color: #fff!important;
}}
</style>
</head>
<body>
<div class="container my-4">
<div class="row">
<div class="col-12">
<h5 class="title">Total Publications and Full-Text Availability</h5>
</div>
<div class="col-12 col-md-6 text-center">
<span class="label-1"><b>Total Documents</b><br></span>
<div class="hn">{total_docs}</div>
</div>
<div class="col-12 col-md-6 text-center">
<span class="label-1"><b>Full Text</b></span> <br>
<div class="hn">{total_fulltext}</div>
</div>

<div class="col-12">
<div id="table2" class="mt-5">
<h5 class="mb-3 text-center title">Document Types</h5>
<div class="table-responsive">
<table class="table table-hover text-center">
<thead class="table-primary">
<tr><th>Type</th><th>Total</th></tr>
</thead>
<tbody>
{doc_types_table}
</tbody>
</table>
</div>
</div>
</div>

{study_types_section}

{dimention_types_section}

{subject_types_section}

<p>Update: {today}</p>

</div>
</div>

<script>
  function sendHeight() {{
    const height = document.body.scrollHeight || document.documentElement.scrollHeight;
    parent.postMessage({{ type: "resize", height }}, "*");
  }}

  window.addEventListener("load", sendHeight);
  window.addEventListener("resize", sendHeight);
  new MutationObserver(sendHeight).observe(document.body, {{ childList: true, subtree: true }});
</script>
</body>
</html>
"""

STUDY_TYPES_TEMPLATE = """
<div id="table3" class="col-12 mt-5">
<h5 class="mb-3 text-center title">Most frequent studies</h5>
<div class="table-responsive">
<table class="table table-hover text-center">
<thead class="table-primary">
<tr><th>Study Type</th><th>Total</th></tr>
</thead>
<tbody>
{study_types_table}
</tbody>
</table>
</div>
</div>
"""

DIMENTION_TYPES_TEMPLATE = """
<div id="table4" class="col-12 mt-5">
<h5 class="mb-3 text-center title">Traditional Medicine Dimensions</h5>
<div class="table-responsive">
<table class="table table-hover text-center">
<thead class="table-primary">
<tr><th>Dimension</th><th>Total</th></tr>
</thead>
<tbody>
{dimention_types_table}
</tbody>
</table>
</div>
</div>
"""

SUBJECT_TYPES_TEMPLATE = """
<div id="table5" class="col-12 mt-5">
  <h5 class="mb-3 text-center title">Main Subjects</h5>
  <div class="table-responsive">
    <table class="table table-hover text-center">
      <thead class="table-primary">
        <tr>
          <th>Subject</th>
          <th>Total</th>
        </tr>
      </thead>
      <tbody>
        {subject_types_table}
      </tbody>
    </table>
  </div>
</div>
"""


def generate_table_rows(data):
    """Gera linhas da tabela HTML a partir dos dados do MongoDB"""
    sorted_data = sorted(data, key=lambda x: x['count'], reverse=True)
    return '\n'.join([f'<tr><td>{item["name"]}</td><td>{item["count"]}</td></tr>' for item in sorted_data])


def generate_html_reports():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_countries_metrics', 'tmgl_metrics')

    # Configuração do diretório de saída
    fs_hook = FSHook(fs_conn_id='TMGL_HTML_OUTPUT')
    output_dir = fs_hook.get_path()

    # Países elegíveis (com total_docs > 0)
    eligible_countries = collection.find(
        {'type': 'total_docs', 'count': {'$gt': 0}},
        {'country': 1}
    ).distinct('country')

    for country in eligible_countries:
        logger.info(f"Gerando HTML - {country}")

        # Coletar métricas
        metrics = {
            'total_docs': collection.find_one(
                {'country': country, 'type': 'total_docs'},
                {'_id': 0, 'count': 1}
            ) or {'count': 0},

            'total_fulltext': collection.find_one(
                {'country': country, 'type': 'total_docs_fulltext'},
                {'_id': 0, 'count': 1}
            ) or {'count': 0},

            'doc_types': list(collection.find(
                {'country': country, 'type': 'doc_type'},
                {'_id': 0, 'name': 1, 'count': 1}
            )),

            'study_types': list(collection.find(
                {'country': country, 'type': 'study_type'},
                {'_id': 0, 'name': 1, 'count': 1}
            )),

            'dimention_types': list(collection.find(
                {'country': country, 'type': 'dimention_type'},
                {'_id': 0, 'name': 1, 'count': 1}
            )),

            'subject_types': list(collection.find(
                {'country': country, 'type': 'subject_type'},
                {'_id': 0, 'name': 1, 'count': 1}
            )),
        }

        # Gerar conteúdo HTML para a seção de tipos de estudo
        if metrics['study_types']:
            study_types_table = generate_table_rows(metrics['study_types'])
            study_types_section = STUDY_TYPES_TEMPLATE.format(study_types_table=study_types_table)
        else:
            study_types_section = ""

        # Gerar conteúdo HTML para a seção de dimensões
        if metrics['dimention_types']:
            dimention_types_table = generate_table_rows(metrics['dimention_types'])
            dimention_types_section = DIMENTION_TYPES_TEMPLATE.format(
                dimention_types_table=dimention_types_table
            )
        else:
            dimention_types_section = ""

        # Seção de subject_types
        if metrics['subject_types']:
            subject_types_table = generate_table_rows(metrics['subject_types'])
            subject_types_section = SUBJECT_TYPES_TEMPLATE.format(subject_types_table=subject_types_table)
        else:
            subject_types_section = ""

        # Gerar conteúdo HTML principal
        html_content = HTML_TEMPLATE.format(
            total_docs=metrics['total_docs'].get('count', 0),
            total_fulltext=metrics['total_fulltext'].get('count', 0),
            doc_types_table=generate_table_rows(metrics['doc_types']),
            study_types_section=study_types_section,
            dimention_types_section=dimention_types_section,
            subject_types_section=subject_types_section,
            today=datetime.now().strftime('%d/%m/%Y')
        )

        # Salvar arquivo
        filename = f"{country.lower().replace(' ', '_')}.html"
        output_path = os.path.join(output_dir, filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'TMGL_07_export_html_reports',
    default_args=default_args,
    description='TMGL - Exporta relatórios HTML com métricas de documentos por país',
    schedule_interval=None,
    catchup=False,
    tags=['tmgl', 'reports', 'html']
) as dag:
    generate_reports_task = PythonOperator(
        task_id='generate_html_reports',
        python_callable=generate_html_reports
    )