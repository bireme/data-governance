"""
# TMGL - TMGL_05_export_html_reports

## Visão Geral
Este DAG gera relatórios HTML por país com métricas de documentos científicos, incluindo:
- Total de documentos e full-texts disponíveis
- Distribuição por tipos de documento
- Tipos de estudo mais frequentes
"""

import os
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
        <spam class="label-1"><b>Total Documents</b><br></spam>
        <div class="hn">{total_docs}</div>
      </div>
      <div class="col-12 col-md-6 text-center">
        <spam class="label-1"><b>Full Text</b></spam> <br>
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
    </div>
  </div>
</body>
</html>
"""


def generate_table_rows(data):
    """Gera linhas da tabela HTML a partir dos dados do MongoDB"""
    sorted_data = sorted(data, key=lambda x: x['count'], reverse=True)
    return '\n'.join([f'<tr><td>{item["name"]}</td><td>{item["count"]}</td></tr>' for item in sorted_data])

def generate_html_reports():
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
            ))
        }
        
        # Gerar conteúdo HTML
        html_content = HTML_TEMPLATE.format(
            total_docs=metrics['total_docs'].get('count', 0),
            total_fulltext=metrics['total_fulltext'].get('count', 0),
            doc_types_table=generate_table_rows(metrics['doc_types']),
            study_types_table=generate_table_rows(metrics['study_types'])
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
    'TMGL_05_export_html_reports',
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