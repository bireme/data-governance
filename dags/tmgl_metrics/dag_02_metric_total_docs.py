"""
# DAG TMGL_02_create_metric_total_docs

Este módulo define um DAG do Apache Airflow responsável pelo cálculo e armazenamento do total de documentos TMGL por país. 
O pipeline executa duas métricas principais: a contagem total de documentos por país e a contagem total de documentos por país considerando critérios de fulltext, armazenando os resultados em uma coleção dedicada de métricas.

## Funcionalidades Principais

- **Cálculo de métricas por país:**  
  Para cada país listado na coleção `00_countries`, o DAG calcula o total de documentos distintos presentes na coleção `01_landing_zone`, agrupando por país e armazenando o resultado na coleção `02_countries_metrics`.
- **Atualização:**  
  Os resultados são armazenados via operações `UpdateOne` com `upsert=True`, garantindo que as métricas sejam atualizadas sem duplicidade.

## Estrutura do Pipeline

1. **create_metric_total_docs**  
   - Calcula o total de documentos distintos por país na coleção `01_landing_zone`.
   - Armazena o resultado na coleção `02_countries_metrics` com o tipo `"total_docs"`.

2. **create_metric_total_docs_fulltext**  
   - Calcula o total de documentos distintos por país considerando critérios de fulltext.
   - Armazena o resultado na coleção `02_countries_metrics` com o tipo `"total_docs_fulltext"`.

## Parâmetros e Conexões

- **MongoDB:**  
  Conexão definida via Airflow Connection `mongo`.
- **Coleções utilizadas:**  
  - `00_countries` (lista de países)
  - `01_landing_zone` (documentos TMGL)
  - `02_countries_metrics` (resultados das métricas)

## Exemplo de Uso

1. Certifique-se de que as conexões e coleções do MongoDB estejam corretamente configuradas.
2. Execute o DAG `TMGL_02_create_metric_total_docs` via interface do Airflow para atualizar as métricas de documentos por país.

## Observações

- As funções dependem do helper `get_tmgl_country_query` para construir as queries de filtragem por país.

## Dependências

- Apache Airflow
- pymongo
- MongoDB
"""


from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_country_query


def create_metric_total_docs():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    countries_collection = mongo_hook.get_collection('00_countries', 'tmgl_metrics')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_countries_metrics', 'tmgl_metrics')
    
    # Obter lista de países
    countries = list(countries_collection.find(
        {},
        {'name': 1}
    ).distinct('name'))
    
    batch = []
    for country in countries:
        country_name = country.strip().lower()
        query = get_tmgl_country_query(country_name)
        
        # Contagem eficiente usando aggregation pipeline
        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$id"}},  # Distinct grouping
            {"$count": "total"}
        ]
        
        count_result = list(source_collection.aggregate(pipeline, collation={'locale': 'en', 'strength': 1}))
        count = count_result[0]['total'] if count_result else 0
        
        batch.append(UpdateOne(
            {"type": "total_docs", "country": country_name},
            {"$set": {
                "count": count,
                "timestamp": datetime.now()
            }},
            upsert=True
        ))
    
    # Armazenar resultados
    if batch:
        target_collection.bulk_write(batch, ordered=False)


def create_metric_total_docs_fulltext():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    countries_collection = mongo_hook.get_collection('00_countries', 'tmgl_metrics')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_countries_metrics', 'tmgl_metrics')
    
    # Obter lista de países
    countries = list(countries_collection.find(
        {},
        {'name': 1}
    ).distinct('name'))
    
    batch = []
    for country in countries:
        country_name = country.strip().lower()
        query = get_tmgl_country_query(country_name)
        
        # Contagem eficiente usando aggregation pipeline
        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$id"}},  # Distinct grouping
            {"$count": "total"}
        ]
        
        count_result = list(source_collection.aggregate(pipeline, collation={'locale': 'en', 'strength': 1}))
        count = count_result[0]['total'] if count_result else 0
        
        batch.append(UpdateOne(
            {"type": "total_docs_fulltext", "country": country_name},
            {"$set": {
                "count": count,
                "timestamp": datetime.now()
            }},
            upsert=True
        ))
    
    # Armazenar resultados
    if batch:
        target_collection.bulk_write(batch, ordered=False)


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'TMGL_02_create_metric_total_docs',
    description='TMGL - Calcula o total de documentos por países',
    tags=["tmgl", "metrics", "mongodb", "total_docs"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_total_docs = PythonOperator(
        task_id='create_metric_total_docs',
        python_callable=create_metric_total_docs
    )

    create_metric_total_docs_fulltext = PythonOperator(
        task_id='create_metric_total_docs_fulltext',
        python_callable=create_metric_total_docs_fulltext
    )