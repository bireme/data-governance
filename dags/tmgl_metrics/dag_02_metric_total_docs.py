"""
# TMGL - TMGL_02_create_metric_total_docs

## Visão Geral

Este DAG calcula e armazena métricas de total de documentos por país para o projeto TMGL, 
utilizando dados armazenados no MongoDB. Ele gera duas métricas principais: o total geral de documentos por país e o total de documentos que possuem o campo `fulltext` marcado.

## Funcionamento

1. **Configuração da Coleção de Métricas**
   - Garante que a coleção `tmgl_metrics` no banco `02_countries_metrics` exista e possua um índice único nos campos `type`, `country` e `name`.

2. **Cálculo do Total de Documentos por País**
   - Para cada país listado na coleção `00_countries.tmgl_metrics`, executa uma contagem de documentos na coleção `01_landing_zone.tmgl_metrics` que estejam associados ao país, 
   considerando diferentes campos (`pais_afiliacao`, `cp`, `who_regions`) com busca case insensitive via regex.
   - O resultado é salvo na coleção de métricas, com o tipo `total_docs`.

3. **Cálculo do Total de Documentos com Fulltext por País**
   - Similar ao passo anterior, mas considera apenas documentos onde o campo `fulltext` é igual a "1".
   - O resultado é salvo na coleção de métricas, com o tipo `total_docs_fulltext`.

4. **Execução em Lote**
   - As operações de atualização/inserção são feitas em lote (bulk) para maior eficiência.

## Coleções Utilizadas

- **00_countries.tmgl_metrics**: Lista de países a serem considerados na métrica.
- **01_landing_zone.tmgl_metrics**: Fonte dos documentos a serem contados.
- **02_countries_metrics.tmgl_metrics**: Destino das métricas geradas.

## Tarefas do DAG

- `setup_collection_metric`: Prepara a coleção de métricas e cria índices necessários.
- `create_metric_total_docs`: Calcula o total de documentos por país.
- `create_metric_total_docs_fulltext`: Calcula o total de documentos com fulltext por país.

## Dependências

- **MongoDB**: Armazenamento dos dados e métricas.
"""

import re
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def setup_collection_metric():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    target_collection = mongo_hook.get_collection('02_countries_metrics', 'tmgl_metrics')
    target_collection.create_index([('type', 1), ('country', 1), ('name', 1)], unique=True)


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
        escaped_country = re.escape(country_name)
        
        # Construir query com regex case-insensitive
        query = {
            "$or": [
                {"pais_afiliacao": {"$regex": f"\\^i.*{escaped_country}.*\\^", "$options": "i"}},
                {"cp": {"$regex": f"^{escaped_country}$", "$options": "i"}},
                {"who_regions": {"$regex": f"/{escaped_country}$", "$options": "i"}}
            ]
        }
        
        # Contagem eficiente usando aggregation pipeline
        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$id"}},  # Distinct grouping
            {"$count": "total"}
        ]
        
        count_result = list(source_collection.aggregate(pipeline))
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
        escaped_country = re.escape(country_name)
        
        # Construir query com regex case-insensitive
        query = {
            "fulltext": "1",
            "$or": [
                {"pais_afiliacao": {"$regex": f"\\^i.*{escaped_country}.*\\^", "$options": "i"}},
                {"cp": {"$regex": f"^{escaped_country}$", "$options": "i"}},
                {"who_regions": {"$regex": f"/{escaped_country}$", "$options": "i"}}
            ]
        }
        
        # Contagem eficiente usando aggregation pipeline
        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$id"}},  # Distinct grouping
            {"$count": "total"}
        ]
        
        count_result = list(source_collection.aggregate(pipeline))
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
    setup_collection_task = PythonOperator(
        task_id='setup_collection_metric',
        python_callable=setup_collection_metric
    )

    create_metric_total_docs = PythonOperator(
        task_id='create_metric_total_docs',
        python_callable=create_metric_total_docs
    )

    create_metric_total_docs_fulltext = PythonOperator(
        task_id='create_metric_total_docs_fulltext',
        python_callable=create_metric_total_docs_fulltext
    )

    setup_collection_task >> create_metric_total_docs
    setup_collection_task >> create_metric_total_docs_fulltext