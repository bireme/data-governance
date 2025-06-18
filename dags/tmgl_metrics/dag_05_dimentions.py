"""
# DAG TMGL_05_create_metric_dimentions

Este módulo define um DAG do Apache Airflow responsável pelo cálculo e armazenamento do total de documentos TMGL por país, agrupados por dimensão temática. 
O pipeline executa a métrica de contagem de documentos por dimensão para cada país, armazenando os resultados em uma coleção dedicada de métricas.

## Funcionalidades Principais

- **Cálculo de métricas por dimensão e país:**  
  Para cada país listado na coleção `00_countries`, o DAG calcula o total de documentos distintos presentes na coleção `01_landing_zone`, agrupando por cada valor de dimensão temática (`tag_dimentions`). 
  Os nomes das dimensões são normalizados para descrições legíveis.
- **Execução eficiente:**  
  Utiliza pipelines de agregação do MongoDB com unwinding e grouping para garantir performance e precisão nas contagens.
- **Atualização:**  
  Os resultados são armazenados via operações `UpdateOne` com `upsert=True`, garantindo que as métricas sejam atualizadas sem duplicidade.

## Estrutura do Pipeline

1. **create_metric_dimentions**  
   - Para cada país, agrupa e conta os documentos por dimensão temática (`tag_dimentions`), normalizando os nomes das dimensões para descrições amigáveis.
   - Armazena o resultado na coleção `02_countries_metrics` com o tipo `"dimention_type"`.

## Parâmetros e Conexões

- **MongoDB:**  
  Conexão definida via Airflow Connection `mongo`.
- **Coleções utilizadas:**  
  - `00_countries` (lista de países)
  - `01_landing_zone` (documentos TMGL)
  - `02_countries_metrics` (resultados das métricas)

## Exemplo de Uso

1. Certifique-se de que as conexões e coleções do MongoDB estejam corretamente configuradas.
2. Execute o DAG `TMGL_05_create_metric_dimentions` via interface do Airflow para atualizar as métricas de documentos por dimensão e país.

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


def create_metric_dimentions():
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
        
        # Group by document type and count
        pipeline = [
            {"$match": query},
            {"$unwind": "$tag_dimentions"},
            {"$group": {
                "_id": {
                    "$let": {
                        "vars": {
                            "preSlash": {
                                "$arrayElemAt": [
                                    {"$split": ["$tag_dimentions", "/"]},
                                    0
                                ]
                            }
                        },
                        "in": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$eq": ["$$preSlash", "tm_for_daily_life"]},
                                        "then": "TM for Daily Life"
                                    },
                                    {
                                        "case": {"$eq": ["$$preSlash", "health_systems_services"]},
                                        "then": "Health Systems & Services"
                                    },
                                    {
                                        "case": {"$eq": ["$$preSlash", "biodiversity_sustainability"]},
                                        "then": "Biodiversity & Sustainability"
                                    },
                                    {
                                        "case": {"$eq": ["$$preSlash", "leadership_policies"]},
                                        "then": "Leadership & Policies"
                                    },
                                    {
                                        "case": {"$eq": ["$$preSlash", "rights_equity_ethics"]},
                                        "then": "Rights, Equity & Ethics"
                                    },
                                    {
                                        "case": {"$eq": ["$$preSlash", "digital_health_frontiers"]},
                                        "then": "Digital Health Frontiers"
                                    },
                                    {
                                        "case": {"$eq": ["$$preSlash", "health_and_well_being"]},
                                        "then": "Health and Well-Being"
                                    },
                                    {
                                        "case": {"$eq": ["$$preSlash", "research_and_evidence"]},
                                        "then": "Research and Evidence"
                                    }
                                ],
                                "default": "$$preSlash"  # Retorna o valor original sem "/"
                            }
                        }
                    }
                },
                "count": {"$sum": 1}
            }}
        ]
        
        # Process document types for this country
        for dimention in source_collection.aggregate(pipeline):
            batch.append(UpdateOne(
                {
                    "type": "dimention_type",
                    "country": country_name,
                    "name": dimention["_id"]  # Use actual document type from grouping
                },
                {
                    "$set": {
                        "count": dimention["count"],
                        "timestamp": datetime.now()
                    }
                },
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
    'TMGL_05_create_metric_dimentions',
    description='TMGL - Calcula o total de dimensões por países',
    tags=["tmgl", "metrics", "mongodb", "dimentions"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_dimentions_task = PythonOperator(
        task_id='create_metric_dimentions',
        python_callable=create_metric_dimentions
    )