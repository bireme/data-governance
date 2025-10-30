import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_countries.misc import get_eligible_countries
from data_governance.dags.tmgl_regions.misc import load_areas


BASE_PIPELINE = [
    {"$match": {"tag_dimentions": {"$exists": True, "$ne": None}}},
    {"$unwind": "$tag_dimentions"},
    {"$addFields": {
        "year": {
            "$toInt": {
                "$ifNull": [
                    {
                        "$getField": {
                            "field": "match",
                            "input": {
                                "$regexFind": {
                                    "input": {
                                        "$cond": [
                                            {"$eq": [{"$type": "$dp"}, "string"]},
                                            "$dp",
                                            ""
                                        ]
                                    },
                                    "regex": r"\d{4}"
                                }
                            }
                        }
                    },
                    "0"  # valor default quando não encontra \d{4}
                ]
            }
        },
        "dimention": {
            "$getField": {
                "field": "match",
                "input": {
                    "$regexFind": {
                        "input": "$tag_dimentions",
                        "regex": r"(.+\/.+)"
                    }
                }
            }
        }
    }},
    # Filtra apenas anos encontrados e maiores ou iguais a 1500
    {"$match": {"year": {"$gte": 1500}, "dimention": {"$exists": True, "$ne": None}}},
    {"$group": {
        "_id": {
            "dimention": "$dimention",
            "year": "$year"
        },
        "count": {"$sum": 1}
    }}
]


def create_metric_dimentions(country):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')

    # Carregar mapeamento de Areas
    areas_col = mongo_hook.get_collection('tmgl_areas', 'TABS')
    areas_map = load_areas(areas_col)
    
    batch = []
    query = get_tmgl_countries_query([country])
    logger.info(query)

    # Agrupa por idioma + ano extraído de dp
    pipeline = [
        {"$match": query}
    ] + BASE_PIPELINE
    
    # Processa cada idioma/ano retornado para o país
    for result in source_collection.aggregate(pipeline):
        dimention_code = result["_id"]["dimention"]
        dimention = areas_map.get(dimention_code, dimention_code)

        year = result["_id"]["year"]
        logger.info(f"{country}, {dimention}, {year}")
        
        # Ignora se não conseguiu extrair ano
        if year is None:
            continue
        
        batch.append(UpdateOne(
            {
                "type": "dimention",
                "country": country,
                "name": dimention,
                "year": year
            },
            {
                "$set": {
                    "count": result["count"],
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
    'TMGL_COUNTRIES_02_create_metric_dimentions',
    description='TMGL COUNTRY - Calcula o total de documentos por país e ano',
    tags=["tmgl", "metrics", "mongodb", "dimention", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    get_eligible_countries_task = PythonOperator(
        task_id='get_eligible_countries',
        python_callable=get_eligible_countries
    )
    create_metric_dimentions_task = PythonOperator.partial(
        task_id='create_metric_dimentions',
        python_callable=create_metric_dimentions
    ).expand(op_args=get_eligible_countries_task.output)


    get_eligible_countries_task >> create_metric_dimentions_task