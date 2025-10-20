import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_regions.misc import get_regions


BASE_PIPELINE = [
    {"$unwind": "$type"},
    {"$addFields": {
        "doctype": {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$type", "article"]}, "then": "Articles"},
                    {"case": {"$eq": ["$type", "monography"]}, "then": "Monograph"},
                    {"case": {"$eq": ["$type", "thesis"]}, "then": "Thesis"},
                    {"case": {"$eq": ["$type", "non-conventional"]}, "then": "Non-conventional"},
                    {"case": {"$eq": ["$type", "project document"]}, "then": "Project document"},
                    {"case": {"$eq": ["$type", "congress and conference"]}, "then": "Congress and conference"},
                    {"case": {"$eq": ["$type", "video"]}, "then": "Multimedia"},
                    {"case": {"$eq": ["$type", "audio"]}, "then": "Multimedia"},
                    {"case": {"$eq": ["$type", "podcast"]}, "then": "Multimedia"}
                ],
                "default": "$type"
            }
        }
    }},
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
        }
    }},
    # Filtra apenas anos encontrados e maiores ou iguais a 1500
    {"$match": {"year": {"$gte": 1500}}},
    {"$group": {
        "_id": {
            "doctype": "$doctype",
            "year": "$year"
        },
        "count": {"$sum": 1}
    }}
]


def create_metric_doctype():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')
    
    who_region_collection = mongo_hook.get_collection('who_region', 'TABS')
    regions = get_regions(who_region_collection)

    for region, countries in regions.items():
        for country in countries:
            batch = []
            query = get_tmgl_countries_query([country])
            logger.info(query)

            # Agrupa por idioma + ano extraído de dp
            pipeline = [
                {"$match": query}
            ] + BASE_PIPELINE
            
            # Processa cada idioma/ano retornado para o país
            for result in source_collection.aggregate(pipeline):
                doctype = result["_id"]["doctype"]
                year = result["_id"]["year"]
                logger.info(f"{country}, {doctype}, {year}")

                # Ignora se não conseguiu extrair ano
                if year is None:
                    continue
                
                batch.append(UpdateOne(
                    {
                        "type": "doctype",
                        "country": country,
                        "name": doctype,
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
    'TMGL_COUNTRIES_02_create_metric_doctype',
    description='TMGL COUNTRIES - Calcula o total de documentos por tipo e ano',
    tags=["tmgl", "metrics", "mongodb", "doctype", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_doctype_task = PythonOperator(
        task_id='create_metric_doctype',
        python_callable=create_metric_doctype
    )