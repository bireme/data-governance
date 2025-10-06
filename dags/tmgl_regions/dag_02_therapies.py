import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_regions.misc import get_regions


BASE_PIPELINE = [
    {"$match": {"tag_mtc_tema3": {"$exists": True, "$ne": None}}},
    {"$unwind": "$tag_mtc_tema3"},
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
        "therapy": {
            "$getField": {
                "field": "match",
                "input": {
                    "$regexFind": {
                        "input": "$tag_mtc_tema3",
                        "regex": r"(.+\/.+)"
                    }
                }
            }
        }
    }},
    # Filtra apenas anos encontrados e maiores ou iguais a 1500
    {"$match": {"year": {"$gte": 1500}, "therapy": {"$exists": True, "$ne": None}}},
    {"$group": {
        "_id": {
            "therapy": "$therapy",
            "year": "$year"
        },
        "count": {"$sum": 1}
    }}
]


def create_metric_therapies():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')
    
    batch = []
    pipeline = BASE_PIPELINE
    
    # Processa cada idioma/ano retornado para o país
    for result in source_collection.aggregate(pipeline):
        therapy = result["_id"]["therapy"]

        year = result["_id"]["year"]
        logger.info(f"{therapy}, {year}")

        # Ignora se não conseguiu extrair ano
        if year is None:
            continue
        
        batch.append(UpdateOne(
            {
                "type": "therapy",
                "region": None,
                "name": therapy,
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


def create_metric_therapies_region():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')
    
    who_region_collection = mongo_hook.get_collection('who_region', 'TABS')
    regions = get_regions(who_region_collection)
    logger.info(f"Regioes carregadas: {regions.keys()}")

    for region, countries in regions.items():
        batch = []
        query = get_tmgl_countries_query(countries)
        logger.info(query)

        # Agrupa por idioma + ano extraído de dp
        pipeline = [
            {"$match": query}
        ] + BASE_PIPELINE
        
        # Processa cada idioma/ano retornado para o país
        for result in source_collection.aggregate(pipeline):
            therapy = result["_id"]["therapy"]

            year = result["_id"]["year"]
            logger.info(f"{region}, {therapy}, {year}")
            
            # Ignora se não conseguiu extrair ano
            if year is None:
                continue
            
            batch.append(UpdateOne(
                {
                    "type": "therapy",
                    "region": region,
                    "name": therapy,
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
    'TMGL_REGION_02_create_metric_therapies',
    description='TMGL REGION - Calcula o total de documentos por terapia e ano',
    tags=["tmgl", "metrics", "mongodb", "therapy", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_therapies_task = PythonOperator(
        task_id='create_metric_therapies',
        python_callable=create_metric_therapies
    )
    create_metric_therapies_region_task = PythonOperator(
        task_id='create_metric_therapies_region',
        python_callable=create_metric_therapies_region
    )
