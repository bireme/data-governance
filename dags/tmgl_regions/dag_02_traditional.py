import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_regions.misc import get_regions
from data_governance.dags.tmgl_regions.misc import load_areas


BASE_PIPELINE = [
    {"$match": {"traditional_medicines_cluster": {"$exists": True, "$ne": None}}},
    {"$unwind": "$traditional_medicines_cluster"},
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
            "name": "$traditional_medicines_cluster",
            "year": "$year"
        },
        "count": {"$sum": 1}
    }}
]


def create_metric_traditional():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    # Carregar mapeamento de Areas
    areas_col = mongo_hook.get_collection('tmgl_areas', 'TABS')
    areas_map = load_areas(areas_col)
    
    batch = []
    # Agrupa por idioma + ano extraído de dp
    pipeline = BASE_PIPELINE
    
    # Processa cada idioma/ano retornado para o país
    for result in source_collection.aggregate(pipeline):
        name_code = result["_id"]["name"]
        name = areas_map.get(name_code, name_code)
        
        year = result["_id"]["year"]
        logger.info(f"{name}, {year}")
        
        # Ignora se não conseguiu extrair ano
        if year is None:
            continue
        
        batch.append(UpdateOne(
            {
                "type": "traditional",
                "region": None,
                "name": name,
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


def create_metric_traditional_region():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    # Carregar mapeamento de Areas
    areas_col = mongo_hook.get_collection('tmgl_areas', 'TABS')
    areas_map = load_areas(areas_col)
    
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
            name_code = result["_id"]["name"]
            name = areas_map.get(name_code, name_code)

            year = result["_id"]["year"]
            logger.info(f"{region}, {name}, {year}")
            
            # Ignora se não conseguiu extrair ano
            if year is None:
                continue
            
            batch.append(UpdateOne(
                {
                    "type": "traditional",
                    "region": region,
                    "name": name,
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
    'TMGL_REGION_02_create_metric_traditional',
    description='TMGL REGION - Calcula o total de documentos por traditional medicine e ano',
    tags=["tmgl", "metrics", "mongodb", "traditional", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_traditional_task = PythonOperator(
        task_id='create_metric_traditional',
        python_callable=create_metric_traditional
    )
    create_metric_traditional_region_task = PythonOperator(
        task_id='create_metric_traditional_region',
        python_callable=create_metric_traditional_region
    )
