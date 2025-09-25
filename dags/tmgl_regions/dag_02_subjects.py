import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_regions.misc import get_regions


BASE_PIPELINE = [
    {"$match": {"mj": {"$exists": True, "$ne": None}}},
    {"$unwind": "$mj"},
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
        "subject_id": {
            "$arrayElemAt": [
                {
                    "$getField": {
                        "field": "captures",
                        "input": {
                            "$regexFind": {
                                "input": "$mj",
                                "regex": r"(\d+)"
                            }
                        }
                    }
                },
                0
            ]
        }
    }},
    # Filtra apenas anos encontrados e maiores ou iguais a 1500
    {"$match": {"year": {"$gte": 1500}, "subject_id": {"$exists": True, "$ne": None}}},
    {"$group": {
        "_id": {
            "subject_id": "$subject_id",
            "year": "$year"
        },
        "count": {"$sum": 1}
    }}
]


def load_descriptors(decs_col):
    """Mapeia códigos DECS para descritores em inglês"""
    descriptor_map = {}
    for decs_doc in decs_col.find():
        raw_mfn = decs_doc.get('Mfn', '').lstrip('0')
        english_desc = decs_doc.get('Descritor Inglês', '').strip()
        if raw_mfn and english_desc:
            descriptor_map[raw_mfn] = english_desc
    return descriptor_map


def create_metric_subjects():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    # Carregar mapeamento DECS
    decs_col = mongo_hook.get_collection('current', 'DECS')
    descriptor_map = load_descriptors(decs_col)
    
    batch = []
    pipeline = BASE_PIPELINE
    
    # Processa cada idioma/ano retornado para o país
    for result in source_collection.aggregate(pipeline):
        subject_id = result["_id"]["subject_id"]
        subject = descriptor_map.get(subject_id, f"{subject_id}")

        year = result["_id"]["year"]
        logger.info(f"{subject}, {year}")
        
        # Ignora se não conseguiu extrair ano
        if year is None:
            continue
        
        batch.append(UpdateOne(
            {
                "type": "subject",
                "region": None,
                "name": subject,
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


def create_metric_subjects_region():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    # Carregar mapeamento DECS
    decs_col = mongo_hook.get_collection('current', 'DECS')
    descriptor_map = load_descriptors(decs_col)
    
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
            subject_id = result["_id"]["subject_id"]
            subject = descriptor_map.get(subject_id, f"{subject_id}")

            year = result["_id"]["year"]
            logger.info(f"{region}, {subject}, {year}")
            
            # Ignora se não conseguiu extrair ano
            if year is None:
                continue
            
            batch.append(UpdateOne(
                {
                    "type": "subject",
                    "region": region,
                    "name": subject,
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
    'TMGL_REGION_02_create_metric_subjects',
    description='TMGL REGION - Calcula o total de documentos por assunto e ano',
    tags=["tmgl", "metrics", "mongodb", "subject", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_subjects_task = PythonOperator(
        task_id='create_metric_subjects',
        python_callable=create_metric_subjects
    )
    create_metric_subjects_region_task = PythonOperator(
        task_id='create_metric_subjects_region',
        python_callable=create_metric_subjects_region
    )
