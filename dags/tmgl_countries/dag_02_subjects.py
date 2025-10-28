import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_countries.misc import get_eligible_countries


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


def create_metric_subjects(country):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')

    # Carregar mapeamento DECS
    decs_col = mongo_hook.get_collection('current', 'DECS')
    descriptor_map = load_descriptors(decs_col)
    
    batch = []
    query = get_tmgl_countries_query([country])
    logger.info(query)

    # Agrupa por idioma + ano extraído de dp
    pipeline = [
        {"$match": query}
    ] + BASE_PIPELINE
    
    # Processa cada idioma/ano retornado para o país
    for result in source_collection.aggregate(pipeline, batchSize=500):
        subject_id = result["_id"]["subject_id"]
        subject = descriptor_map.get(subject_id, f"{subject_id}")

        year = result["_id"]["year"]
        logger.info(f"{country}, {subject}, {year}")
        
        # Ignora se não conseguiu extrair ano
        if year is None:
            continue
        
        batch.append(UpdateOne(
            {
                "type": "subject",
                "country": country,
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

        if len(batch) >= 500:
            target_collection.bulk_write(batch, ordered=False)
            batch = []

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
    'TMGL_COUNTRIES_02_create_metric_subjects',
    description='TMGL COUNTRIES - Calcula o total de documentos por assunto e ano',
    tags=["tmgl", "metrics", "mongodb", "subject", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    get_eligible_countries_task = PythonOperator(
        task_id='get_eligible_countries',
        python_callable=get_eligible_countries
    )
    create_metric_subjects_task = PythonOperator.partial(
        task_id='create_metric_subjects',
        python_callable=create_metric_subjects
    ).expand(op_args=get_eligible_countries_task.output)


    get_eligible_countries_task >> create_metric_subjects_task