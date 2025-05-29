"""
# TMGL - TMGL_06_subjects

TBD

"""

import re
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def load_descriptors(decs_col):
    """Mapeia códigos DECS para descritores em inglês"""
    descriptor_map = {}
    for decs_doc in decs_col.find():
        raw_mfn = decs_doc.get('Mfn', '').lstrip('0')
        english_desc = decs_doc.get('Descritor Inglês', '').strip()
        if raw_mfn and english_desc:
            descriptor_map[raw_mfn] = english_desc
    return descriptor_map


def clean_previous_records(target_col):
    """Remove todos os registros do tipo subject_type"""
    target_col.delete_many({"type": "subject_type"})


def create_metric_subjects():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    countries_collection = mongo_hook.get_collection('00_countries', 'tmgl_metrics')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_countries_metrics', 'tmgl_metrics')
    decs_col = mongo_hook.get_collection('current', 'DECS')
    
    # Limpar dados anteriores
    clean_previous_records(target_collection)

    # Carregar mapeamento DECS
    descriptor_map = load_descriptors(decs_col)

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

        pipeline = [
            {"$match": query},
            {"$unwind": "$mj"},
            {"$project": {
                "d_value": {"$regexFind": {"input": "$mj", "regex": r"\^d(\d+)"}}
            }},
            {"$match": {"d_value": {"$ne": None}}},
            {"$group": {
                "_id": {"$arrayElemAt": ["$d_value.captures", 0]},
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]

        top_descriptors = source_collection.aggregate(pipeline)
        
        for descriptor in top_descriptors:
            descriptor_id = descriptor["_id"].lstrip('0')
            english_desc = descriptor_map.get(descriptor_id, f"{descriptor_id}")

            batch.append(UpdateOne(
                {
                    "type": "subject_type",
                    "country": country_name,
                    "name": english_desc
                },
                {
                    "$set": {
                        "count": descriptor["count"],
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
    'TMGL_06_create_metric_main_subjects',
    description='TMGL - Calcula o total de assuntos principais por países',
    tags=["tmgl", "metrics", "mongodb", "subjects"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_subjects_task = PythonOperator(
        task_id='create_metric_subjects',
        python_callable=create_metric_subjects
    )