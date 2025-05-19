"""
# TMGL - TMGL_05_dimentions

TBD

"""

import re
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


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
        escaped_country = re.escape(country_name)
        
        # Construir query com regex case-insensitive
        query = {
            "$or": [
                {"pais_afiliacao": {"$regex": f"\\^i.*{escaped_country}.*\\^", "$options": "i"}},
                {"cp": {"$regex": f"^{escaped_country}$", "$options": "i"}},
                {"who_regions": {"$regex": f"/{escaped_country}$", "$options": "i"}}
            ]
        }
        
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