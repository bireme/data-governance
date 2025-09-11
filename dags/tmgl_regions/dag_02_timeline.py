import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_regions.misc import get_regions


BASE_PIPELINE = [
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
                    "0"  # default se não encontrou ano
                ]
            }
        }
    }},
    {"$match": {"year": {"$gte": 1500}}},
    {"$group": {
        "_id": {
            "year": "$year"
        },
        "total": {"$sum": 1},
        "with_fulltext": {
            "$sum": {
                "$cond": [{"$eq": ["$fulltext", "1"]}, 1, 0]
            }
        }
    }}
]


def create_timeline():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')

    batch = []
    pipeline = BASE_PIPELINE

    for result in source_collection.aggregate(pipeline):
        year = result["_id"]["year"]

        if year is None:
            continue

        logger.info(f"year={year}, total={result['total']}, fulltext={result['with_fulltext']}")

        batch.append(UpdateOne(
            {
                "type": "timeline",
                "region": None,
                "year": year
            },
            {
                "$set": {
                    "total": result["total"],
                    "with_fulltext": result["with_fulltext"],
                    "timestamp": datetime.now()
                }
            },
            upsert=True
        ))

    if batch:
        target_collection.bulk_write(batch, ordered=False)


def create_timeline_region():
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

        pipeline = [
            {"$match": query}
        ] + BASE_PIPELINE

        for result in source_collection.aggregate(pipeline):
            year = result["_id"]["year"]

            if year is None:
                continue

            logger.info(f"{region}, year={year}, total={result['total']}, fulltext={result['with_fulltext']}")

            batch.append(UpdateOne(
                {
                    "type": "timeline",
                    "region": region,
                    "year": year
                },
                {
                    "$set": {
                        "total": result["total"],
                        "with_fulltext": result["with_fulltext"],
                        "timestamp": datetime.now()
                    }
                },
                upsert=True
            ))

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
    'TMGL_REGION_02_create_timeline',
    description='TMGL REGION - Calcula o total de documentos por ano',
    tags=["tmgl", "metrics", "mongodb", "timeline", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_timeline_task = PythonOperator(
        task_id='create_timeline',
        python_callable=create_timeline
    )
    create_timeline_region_task = PythonOperator(
        task_id='create_timeline_region',
        python_callable=create_timeline_region
    )
