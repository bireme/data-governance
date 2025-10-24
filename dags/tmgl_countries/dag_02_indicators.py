import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_countries.misc import get_eligible_countries


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


def create_timeline(country):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')
    
    batch = []
    query = get_tmgl_countries_query([country])
    logger.info(query)

    pipeline = [
        {"$match": query}
    ] + BASE_PIPELINE

    for result in source_collection.aggregate(pipeline):
        year = result["_id"]["year"]

        if year is None:
            continue

        logger.info(f"{country}, year={year}, total={result['total']}, fulltext={result['with_fulltext']}")

        batch.append(UpdateOne(
            {
                "type": "timeline",
                "country": country,
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
    'TMGL_COUNTRIES_02_create_timeline',
    description='TMGL COUNTRIES - Calcula o total de documentos por ano',
    tags=["tmgl", "metrics", "mongodb", "timeline", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    get_eligible_countries_task = PythonOperator(
        task_id='get_eligible_countries',
        python_callable=get_eligible_countries
    )
    create_timeline_task = PythonOperator.partial(
        task_id='create_timeline',
        python_callable=create_timeline
    ).expand(op_args=get_eligible_countries_task.output)


    get_eligible_countries_task >> create_timeline_task