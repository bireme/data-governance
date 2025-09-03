import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_regions.misc import get_regions
from data_governance.dags.tmgl_regions.misc import get_country_data


def create_map():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')
    
    who_region_collection = mongo_hook.get_collection('who_region', 'TABS')
    regions = get_regions(who_region_collection)
    logger.info(f"Regioes carregadas: {regions.keys()}")

    for region, countries in regions.items():
        for country in countries:
            batch = []

            country_data = get_country_data(who_region_collection, country)
            logger.info(f"{country}, {country_data}")
            country_name = country_data.get("pais_tmgl") or country_data.get("pais_en")
            country_iso = next((iso for iso in country_data.get("pais_sinonimo", []) if len(iso) == 2), None)
            logger.info(f"{country_name}, {country_iso}")
            
            query = get_tmgl_countries_query([country])

            pipeline = [
                {"$match": query},
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

            for result in source_collection.aggregate(pipeline):
                year = result["_id"]["year"]

                if year is None:
                    continue

                logger.info(f"{country_iso}, year={year}, total={result['total']}, fulltext={result['with_fulltext']}")

                batch.append(UpdateOne(
                    {
                        "type": "map",
                        "region": region,
                        "country_name": country_name,
                        "country_iso": country_iso,
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
    'TMGL_REGION_02_create_map',
    description='TMGL REGION - Calcula o total de documentos por ano para o mapa mundi',
    tags=["tmgl", "metrics", "mongodb", "map", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_map_task = PythonOperator(
        task_id='create_map',
        python_callable=create_map
    )
