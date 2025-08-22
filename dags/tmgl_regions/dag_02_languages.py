import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_country_query_no_subject


def create_metric_languages():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    countries_collection = mongo_hook.get_collection('00_countries', 'tmgl_metrics')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics', 'tmgl_charts')
    
    # Obter lista de países
    countries = list(countries_collection.find({}, {'name': 1}).distinct('name'))
    
    for country in countries:
        batch = []
        country_name = country.strip().lower()
        query = get_tmgl_country_query_no_subject(country_name)
        
        # Agrupa por idioma + ano extraído de dp
        pipeline = [
            {"$match": query},
            {"$unwind": "$la"},
            {"$addFields": {
                "year": {
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
                }
            }},
            {"$group": {
                "_id": {
                    "language": {"$toLower": "$la"},
                    "year": "$year"
                },
                "count": {"$sum": 1}
            }}
        ]
        
        # Processa cada idioma/ano retornado para o país
        for result in source_collection.aggregate(pipeline):
            lang = result["_id"]["language"]
            year = result["_id"]["year"]
            logger.info(f"{country_name}, {lang}, {year}")
            
            # Ignora se não conseguiu extrair ano
            if year is None:
                continue
            
            batch.append(UpdateOne(
                {
                    "type": "language",
                    "country": country_name,
                    "name": lang,
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
    'TMGL_REGION_03_create_metric_languages',
    description='TMGL REGION - Calcula o total de documentos por idioma e ano',
    tags=["tmgl", "metrics", "mongodb", "language", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_languages_task = PythonOperator(
        task_id='create_metric_languages',
        python_callable=create_metric_languages
    )
