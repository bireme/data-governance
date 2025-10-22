import logging
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_countries_query
from data_governance.dags.tmgl_countries.misc import get_eligible_countries


BASE_PIPELINE = [
    {"$unwind": "$type_of_study"},
    {"$addFields": {
        "studytype": {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$type_of_study", "systematic_reviews"]}, "then": "Systematic review"},
                    {"case": {"$eq": ["$type_of_study", "systematic_review_of_observational_studies"]}, "then": "Systematic review"},
                    {"case": {"$eq": ["$type_of_study", "literature_review"]}, "then": "Other Reviews"},
                    {"case": {"$eq": ["$type_of_study", "review"]}, "then": "Other Reviews"},
                    {"case": {"$eq": ["$type_of_study", "guideline"]}, "then": "Practice guideline"},
                    {"case": {"$eq": ["$type_of_study", "clinical_trials"]}, "then": "Controlled Clinical Trials"},
                    {"case": {"$eq": ["$type_of_study", "qualitative_research"]}, "then": "Qualitative studies"},
                    {"case": {"$eq": ["$type_of_study", "risk_factors_studies"]}, "then": "Risk factors"},
                    {"case": {"$eq": ["$type_of_study", "overview"]}, "then": "Overview"},
                    {"case": {"$eq": ["$type_of_study", "evidence_synthesis"]}, "then": "Evidence synthesis"},
                    {"case": {"$eq": ["$type_of_study", "observational_studies"]}, "then": "Observational studies"},
                    {"case": {"$eq": ["$type_of_study", "sysrev_observational_studies"]}, "then": "Observational studies"},
                    {"case": {"$eq": ["$type_of_study", "policy_brief"]}, "then": "Policy brief"},
                    {"case": {"$eq": ["$type_of_study", "diagnostic_studies"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "etiology_studies"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "prognostic_studies"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "prevalence_studies"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "screening_studies"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "incidence_studies"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "health_technology_assessment"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "health_economic_evaluation"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "evaluation_studies"]}, "then": "Other studies"},
                    {"case": {"$eq": ["$type_of_study", "overview_evidence_synthesis"]}, "then": "Other studies"}
                ],
                "default": "$type_of_study"
            }
        }
    }},
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
            "studytype": "$studytype",
            "year": "$year"
        },
        "count": {"$sum": 1}
    }}
]


def create_metric_studytype(country):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')
    
    batch = []
    query = get_tmgl_countries_query([country])
    logger.info(query)

    # Agrupa por idioma + ano extraído de dp
    pipeline = [
        {"$match": query}
    ] + BASE_PIPELINE
    
    # Processa cada idioma/ano retornado para o país
    for result in source_collection.aggregate(pipeline):
        studytype = result["_id"]["studytype"]
        year = result["_id"]["year"]
        logger.info(f"{country}, {studytype}, {year}")

        # Ignora se não conseguiu extrair ano
        if year is None:
            continue
        
        batch.append(UpdateOne(
            {
                "type": "studytype",
                "country": country,
                "name": studytype,
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
    'TMGL_COUNTRIES_02_create_metric_studytype',
    description='TMGL COUNTRIES - Calcula o total de documentos por tipo de estudo e ano',
    tags=["tmgl", "metrics", "mongodb", "studytype", "year"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    get_eligible_countries_task = PythonOperator(
        task_id='get_eligible_countries',
        python_callable=get_eligible_countries
    )
    create_metric_studytype_task = PythonOperator.partial(
        task_id='create_metric_studytype',
        python_callable=create_metric_studytype
    ).expand(op_args=get_eligible_countries_task.output)


    get_eligible_countries_task >> create_metric_studytype_task