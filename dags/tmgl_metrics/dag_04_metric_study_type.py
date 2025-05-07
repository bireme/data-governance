"""
# TMGL - TMGL_04_create_metric_study_type

## Visão Geral

Este DAG calcula e armazena métricas de **tipos de estudo** por país para o projeto TMGL. 
Ele categoriza diferentes metodologias de pesquisa (revisões sistemáticas, ensaios clínicos, estudos observacionais, etc.) associadas a cada país.

## Funcionamento

1. **Coleta de Países**:
   - Obtém a lista de países da coleção `00_countries.tmgl_metrics`.

2. **Processamento por País**:
   - Para cada país, executa uma pipeline de agregação que:
     1. Filtra documentos associados ao país (campos: `pais_afiliacao`, `cp`, `who_regions`)
     2. Desagrupa (`$unwind`) o campo `type_of_study` (que pode ser array)
     3. Classifica os tipos em categorias padronizadas usando `$switch`

3. **Mapeamento de Tipos de Estudo**:
   - Agrupa 21 tipos originais em 7 categorias principais:
     ```
     Revisões Sistemáticas → "Systematic review"
     Diretrizes → "Practice guideline"
     Ensaios Clínicos → "Controlled Clinical Trials"
     Estudos Observacionais → "Observationals"
     Estudos Qualitativos → "Qualitatives"
     Outros Revisões → "Other Reviews"
     Demais Tipos → "Other studies"
     ```

4. **Armazenamento**:
   - Salva as contagens na coleção `02_countries_metrics.tmgl_metrics` com:
     - Tipo: `study_type`
     - País
     - Nome da categoria
     - Contagem
     - Timestamp

## Mapeamento Completo de Tipos

| Tipo Original                          | Categoria Final           |
|----------------------------------------|---------------------------|
| systematic_reviews                     | Systematic review         |
| systematic_review_of_observational_studies | Systematic review         |
| guideline                              | Practice guideline        |
| clinical_trials                        | Controlled Clinical Trials|
| observational_studies                  | Observationals            |
| sysrev_observational_studies           | Observationals            |
| qualitative_research                   | Qualitatives              |
| literature_review                      | Other Reviews             |
| review                                 | Other Reviews             |
| Demais 12 tipos                        | Other studies             |

## Coleções Envolvidas

| Coleção                     | Banco              | Uso                                  |
|-----------------------------|--------------------|--------------------------------------|
| `tmgl_metrics`              | `00_countries`     | Lista de países para processar       |
| `tmgl_metrics`              | `01_landing_zone`  | Fonte dos documentos brutos          |
| `tmgl_metrics`              | `02_countries_metrics` | Destino das métricas calculadas |

## Tarefas

- `create_metric_studytypes`: Única tarefa que executa todo o fluxo de coleta, processamento e armazenamento.

## Dependências

- **MongoDB**: Armazenamento dos dados

"""

import re
from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def create_metric_studytypes():
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
            {"$unwind": "$type_of_study"},
            {"$group": {
                "_id": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$type_of_study", "systematic_reviews"]}, "then": "Systematic review"},
                            {"case": {"$eq": ["$type_of_study", "systematic_review_of_observational_studies"]}, "then": "Systematic review"},
                            {"case": {"$eq": ["$type_of_study", "literature_review"]}, "then": "Other Reviews"},
                            {"case": {"$eq": ["$type_of_study", "review"]}, "then": "Other Reviews"},
                            {"case": {"$eq": ["$type_of_study", "guideline"]}, "then": "Practice guideline"},
                            {"case": {"$eq": ["$type_of_study", "clinical_trials"]}, "then": "Controlled Clinical Trials"},
                            {"case": {"$eq": ["$type_of_study", "qualitative_research"]}, "then": "Qualitatives"},
                            {"case": {"$eq": ["$type_of_study", "risk_factors_studies"]}, "then": "Risk factors"},
                            {"case": {"$eq": ["$type_of_study", "overview"]}, "then": "Overview"},
                            {"case": {"$eq": ["$type_of_study", "evidence_synthesis"]}, "then": "Evidence synthesis"},
                            {"case": {"$eq": ["$type_of_study", "observational_studies"]}, "then": "Observationals"},
                            {"case": {"$eq": ["$type_of_study", "sysrev_observational_studies"]}, "then": "Observationals"},
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
                },
                "count": {"$sum": 1}
            }}
        ]
        
        # Process document types for this country
        for doc_type in source_collection.aggregate(pipeline):
            batch.append(UpdateOne(
                {
                    "type": "study_type",
                    "country": country_name,
                    "name": doc_type["_id"]  # Use actual document type from grouping
                },
                {
                    "$set": {
                        "count": doc_type["count"],
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
    'TMGL_04_create_metric_study_type',
    description='TMGL - Calcula o total de tipos de estudo por países',
    tags=["tmgl", "metrics", "mongodb", "study_types"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_studytypes_task = PythonOperator(
        task_id='create_metric_studytypes',
        python_callable=create_metric_studytypes
    )