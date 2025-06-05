"""
# TMGL - TMGL_03_create_metric_doc_type

## Visão Geral

Este DAG calcula e armazena métricas de **tipos de documentos** por país para o projeto TMGL. 
Analisa a distribuição de diferentes categorias de documentos (artigos, teses, multimídia, etc.) associados a cada país.

## Funcionamento

1. **Coleta de Países**:
   - Obtém a lista de países da coleção `00_countries.tmgl_metrics`.

2. **Processamento por País**:
   - Para cada país, executa uma pipeline de agregação que:
     1. Filtra documentos associados ao país (usando campos: `pais_afiliacao`, `cp`, `who_regions`)
     2. Desagrupa (`$unwind`) o campo `type` (que pode ser array)
     3. Classifica os tipos em categorias pré-definidas usando `$switch`

3. **Mapeamento de Tipos**:
   - Agrupa tipos semelhantes em categorias:
     ```
     "article" → "Articles"
     "monography" → "Monograph"
     "thesis" → "Thesis"
     "non-conventional" → "Non-conventional"
     "video/audio/podcast" → "Multimedia"  # Agrupados
     ```

4. **Armazenamento**:
   - Salva as contagens na coleção `02_countries_metrics.tmgl_metrics` com:
     - Tipo: `doc_type`
     - País
     - Nome da categoria
     - Contagem
     - Timestamp

## Coleções Envolvidas

| Coleção                     | Banco              | Uso                                  |
|-----------------------------|--------------------|--------------------------------------|
| `tmgl_metrics`              | `00_countries`     | Lista de países para processar       |
| `tmgl_metrics`              | `01_landing_zone`  | Fonte dos documentos brutos          |
| `tmgl_metrics`              | `02_countries_metrics` | Destino das métricas calculadas |

## Tarefas

- `create_metric_doctypes`: Única tarefa que executa todo o fluxo de coleta, processamento e armazenamento.

## Dependências

- **MongoDB**: Armazenamento dos dados
"""

from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_country_query


def create_metric_doctypes():
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
        query = get_tmgl_country_query(country_name)
        
        # Group by document type and count
        pipeline = [
            {"$match": query},
            {"$unwind": "$type"},
            {"$group": {
                "_id": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$type", "article"]}, "then": "Articles"},
                            {"case": {"$eq": ["$type", "monography"]}, "then": "Monograph"},
                            {"case": {"$eq": ["$type", "thesis"]}, "then": "Thesis"},
                            {"case": {"$eq": ["$type", "non-conventional"]}, "then": "Non-conventional"},
                            {"case": {"$eq": ["$type", "project document"]}, "then": "Project document"},
                            {"case": {"$eq": ["$type", "congress and conference"]}, "then": "Congress and conference"},
                            {"case": {"$eq": ["$type", "video"]}, "then": "Multimedia"},
                            {"case": {"$eq": ["$type", "audio"]}, "then": "Multimedia"},
                            {"case": {"$eq": ["$type", "podcast"]}, "then": "Multimedia"}
                        ],
                        "default": "$type"
                    }
                },
                "count": {"$sum": 1}
            }}
        ]
        
        # Process document types for this country
        for doc_type in source_collection.aggregate(pipeline):
            batch.append(UpdateOne(
                {
                    "type": "doc_type",
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
    'TMGL_03_create_metric_doc_type',
    description='TMGL - Calcula o total de tipo de documentos por países',
    tags=["tmgl", "metrics", "mongodb", "doc_types"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    create_metric_doctypes_task = PythonOperator(
        task_id='create_metric_doctypes',
        python_callable=create_metric_doctypes
    )