"""
# DAG TMGL_06_create_metric_main_subjects

Este módulo define um DAG do Apache Airflow responsável pelo cálculo e armazenamento dos principais assuntos (descritores DECS) por país. 
O pipeline identifica, para cada país, os 10 assuntos mais frequentes nos documentos TMGL, mapeando os códigos DECS para descritores em inglês e armazenando os resultados em uma coleção dedicada de métricas.

## Funcionalidades Principais

- **Mapeamento de descritores DECS:**  
  Carrega o mapeamento de códigos DECS para descritores em inglês a partir da coleção `current` do banco `DECS`, garantindo que os resultados sejam apresentados de forma legível e padronizada.
- **Cálculo dos principais assuntos por país:**  
  Para cada país listado na coleção `00_countries`, o DAG executa um pipeline de agregação no MongoDB para identificar os 10 assuntos principais (descritores DECS) mais frequentes nos documentos da coleção `01_landing_zone`.
- **Execução eficiente:**  
  Utiliza unwinding, regex e grouping no pipeline de agregação para extrair e contar os descritores de forma eficiente.
- **Atualização:**  
  Os resultados são armazenados via operações `UpdateOne` com `upsert=True`, garantindo atualização sem duplicidade.

## Estrutura do Pipeline

1. **load_descriptors**  
   - Carrega o mapeamento de códigos DECS para descritores em inglês a partir da coleção `current` do banco `DECS`.

2. **create_metric_subjects**  
   - Para cada país, identifica os 10 assuntos principais (descritores DECS) mais frequentes nos documentos TMGL.
   - Mapeia os códigos DECS para descritores em inglês.
   - Armazena o resultado na coleção `02_countries_metrics` com o tipo `"subject_type"`.

## Parâmetros e Conexões

- **MongoDB:**  
  Conexão definida via Airflow Connection `mongo`.
- **Coleções utilizadas:**  
  - `00_countries` (lista de países)
  - `01_landing_zone` (documentos TMGL)
  - `02_countries_metrics` (resultados das métricas)
  - `current` do banco `DECS` (descritores DECS)

## Exemplo de Uso

1. Certifique-se de que as conexões e coleções do MongoDB estejam corretamente configuradas.
2. Execute o DAG `TMGL_06_create_metric_main_subjects` via interface do Airflow para atualizar as métricas dos principais assuntos por país.

## Observações

- As funções dependem do helper `get_tmgl_country_query` para construir as queries de filtragem por país.
- O mapeamento DECS garante que os resultados sejam apresentados de forma legível e padronizada.

## Dependências

- Apache Airflow
- pymongo
- MongoDB

"""


from datetime import datetime
from pymongo import UpdateOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_metrics.misc import get_tmgl_country_query


def load_descriptors(decs_col):
    """Mapeia códigos DECS para descritores em inglês"""
    descriptor_map = {}
    for decs_doc in decs_col.find():
        raw_mfn = decs_doc.get('Mfn', '').lstrip('0')
        english_desc = decs_doc.get('Descritor Inglês', '').strip()
        if raw_mfn and english_desc:
            descriptor_map[raw_mfn] = english_desc
    return descriptor_map


def create_metric_subjects():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    countries_collection = mongo_hook.get_collection('00_countries', 'tmgl_metrics')
    source_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection = mongo_hook.get_collection('02_countries_metrics', 'tmgl_metrics')
    decs_col = mongo_hook.get_collection('current', 'DECS')

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
        query = get_tmgl_country_query(country_name)

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