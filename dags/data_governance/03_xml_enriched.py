"""
# DG_03_enrich_xml

Esta DAG realiza o enriquecimento da coleção XML utilizada pelo iah-X, adicionando campos estáticos e dinâmicos de instância e coleções temáticas, a partir de múltiplas fontes no MongoDB.

## Objetivos

- **Enriquecer a coleção `03_xml_enriched`** com campos estáticos e dinâmicos, facilitando consultas e segmentações no iah-X.
- **Adicionar informações de instância** provenientes de diversas coleções temáticas (`TEMAS_BVS`).
- **Padronizar campos de coleção** para diferentes recortes temáticos.

## Principais Etapas

1. **enrich_static_data**  
   - Copia o conteúdo da coleção `02_iahx_xml` para `03_xml_enriched`.
   - Adiciona campos estáticos como `instance` (lista de instâncias regionais e nacionais) e coleções temáticas (`collection_*`).

2. **enrich_instancia**  
   - Para cada coleção em `TEMAS_BVS`, busca os identificadores presentes em `03_xml_enriched`.
   - Atualiza o campo `instance` de cada documento, unificando as instâncias encontradas em todas as coleções.

## Campos Enriquecidos

| Campo                     | Descrição                                                     |
|---------------------------|---------------------------------------------------------------|
| instance                  | Lista de instâncias associadas ao documento                   |
| collection                | Nome da coleção internacional (ex: "01-internacional")        |
| collection_enfermeria     | Nome da coleção temática de enfermagem                        |
| collection_hanseniase     | Nome da coleção temática de hanseníase                        |
| collection_peru           | Nome da coleção temática do Peru                              |
| collection_nicaragua      | Nome da coleção temática da Nicarágua                         |
| collection_bolivia        | Nome da coleção temática da Bolívia                           |
| collection_cns-br         | Nome da coleção temática do CNS-BR                            |
| collection_guatemala      | Nome da coleção temática da Guatemala                         |
| collection_honduras       | Nome da coleção temática de Honduras                          |
| collection_costarica      | Nome da coleção temática da Costa Rica                        |

"""


import logging
from datetime import datetime
from itertools import islice
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo import UpdateOne


def enrich_static_data():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_col = mongo_hook.get_collection('02_iahx_xml', 'data_governance')
    target_col = mongo_hook.get_collection('03_xml_enriched', 'data_governance')
    
    # Copia a coleção original para a nova
    pipeline = [{"$out": "03_xml_enriched"}]
    source_col.aggregate(pipeline)
    
    # Define os novos campos
    instance_values = [
        "regional", "bvsespana", "paraguay", "panama",
        "bvsms", "peru", "nicaragua", "bolivia",
        "cns-br", "guatemala", "honduras", "costarica"
    ]
    
    collection_fields = {
        "collection": "01-internacional",
        "collection_enfermeria": "LILACS",
        "collection_hanseniase": "LILACS",
        "collection_peru": "LILACS",
        "collection_nicaragua": "LILACS",
        "collection_bolivia": "LILACS",
        "collection_cns-br": "LILACS",
        "collection_guatemala": "LILACS",
        "collection_honduras": "LILACS",
        "collection_costarica": "LILACS"
    }

    update_fields = {"instance": instance_values}
    update_fields.update(collection_fields)
    
    # Atualiza todos os documentos na nova coleção
    target_col.update_many(
        {},
        {"$set": update_fields}
    )


def enrich_instancia():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')

    # 1. Obter todos os id_pk da coleção origem
    id_pks = list(enriched_collection.distinct('id_pk'))
    id_pks = [str(x) for x in id_pks]
    
    # 2. Para cada coleção no TEMAS_BVS
    temas_bvs = mongo_client["TEMAS_BVS"]
    temas_collections = temas_bvs.list_collection_names()
    batch_size = 5000
    doc_map = {}

    for coll_name in temas_collections:
        coll = temas_bvs[coll_name]
        cursor = coll.find(
            {'id_isis': {'$in': list(id_pks)}},
            {'_id': 0, 'id_isis': 1, 'instancia': 1},
            batch_size=batch_size
        )

        for doc in cursor:
            key = int(doc['id_isis'])
            if key not in doc_map:
                doc_map[key] = {'instances': set(), 'collections': set()}
            doc_map[key]['instances'].add(doc['instancia'])
            doc_map[key]['collections'].add(coll_name)
            
            # 3. Enviar batch
            if len(doc_map) >= batch_size:
                logger.info("Enviando batch de %i da coleção %s para o MongoDB" % (batch_size, coll_name))
                _process_batch(enriched_collection, doc_map)
                doc_map.clear()

        # 4. Processar restante
        if doc_map:
            logger.info("Enviando batch de menos de %i da coleção %s para o MongoDB" % (batch_size, coll_name,))
            _process_batch(enriched_collection, doc_map)
            doc_map.clear()
    

def _process_batch(collection, doc_map):
    bulk_ops = []
    for id_pk, data in doc_map.items():
        set_fields = {
            'instance': {
                '$setUnion': [
                    {'$ifNull': ['$instance', []]},
                    {'$literal': list(data['instances'])}
                ]
            }
        }
        # Adiciona campos collection_<nome_colecao>: "LILACS"
        for coll_name in data['collections']:
            set_fields[f'collection_{coll_name}'] = "LILACS"

        bulk_ops.append(
            UpdateOne(
                {'id_pk': id_pk},
                [{'$set': set_fields}]
            )
        )
    if bulk_ops:
        collection.bulk_write(bulk_ops, ordered=False)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 15),
    'retries': 0
}
with DAG(
    dag_id='DG_03_enrich_xml',
    default_args=default_args,
    description='Data Governance - Transforma a coleção com nomenclatura de XML do iah-X com novos campos enriquecidos',
    tags=["data_governance", "fi-admin", "mongodb", "iahx", "data enrichment"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    enrich_static_data_task = PythonOperator(
        task_id='enrich_static_data',
        python_callable=enrich_static_data
    )

    enrich_instancia_task = PythonOperator(
        task_id='enrich_instancia',
        python_callable=enrich_instancia
    )