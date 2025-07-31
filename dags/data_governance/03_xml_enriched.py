"""
# DG_03_enrich_xml

Esta DAG realiza o enriquecimento da coleção XML utilizada pelo iah-X, adicionando campos estáticos e dinâmicos de instância e coleções temáticas a partir de múltiplas fontes no MongoDB.

---

## Objetivos

- **Enriquecer a coleção `03_xml_enriched`** com campos estáticos e dinâmicos, facilitando consultas e segmentações no iah-X.
- **Adicionar informações de instância e coleções temáticas** provenientes de diversas coleções do MongoDB (TEMAS_BVS).
- **Padronizar e expandir metadados** para diferentes recortes temáticos e contextos de uso.

---

## Fluxo Principal

1. **enrich_static_data**
   - Copia a coleção `02_iahx_xml` para `03_xml_enriched`.
   - Adiciona campos estáticos como `instance` (lista de instâncias regionais e nacionais) e coleções temáticas (`collection_*`), padronizando o ponto de partida do enriquecimento.

2. **enrich_instancia**
   - Para cada coleção em TEMAS_BVS, busca os identificadores presentes em `03_xml_enriched`.
   - Atualiza o campo `instance` de cada documento, unificando as instâncias encontradas em todas as coleções.
   - Aplica as enrichment_rules para adicionar campos dinâmicos conforme regras temáticas, contextos e tags.

---

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
| tags/contextos            | Campos dinâmicos para segmentação e contexto temático         |

---

## Regras de Enriquecimento (`enrichment_rules`)

O enriquecimento dinâmico é controlado pelo dicionário `enrichment_rules`, que define, para cada coleção temática de TEMAS_BVS, quais campos adicionais devem ser aplicados aos documentos correspondentes.  
Cada regra pode adicionar:

- **instances**: Adiciona valores ao array `instance` do documento.
- **collections**: Cria campos do tipo `collection_<tema>` com valor padrão (ex: "LILACS").
- **tags**: Cria campos do tipo `tag_<tema>` populados com valores específicos do tema/subtema.
- **contextos**: Cria campos do tipo `tag_contexto` associado ao campo instância.

---

## Como funcionam as enrichment_rules

- **Estrutura**:  
  Cada chave de `enrichment_rules` corresponde ao nome de uma coleção em TEMAS_BVS. Os valores são dicionários que especificam quais campos devem ser enriquecidos e como.
- **Execução**:  
  Durante o enriquecimento, para cada documento em `03_xml_enriched` que tenha correspondência em uma coleção de TEMAS_BVS, aplica-se a regra definida:
    - Campos do tipo `instances` são agregados ao array `instance`.
    - Campos do tipo `collections` criam ou atualizam campos `collection_<tema>`.
    - Campos do tipo `tags` criam arrays com os subtemas ou classificações do documento.
    - Campos do tipo `contextos` criam arrays com valores contextuais, baseado no campo `instancia`.
- **Exemplo prático**:  
  Se um documento com `id_pk=123` existe em `TEMAS_BVS.hanseniase` com `tema_subtema="diagnóstico"` e `instancia="regional"`, após o enriquecimento ele terá:
  - `instance`: incluirá "hanseniase"
  - `collection_hanseniase`: "LILACS"
  - `tag_hanseniase`: ["diagnóstico"]

---

## Considerações Técnicas

- **Desempenho**: O processamento é feito em batches para eficiência, utilizando operações em lote (`bulk_write`) no MongoDB.
- **Idempotência**: O enriquecimento pode ser reexecutado sem duplicar valores, pois arrays são atualizados via operações de união.
- **Extensibilidade**: Novas regras podem ser adicionadas facilmente ao dicionário `enrichment_rules`, permitindo expansão para novas coleções ou temas.

"""


import logging
from datetime import datetime
from itertools import islice
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo import UpdateOne
from data_governance.dags.data_governance.misc import load_instanceEcollection
from data_governance.dags.data_governance.misc import load_DBinstanceEcollection


BATCH_SIZE = 1000


def create_union_view():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()

    db = mongo_client["TEMAS_BVS2"]
    view_name = '0_view_uniao_data_governance'

    # Verifica se a view já existe
    existing_views = db.list_collection_names(filter={"type": "view"})
    if view_name in existing_views:
        logger.info(f"View '{view_name}' já existe. Removendo-a.")
        db[view_name].drop()

    all_collections = [
        name for name in db.list_collection_names()
        if not name.startswith("system.")
    ]

    # Pipeline para adicionar o campo '_source' e unir as collections
    pipeline = []
    
    # Primeira collection: adiciona _source e define como base
    first_coll = all_collections[0]
    pipeline.append({
        '$addFields': {
            '_source': first_coll  # Nome da collection de origem
        }
    })
    
    # Demais collections: união com adição de _source
    for coll in all_collections[1:]:
        pipeline.append({
            '$unionWith': {
                'coll': coll,
                'pipeline': [{
                    '$addFields': {
                        '_source': coll  # Nome da collection atual
                    }
                }]
            }
        })

    # Cria a view usando a primeira collection como base
    db.command('create', view_name, viewOn=first_coll, pipeline=pipeline)
    logger.info(f"View '{view_name}' criada com sucesso.")


def setup_03_xml_enriched():
    """
    Duplica a coleção 02_iahx_xml para 03_xml_enriched e cria índices nos campos:
    - id (ascendente)
    - db (ascendente)
    - id_pk (ascendente)
    """
    logger = logging.getLogger(__name__)
    hook = MongoHook(mongo_conn_id='mongo')
    client = hook.get_conn()
    db = client["data_governance"]
    
    source_collection = "02_iahx_xml"
    target_collection = "03_xml_enriched"
    
    logger.info(f"Duplicando coleção: {source_collection} -> {target_collection}")
    
    # Remove a coleção de destino se já existir
    if target_collection in db.list_collection_names():
        db[target_collection].drop()
        logger.info(f"Coleção {target_collection} foi removida")
    
    # Duplica a coleção usando aggregation $out
    pipeline = [{"$out": target_collection}]
    db[source_collection].aggregate(pipeline)
    
    logger.info(f"Criando índices na coleção {target_collection}")
    db[target_collection].create_index([("id", 1)])
    db[target_collection].create_index([("db", 1)])
    db[target_collection].create_index([("database", 1)])
    db[target_collection].create_index([("id_pk", 1)])
    
    logger.info("Operação concluída: coleção duplicada e índices criados")


def list_join_db_batches():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')
    query = {"db": {"$exists": True, "$not": {"$size": 0}}}
    total_docs = enriched_collection.count_documents(query)
    logger.info(f"Encontrados {total_docs} documentos para processamento")

    return [[i] for i in range(0, total_docs, BATCH_SIZE)]


def list_join_database_batches():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')
    query = {"database": {"$exists": True, "$not": {"$size": 0}}}
    total_docs = enriched_collection.count_documents(query)
    logger.info(f"Encontrados {total_docs} documentos para processamento")

    return [[i] for i in range(0, total_docs, BATCH_SIZE)]


def enrich_join_instanceEcollection(offset):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')

    instanceEcollection = mongo_hook.get_collection('instanceEcollection', 'TABS')
    databases_data = load_instanceEcollection(instanceEcollection)
    
    query = {"db": {"$exists": True, "$not": {"$size": 0}}}
    offset = int(offset)

    cursor = enriched_collection.find(query).skip(offset).limit(BATCH_SIZE)
    current_batch = list(cursor)
    
    bulk_ops = []
    for doc in current_batch:
        doc_id = doc["_id"]

        db_list = doc.get("db", [])
        instances = set()
        collections = set()
        collection_instances = {}
        for db_name in db_list:              
            if db_name in databases_data:
                db_data = databases_data[db_name]

                # Tratamento para instâncias
                if 'instance' in db_data:
                    if isinstance(db_data['instance'], list):
                        instances.update(db_data['instance'])
                    else:
                        instances.add(str(db_data['instance']))
                
                # Tratamento para coleções
                if 'collection' in db_data:
                    if isinstance(db_data['collection'], list):
                        collections.update(db_data['collection'])
                    else:
                        collections.add(str(db_data['collection']))

                # Tratamento para collection_instance
                if 'collection_instance' in db_data:
                    for collection_instance in db_data['collection_instance']:
                        if collection_instance:
                            if collection_instance not in collection_instances:
                                collection_instances[collection_instance] = set()
                            collection_instances[collection_instance].add(db_name)

        # Preparar operação de atualização
        update_fields = {}
        if instances:
            update_fields["instance"] = {
                "$setUnion": [
                    {"$ifNull": ["$instance", []]},
                    list(instances)
                ]
            }
        if collections:
            update_fields["collection"] = {
                "$setUnion": [
                    {"$ifNull": ["$collection", []]},
                    list(collections)
                ]
            }
        for collection_instance, dbs in collection_instances.items():
            update_fields[collection_instance] = {
                "$setUnion": [
                    {"$ifNull": [f"${collection_instance}", []]},
                    list(dbs)
                ]
            }
        
        if update_fields:
            bulk_ops.append(
                UpdateOne(
                    {"_id": doc_id},
                    [{"$set": update_fields}]
                )
            )

    # Executa as operações em lote
    if bulk_ops:
        enriched_collection.bulk_write(bulk_ops, ordered=False)
        logger.info(f"Lote {offset//BATCH_SIZE} atualizado com {len(bulk_ops)} documentos")


def enrich_join_DBinstanceEcollection(offset):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')

    DBinstanceEcollection = mongo_hook.get_collection('DBinstanceEcollection', 'TABS')
    databases_data = load_DBinstanceEcollection(DBinstanceEcollection)
    
    query = {"database": {"$exists": True, "$not": {"$size": 0}}}
    offset = int(offset)

    cursor = enriched_collection.find(query).skip(offset).limit(BATCH_SIZE)
    current_batch = list(cursor)
    
    bulk_ops = []
    for doc in current_batch:
        doc_id = doc["_id"]

        db_list = doc.get("database", [])
        dbs = set()
        instances = set()
        collections = set()
        collection_instances = {}
        for db_name in db_list:              
            if db_name in databases_data:
                db_data = databases_data[db_name]

                # Tratamento para dbs
                if 'db' in db_data:
                    if isinstance(db_data['db'], list):
                        dbs.update(db_data['db'])
                    else:
                        dbs.add(str(db_data['db']))

                # Tratamento para instâncias
                if 'instance' in db_data:
                    if isinstance(db_data['instance'], list):
                        instances.update(db_data['instance'])
                    else:
                        instances.add(str(db_data['instance']))
                
                # Tratamento para coleções
                if 'collection' in db_data:
                    if isinstance(db_data['collection'], list):
                        collections.update(db_data['collection'])
                    else:
                        collections.add(str(db_data['collection']))

                # Tratamento para collection_instance
                if 'collection_instance' in db_data:
                    for collection_instance in db_data['collection_instance']:
                        if collection_instance:
                            if collection_instance not in collection_instances:
                                collection_instances[collection_instance] = set()
                            collection_instances[collection_instance].add(db_name)

        # Preparar operação de atualização
        update_fields = {}
        if dbs:
            update_fields["db"] = {
                "$setUnion": [
                    {"$ifNull": ["$db", []]},
                    list(dbs)
                ]
            }
        if instances:
            update_fields["instance"] = {
                "$setUnion": [
                    {"$ifNull": ["$instance", []]},
                    list(instances)
                ]
            }
        if collections:
            update_fields["collection"] = {
                "$setUnion": [
                    {"$ifNull": ["$collection", []]},
                    list(collections)
                ]
            }
        for collection_instance, dbs in collection_instances.items():
            update_fields[collection_instance] = {
                "$setUnion": [
                    {"$ifNull": [f"${collection_instance}", []]},
                    list(dbs)
                ]
            }
        
        if update_fields:
            bulk_ops.append(
                UpdateOne(
                    {"_id": doc_id},
                    [{"$set": update_fields}]
                )
            )

    # Executa as operações em lote
    if bulk_ops:
        enriched_collection.bulk_write(bulk_ops, ordered=False)
        logger.info(f"Lote {offset//BATCH_SIZE} atualizado com {len(bulk_ops)} documentos")


def enrich_instancia():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')

    # 1. Obter todos os ids da coleção origem
    pipeline = [{"$group": {"_id": "$id"}}]
    cursor = enriched_collection.aggregate(pipeline, allowDiskUse=True)
    id_values = [str(doc["_id"]) for doc in cursor]
    
    # 2. Para cada coleção no TEMAS_BVS2
    temas_bvs = mongo_client["TEMAS_BVS2"]
    coll = temas_bvs["0_view_uniao_data_governance"]

    doc_map = {}
    for i in range(0, len(id_values), BATCH_SIZE):
        batch_ids = id_values[i:i + BATCH_SIZE]
        cursor = coll.find(
            {'id_iahx': {'$in': batch_ids}},
            {'_id': 0, 'id_iahx': 1, 'db': 1, 'instance_iahx': 1, 'collection_iahx': 1, 'tema_subtema': 1, 'tema': 1, 'projeto': 1}
        )

        for doc in cursor:
            instances = doc.get('instance_iahx', [])
            collections = doc.get('collection_iahx', [])
            contextos = doc.get('projeto', [])
            tags = doc.get('tema_subtema', []) + doc.get('tema', [])
            database = [doc.get('db', '')]

            key = doc['id_iahx']
            if key not in doc_map:
                doc_map[key] = {
                    'instances': set(), 
                    'collections': set(), 
                    'tags': {},
                    'contextos': {}
                }

            doc_map[key]['instances'].update(instances)
            doc_map[key]['collections'].update(collections)

            i = 0
            while i < len(tags):
                tag = tags[i]

                if tag not in doc_map[key]['tags']:
                    doc_map[key]['tags'][tag] = []

                doc_map[key]['tags'][tag].append(tags[i+1] if i + 1 < len(tags) else None)
                i += 2

            i = 0
            while i < len(contextos):
                contexto = contextos[i]

                if contexto not in doc_map[key]['contextos']:
                    doc_map[key]['contextos'][contexto] = []

                doc_map[key]['contextos'][contexto].append(contextos[i+1] if i + 1 < len(contextos) else None)
                i += 2
            
        # 3. Enviar batch
        if doc_map:
            logger.info("Enviando batch de %i para o MongoDB" % (BATCH_SIZE, ))
            _process_batch(enriched_collection, doc_map, database)
            doc_map.clear()
    

def _process_batch(collection, doc_map, database):
    bulk_ops = []
    for id_value, data in doc_map.items():
        set_fields = {
            'instance': {
                '$setUnion': [
                    {'$ifNull': ['$instance', []]},
                    {'$literal': list(data['instances'])}
                ]
            }
        }

        # Adiciona campos collection_<nome_colecao>: db
        for coll_name in data['collections']:
            set_fields[coll_name] = {
                '$setUnion': [
                    {'$ifNull': [f'${coll_name}', []]},
                    {'$literal': database}
                ]
            }

        for coll_name in data['tags'].keys():
            set_fields[coll_name] = {
                '$setUnion': [
                    {'$ifNull': [f'${coll_name}', []]},
                    {'$literal': data['tags'][coll_name]}
                ]
            }

        for coll_name in data['contextos'].keys():
            set_fields[coll_name] = {
                '$setUnion': [
                    {'$ifNull': [f'${coll_name}', []]},
                    {'$literal': data['contextos'][coll_name]}
                ]
            }

        print(id_value, set_fields)

        bulk_ops.append(
            UpdateOne(
                {'id': id_value},
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
    create_union_view_task = PythonOperator(
        task_id='create_union_view',
        python_callable=create_union_view
    )
    setup_03_xml_enriched_task = PythonOperator(
        task_id='setup_03_xml_enriched',
        python_callable=setup_03_xml_enriched
    )

    """list_join_db_batches_task = PythonOperator(
        task_id='list_join_db_batches',
        python_callable=list_join_db_batches
    )
    enrich_join_instanceEcollection_task = PythonOperator.partial(
        task_id='enrich_join_instanceEcollection',
        python_callable=enrich_join_instanceEcollection
    ).expand(op_args=list_join_db_batches_task.output)

    list_join_database_batches_task = PythonOperator(
        task_id='list_join_database_batches',
        python_callable=list_join_database_batches
    )
    enrich_join_DBinstanceEcollection_task = PythonOperator.partial(
        task_id='enrich_join_DBinstanceEcollection',
        python_callable=enrich_join_DBinstanceEcollection
    ).expand(op_args=list_join_database_batches_task.output)"""

    enrich_instancia_task = PythonOperator(
        task_id='enrich_instancia',
        python_callable=enrich_instancia
    )

    #setup_03_xml_enriched_task >> list_join_db_batches_task
    #setup_03_xml_enriched_task >> list_join_database_batches_task

    #list_join_db_batches_task >> enrich_join_instanceEcollection_task
    #list_join_database_batches_task >> enrich_join_DBinstanceEcollection_task

    #enrich_join_instanceEcollection_task >> enrich_join_DBinstanceEcollection_task
    #enrich_join_DBinstanceEcollection_task >> enrich_instancia_task
    create_union_view_task >> enrich_instancia_task

    setup_03_xml_enriched_task >> enrich_instancia_task