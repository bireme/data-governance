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


enrichment_rules = {
    'adolec': {
        'instances': ['adolec']
    },
    'aps': {
        'instances': ['aps']
    },
    'aps_colecao': {
        'instances': ['collection_aps']
    },
    'assa2030': {
        'tags': ['tag_assa2030']
    },
    'assa2030_wizard': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'aspect': {
        'tags': ['aspect']
    },
    'brasil': {
        'tags': ['tema_brasil']
    },
    'bvs_sp': {
        'tags': ['tag_sp']
    },
    'carpha_country_focus': {
        'tags': ['carpha_country_focus']
    },
    'carpha_language': {
        'tags': ['carpha_language']
    },
    'carpha_type_document': {
        'tags': ['carpha_type_document']
    },
    'carpha_author_location': {
        'tags': ['carpha_author_location']
    },
    'carpha_geral': {
        'instances': ['carpha'],
        'tags': ['carpha_geral']
    },      
    'controle_cancer': {
        'tags': ['tag_controlecancer']
    },
    'dimentions': {
        'tags': ['tag_dimentions']
    },
    'ecos': {
        'instances': ['economia'],
        'collections': ['collection_economia'],
        'tags': ['economia_tag']
    },
    'enfermeria': {
        'tags': ['tag_enfermeria']
    },
    'evideasy_perguntas': {
        'tags': ['evideasy_perguntas']
    },
    'hanseniase': {
        'instances': ['hanseniase'],
        'collections': ['collection_hanseniase'],
        'tags': ['tag_hanseniase']
    },
    'homeopatia': {
        'instances': ['homeopatia']
    },
    'lilacsplus_alc': {
        'instances': ['lilacsplus']
    },
    'limit': {
        'tags': ['limit']
    },
    'mocambique': {
        'instances': ['mocambique'],
        'collections': ['collection_mocambique']
    },
    'mtc': {
        'instances': ['mtc', 'tmgl'],
        'collections': ['collection_mtc', 'collection_tmgl']
    },
    'mtc_elementos': {
        'instances': ['mtc', 'tmgl'],
        'collections': ['collection_mtc', 'collection_tmgl'],
        'tags': ['tag_mtc_elementos']
    },
    'mtc_tema1': {
        'instances': ['mtc', 'tmgl'],
        'collections': ['collection_mtc', 'collection_tmgl'],
        'tags': ['tag_mtc_tema1']
    },
    'mtc_tema2': {
        'instances': ['mtc', 'tmgl'],
        'collections': ['collection_mtc', 'collection_tmgl'],
        'tags': ['tag_mtc_tema2']
    },
    'mtc_tema3': {
        'instances': ['mtc', 'tmgl'],
        'collections': ['collection_mtc', 'collection_tmgl'],
        'tags': ['tag_mtc_tema3']
    },
    'mtc_tema4': {
        'tags': ['tag_mtc_tema4']
    },
    'mtc_transversales': {
        'instances': ['mtc', 'tmgl'],
        'collections': ['collection_mtc', 'collection_tmgl'],
        'tags': ['tag_mtc_transversales']
    },
    'odontologia_temas': {
        'instances': ['odontologia'],
        'tags': ['tema_odontologia']
    },
    'ods3': {
        'tags': ['tag_ods3']
    },
    'ods3_wizard': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_mortalidade_materna': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_enfermedades_notrasmisibles': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_muertes_prevenibles_nacidos_ninos': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_consumo_sustancias_psicoactivas': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_accidentes_transito': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_salud_sexual_reprodutiva': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_cobertura_universal': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_hazardous_chemicals_pollution_contamination': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_tobacco_control': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_health_workforce': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'ods3_wizard_global_health_risks': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'pais_assunto': {
        'tags': ['pais_assunto']
    },
    'neglected_disease': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'rhs': {
        'instances': ['recursos_humanos'],
        'tags': ['tag_rhs']
    },
    'saude_ambiental': {
        'instances': ['saude_ambiental'],
        'tags': ['tag_saudeambiental']
    },
    'sessp': {
        'tags': ['tag_sessp']
    },
    'study_type': {
        'tags': ['type_of_study']
    },
    'transmisible_diseases': {
        'tags': ['tag_tema_saude'],
        'contextos': ['tag_contexto']
    },
    'who_regions': {
        'tags': ['who_regions']
    }
}


def create_union_view():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()

    db = mongo_client["TEMAS_BVS"]
    view_name = '0_view_uniao_data_governance'

    # Verifica se a view já existe
    existing_views = db.list_collection_names(filter={"type": "view"})
    if view_name in existing_views:
        logger.info(f"View '{view_name}' já existe. Removendo-a.")
        db[view_name].drop()

    all_collections = list(enrichment_rules.keys())

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
    db[target_collection].create_index([("id_pk", 1)])
    
    logger.info("Operação concluída: coleção duplicada e índices criados")


def enrich_join_database():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')

    instanceEcollection = mongo_hook.get_collection('instanceEcollection', 'TABS')
    databases_data = load_instanceEcollection(instanceEcollection)

    # Consulta para documentos com campo 'db' não vazio
    query = {"db": {"$exists": True, "$not": {"$size": 0}}}
    total_docs = enriched_collection.count_documents(query)
    logger.info(f"Encontrados {total_docs} documentos para processamento")
    
    # Processamento em lotes
    batch_size = 1000
    for offset in range(0, total_docs, batch_size):
        # Busca lote atual
        cursor = enriched_collection.find(query).skip(offset).limit(batch_size)
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
            logger.info(f"Lote {offset//batch_size} atualizado com {len(bulk_ops)} documentos")


def enrich_instancia():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_client = mongo_hook.get_conn()
    enriched_collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')

    # 1. Obter todos os ids da coleção origem
    pipeline = [{"$group": {"_id": "$id"}}]
    cursor = enriched_collection.aggregate(pipeline, allowDiskUse=True)
    id_values = [str(doc["_id"]) for doc in cursor]
    
    # 2. Para cada coleção no TEMAS_BVS especificado no enrichment_rules
    temas_bvs = mongo_client["TEMAS_BVS"]
    coll = temas_bvs["0_view_uniao_data_governance"]

    batch_size = 1000
    doc_map = {}
    for i in range(0, len(id_values), batch_size):
        batch_ids = id_values[i:i + batch_size]
        cursor = coll.find(
            {'id_iahx': {'$in': batch_ids}},
            {'_id': 0, 'id_iahx': 1, 'instancia': 1, 'tema_subtema': 1, '_source': 1}
        )

        for doc in cursor:
            coll_name = doc['_source']

            instances = enrichment_rules[coll_name].get('instances', [])
            collections = enrichment_rules[coll_name].get('collections', [])
            contextos = enrichment_rules[coll_name].get('contextos', [])
            tags = enrichment_rules[coll_name].get('tags', [])
            
            key = doc['id_iahx']
            if key not in doc_map:
                doc_map[key] = {
                    'instances': set(), 
                    'collections': set(), 
                    'campo_instancia': set(),
                    'tags': {},
                    'contextos': set()
                }

            doc_map[key]['instances'].update(instances)

            if doc['instancia']:
                doc_map[key]['campo_instancia'].add(doc['instancia'])

            doc_map[key]['collections'].update(collections)
            doc_map[key]['contextos'].update(contextos)

            for tag in tags:
                if tag not in doc_map[key]['tags']:
                    doc_map[key]['tags'][tag] = []

                if doc['tema_subtema']:
                    # Corner case for Odontologia
                    if not (coll_name == 'odontologia_temas' and doc['tema_subtema'] == 'geral'):
                        doc_map[key]['tags'][tag].append(doc['tema_subtema'])
            
        # 3. Enviar batch
        if doc_map:
            logger.info("Enviando batch de %i para o MongoDB" % (batch_size, ))
            _process_batch(enriched_collection, doc_map)
            doc_map.clear()
    

def _process_batch(collection, doc_map):
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
        # Adiciona campos collection_<nome_colecao>: "LILACS"
        for coll_name in data['collections']:
            set_fields[coll_name] = "LILACS"

        for coll_name in data['tags'].keys():
            set_fields[coll_name] = {
                '$setUnion': [
                    {'$ifNull': [f'${coll_name}', []]},
                    {'$literal': data['tags'][coll_name]}
                ]
            }

        for coll_name in data['contextos']:
            set_fields[coll_name] = {
                '$setUnion': [
                    {'$ifNull': [f'${coll_name}', []]},
                    {'$literal': list(data['campo_instancia'])}
                ]
            }

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
    enrich_join_database_task = PythonOperator(
        task_id='enrich_join_database',
        python_callable=enrich_join_database
    )
    enrich_instancia_task = PythonOperator(
        task_id='enrich_instancia',
        python_callable=enrich_instancia
    )

    setup_03_xml_enriched_task >> enrich_join_database_task 
    setup_03_xml_enriched_task >> enrich_instancia_task
    enrich_join_database_task >> enrich_instancia_task
    create_union_view_task >> enrich_instancia_task