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
    'controlecancer': {
        'instances': ['controlecancer'],
        'collections': ['collection_controlecancer']
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
        logger.info(f"View '{view_name}' já existe. Nenhuma ação necessária.")

    else:
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
    target_col.create_index("id")


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
    batch_size = 100
    doc_map = {}

    cursor = coll.find(
        {'id_iahx': {'$in': list(id_values)}},
        {'_id': 0, 'id_iahx': 1, 'instancia': 1, 'tema_subtema': 1, '_source': 1},
        batch_size=batch_size
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
                'temas': set(), 
                'campo_instancia': set(),
                'tags': set(),
                'contextos': set()
            }

        doc_map[key]['instances'].update(instances)

        if doc['tema_subtema']:
            # Corner case for Odontologia
            if not (coll_name == 'odontologia_temas' and doc['tema_subtema'] == 'geral'):
                doc_map[key]['temas'].add(doc['tema_subtema'])

        if doc['instancia']:
            doc_map[key]['campo_instancia'].add(doc['instancia'])

        doc_map[key]['collections'].update(collections)
        doc_map[key]['contextos'].update(contextos)
        doc_map[key]['tags'].update(tags)
        
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

        for coll_name in data['tags']:
            set_fields[coll_name] = {
                '$setUnion': [
                    {'$ifNull': [f'${coll_name}', []]},
                    {'$literal': list(data['temas'])}
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
    enrich_static_data_task = PythonOperator(
        task_id='enrich_static_data',
        python_callable=enrich_static_data
    )

    enrich_instancia_task = PythonOperator(
        task_id='enrich_instancia',
        python_callable=enrich_instancia
    )

    create_union_view_task = PythonOperator(
        task_id='create_union_view',
        python_callable=create_union_view
    )

    enrich_static_data_task >> enrich_instancia_task
    create_union_view_task >> enrich_instancia_task