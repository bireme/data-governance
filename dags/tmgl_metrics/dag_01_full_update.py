"""
# DAG TMGL_01_full_update

Este módulo define um pipeline de ETL utilizando Apache Airflow para processar arquivos XML contendo dados TMGL e armazená-los em uma coleção MongoDB. 
O fluxo é projetado para realizar uma atualização completa ("full update"), processando todos os registros disponíveis.

## Funcionalidades Principais

- **Configuração da coleção MongoDB**: Criação e indexação da coleção de landing zone, garantindo unicidade e performance nas consultas.
- **Listagem de arquivos XML**: Busca automática de arquivos XML em um diretório configurado via conexão Airflow.
- **Processamento eficiente de XML**: Parsing iterativo dos arquivos XML, extraindo apenas documentos relevantes e realizando inserções em lote no MongoDB.

## Estrutura do Pipeline

1. **setup_collection_landing_zone**  
   - Remove a coleção de landing zone existente e recria com índices apropriados para os campos `id`, `cp`, `pais_afiliacao` e `who_regions`.
   - Garante que a coleção esteja pronta para receber novos dados sem duplicidade.

2. **list_xml_files**  
   - Lista todos os arquivos XML no diretório configurado que terminam com `_regional_tmgl.xml`.
   - Retorna a lista de arquivos para processamento subsequente.

3. **process_xml_file**  
   - Realiza parsing iterativo dos arquivos XML, extraindo apenas documentos com o campo `instance` igual a `tmgl`.
   - Garante que apenas documentos únicos (por `id`) sejam inseridos/atualizados na coleção.
   - Utiliza operações em lote (`bulk_write`) para eficiência e menor uso de memória.

## Exemplo de Uso

1. Configure as conexões no Airflow (`mongo` e `TMGL_INPUT_XML_FOLDER`).
2. Coloque os arquivos XML no diretório configurado.
3. Execute o DAG `TMGL_01_full_update` via interface do Airflow.

## Dependências

- Apache Airflow
- pymongo
- lxml
- MongoDB
"""


import json
import os
import logging
import fnmatch
from lxml import etree as ET
from xml.sax import make_parser, handler
from datetime import datetime
from pymongo import ReplaceOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


def setup_collections():
    """Configura coleção e índices no MongoDB"""
    mongo_hook = MongoHook(mongo_conn_id='mongo')

    # Deleta coleções a serem atualizadas
    db = mongo_hook.get_conn()['tmgl_metrics']
    db['01_landing_zone'].drop()
    db['02_countries_metrics'].drop()

    # Cria coleção de landing zone
    collection_lz = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    collection_lz.create_index([('id', 1)], unique=True)
    collection_lz.create_index([('fulltext', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('mj', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('tag_dimentions', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('type_of_study', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('type', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('cp', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('la', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('ta', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('traditional_medicines_cluster', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('pais_afiliacao', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('who_regions', 1)], collation={ 'locale': 'en', 'strength': 1 })
    collection_lz.create_index([('year', 1)])

    # Cria coleção de métricas
    #collection_metrics = mongo_hook.get_collection('02_countries_metrics', 'tmgl_metrics')
    #collection_metrics.create_index([('type', 1), ('country', 1), ('name', 1)], unique=True)


def list_xml_files():
    logger = logging.getLogger(__name__)

    fs_hook = FSHook(fs_conn_id='TMGL_INPUT_XML_FOLDER')
    xml_folder_path = fs_hook.get_path()
    files = [
        [filename] for filename in os.listdir(xml_folder_path)
        if filename.endswith('_regional_tmgl.xml')
    ]
    logger.info(f"Arquivos encontrados: {files}")
    return files


def process_xml_file(filename):
    """Processa arquivos XML de forma eficiente usando parsing iterativo"""
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    fs_hook = FSHook(fs_conn_id='TMGL_INPUT_XML_FOLDER')
    xml_folder_path = fs_hook.get_path()

    logger.info(f"Iniciando processamento de {filename}")
    file_path = os.path.join(xml_folder_path, filename)
    
    batch = []
    batch_size = 1000
    doc_count = 0
    seen_ids = set()

    context = ET.iterparse(
        file_path,
        events=('start', 'end'),
        tag=['doc', 'field'],
        huge_tree=True,
        remove_blank_text=True
    )
    context = iter(context)
    
    current_fields = {}
    in_relevant_doc = False

    for event, elem in context:
        if event == 'start' and elem.tag == 'doc':
            current_fields = {}
            in_relevant_doc = False

        elif event == 'end' and elem.tag == 'field':
            field_name = elem.get('name')
            field_value = elem.text.strip() if elem.text else None
            
            if field_name == 'instance' and field_value == 'tmgl':
                in_relevant_doc = True
            
            if field_name and field_value:
                if field_name in current_fields:
                    if not isinstance(current_fields[field_name], list):
                        current_fields[field_name] = [current_fields[field_name]]
                    current_fields[field_name].append(field_value)
                else:
                    current_fields[field_name] = field_value
            
            elem.clear()

        elif event == 'end' and elem.tag == 'doc':
            if in_relevant_doc and 'id' in current_fields:
                id_field = current_fields['id']
                
                # Checa duplicatas
                if id_field not in seen_ids:
                    batch.append(ReplaceOne(
                        {'id': id_field},
                        current_fields.copy(),
                        upsert=True
                    ))
                    seen_ids.add(id_field)
                    doc_count += 1

                if len(batch) >= batch_size:
                    logger.info(f"Persistindo lote de {batch_size} documentos")
                    collection.bulk_write(batch, ordered=False)
                    batch = []

            # Limpeza de memória
            current_fields.clear()
            elem.clear()
            if elem.getparent() is not None:
                elem.getparent().clear()

    # Processar último lote
    if batch:
        collection.bulk_write(batch, ordered=False)
        logger.info(f"Arquivo {filename} processado. Total documentos: {doc_count}")

    # Limpeza final
    del context
    seen_ids.clear()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'TMGL_01_full_update',
    default_args=default_args,
    description='TMGL - Inicia o processamento em modo Full update, ou seja, coletará e processará todos os registros XML',
    tags=["tmgl", "xml", "mongodb", "full_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    setup_task = PythonOperator(
        task_id='setup_collections',
        python_callable=setup_collections
    )

    list_files_task = PythonOperator(
        task_id='list_xml_files',
        python_callable=list_xml_files
    )

    process_task = PythonOperator.partial(
        task_id='process_xml_file',
        python_callable=process_xml_file
    ).expand(op_args=list_files_task.output)

    setup_task >> list_files_task >> process_task