"""
# TMGL - TMGL_01_full_update

## Visão Geral

Este DAG executa uma **atualização completa** da coleção `tmgl_metrics` no MongoDB, processando arquivos XML específicos de uma pasta configurada no sistema de arquivos. 
O fluxo foi desenvolvido para ingerir, transformar e inserir dados de forma eficiente para o projeto TMGL.

## Como Funciona

1. **Configuração da Coleção**:  
   Garante que a coleção `tmgl_metrics` (no banco `01_landing_zone`) exista no MongoDB e que possua um índice único no campo `id_pk`.

2. **Seleção dos Arquivos XML**:  
   Apenas arquivos XML que atendam aos seguintes critérios serão processados:
   - Arquivos com nomes exatamente iguais aos listados na seção abaixo.
   - Arquivos que correspondam ao padrão: `md*_regional_tmgl.xml`.

3. **Processamento e Inserção dos Dados**:
   - Cada arquivo XML selecionado é lido e convertido em dicionário usando `xmltodict`.
   - Apenas documentos onde o campo `instance` seja igual a `tmgl` são considerados.
   - Campos com múltiplos valores são transformados em arrays.
   - Os documentos são inseridos/atualizados (upsert) no MongoDB em lotes de 1000 registros para maior eficiência.

## Arquivos Processados

- Padrão: `md*_regional_tmgl.xml`
- Nomes exatos:
    - `wpr_regional.xml`
    - `lil_regional.xml`
    - `sea_regional.xml`
    - `ibc_regional.xml`
    - `cum_regional.xml`
    - `bde_regional.xml`
    - `who_regional.xml`
    - `bin_regional.xml`
    - `vti_regional.xml`
    - `mtc_regional.xml`
    - `ijh_regional.xml`
    - `bbo_regional.xml`
    - `sus_regional.xml`
    - `sms_regional.xml`
    - `lip_regional.xml`
    - `aim_regional.xml`
    - `med_regional.xml`
    - `phr_regional.xml`
    - `hom_regional.xml`
    - `bri_regional.xml`

## Dependências

- **MongoDB**: Armazenamento dos dados
- **xmltodict**: Conversão de XML para dicionário
- **Sistema de arquivos**: Pasta de entrada configurada via conexão Airflow `TMGL_INPUT_XML_FOLDER`

## Tarefas

- `setup_collection`: Garante a existência da coleção e do índice no MongoDB.
- `process_xml_files`: Processa e carrega os dados dos arquivos XML no MongoDB.
"""

import xmltodict
import json
import os
import logging
import fnmatch
from datetime import datetime
from pymongo import ReplaceOne
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook


XML_FILES = {
    'wpr_regional.xml',
    'lil_regional.xml',
    'sea_regional.xml',
    'ibc_regional.xml',
    'cum_regional.xml',
    'bde_regional.xml',
    'who_regional.xml',
    'bin_regional.xml',
    'vti_regional.xml',
    'mtc_regional.xml',
    'ijh_regional.xml',
    'bbo_regional.xml',
    'sus_regional.xml',
    'sms_regional.xml',
    'lip_regional.xml',
    'aim_regional.xml',
    'med_regional.xml',
    'phr_regional.xml',
    'hom_regional.xml',
    'bri_regional.xml'
}


def setup_collection():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    target_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection.create_index([('id_pk', 1)], unique=True)


def process_xml_files():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')

    fs_hook = FSHook(fs_conn_id='TMGL_INPUT_XML_FOLDER')
    xml_folder_path = fs_hook.get_path()
    
    batch = []
    batch_size = 1000
    for filename in os.listdir(xml_folder_path):
        if (filename.endswith('.xml') and 
            (fnmatch.fnmatch(filename, 'md*_regional_tmgl.xml') or 
             filename in XML_FILES)):

            logger.info(f"Processando {filename}.")

            with open(os.path.join(xml_folder_path, filename), 'r', encoding='ISO-8859-1') as f:
                xml_data = xmltodict.parse(f.read())
                docs = xml_data.get('add', {}).get('doc', [])
                docs = docs if isinstance(docs, list) else [docs]
                
                for doc in docs:
                    fields = doc.get('field', [])
                    fields = fields if isinstance(fields, list) else [fields]
                    if any(f.get('@name') == 'instance' and f.get('#text') == 'tmgl' for f in fields):
                        processed_doc = {}
                        for field in fields:
                            field_name = field.get('@name')
                            field_value = field.get('#text')
                            
                            # Agrupa múltiplos valores do mesmo campo em arrays
                            if field_name in processed_doc:
                                if not isinstance(processed_doc[field_name], list):
                                    processed_doc[field_name] = [processed_doc[field_name]]
                                processed_doc[field_name].append(field_value)
                            else:
                                processed_doc[field_name] = field_value
                        
                        if 'id_pk' in processed_doc:
                            batch.append(ReplaceOne(
                                {"id_pk": processed_doc['id_pk']},
                                processed_doc,
                                upsert=True
                            ))
                        
                        if len(batch) >= batch_size:
                            logger.info(f"Inserindo {batch_size} no MongoDB.")
                            collection.bulk_write(batch, ordered=False)
                            batch = []
    
    if batch:
        collection.bulk_write(batch)


# Configuração do DAG
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
    description='TMGL - Inicia o processamento em modo Full update, ou seja, coletará e processará todos os registros XML sem filtrar por update date',
    tags=["tmgl", "xml", "mongodb", "full_update"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    setup_collection_task = PythonOperator(
        task_id='setup_collection',
        python_callable=setup_collection
    )

    migration_task = PythonOperator(
        task_id='process_xml_files',
        python_callable=process_xml_files
    )

    setup_collection_task >> migration_task