"""
# TMGL - TMGL_01_full_update

## Visão Geral

Este DAG executa uma **atualização completa** da coleção `tmgl_metrics` no MongoDB, processando arquivos XML específicos de uma pasta configurada no sistema de arquivos. 
O fluxo foi desenvolvido para extrair, transformar e inserir dados de forma eficiente e com controle de memória para o projeto TMGL.

## Como Funciona

1. **Configuração da Coleção**:  
   Garante que a coleção `tmgl_metrics` (no banco `01_landing_zone`) exista no MongoDB e que possua um índice único no campo `id_pk`.

2. **Seleção dos Arquivos XML**:  
   Apenas arquivos XML que atendam aos seguintes critérios serão processados:
   - Arquivos com nomes exatamente iguais aos listados na seção abaixo
   - Arquivos que correspondam ao padrão: `md*_regional_tmgl.xml`

3. **Processamento e Inserção dos Dados**:
   - Processamento iterativo de XML usando `lxml.etree` para controle de memória
   - Apenas documentos onde o campo `instance` seja igual a `tmgl` são considerados
   - Campos com múltiplos valores são transformados em arrays
   - Verificação de duplicatas de `id_pk` dentro do mesmo lote
   - Inserção/atualização (upsert) no MongoDB em lotes de 1000 registros
   - Limpeza de memória durante o parsing

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
    - `psi_regional.xml`
    - `ijh_regional.xml`
    - `bbo_regional.xml`
    - `sus_regional.xml`
    - `ses_regional.xml`
    - `sms_regional.xml`
    - `lip_regional.xml`
    - `aim_regional.xml`
    - `med_regional.xml`
    - `phr_regional.xml`
    - `hom_regional.xml`
    - `bri_regional.xml`
    - `cid_regional.xml`
    - `pru_regional.xml`
    - `han_regional.xml`
    - `big_regional.xml`
    - `bdn_regional.xml`
    - `pie_regional.xml`
    - `rhs_regional.xml`
    - `arg_regional.xml`

## Dependências

- **MongoDB**: Armazenamento dos dados
- **lxml**: Processamento eficiente de XML
- **Sistema de arquivos**: Pasta de entrada configurada via conexão Airflow `TMGL_INPUT_XML_FOLDER`

## Tarefas

- `setup_collection`:  
  Garante a existência da coleção e do índice único no MongoDB

- `process_xml_files`:  
  Processamento otimizado com:
  - Parsing iterativo do XML para controle de memória
  - Detecção de duplicatas de registros em cada XML
  - Inserção em massa no MongoDB
  - Logs detalhados de progresso
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
    'psi_regional.xml',
    'ijh_regional.xml',
    'bbo_regional.xml',
    'sus_regional.xml',
    'ses_regional.xml',
    'sms_regional.xml',
    'lip_regional.xml',
    'aim_regional.xml',
    'med_regional.xml',
    'phr_regional.xml',
    'hom_regional.xml',
    'bri_regional.xml',
    'cid_regional.xml',
    'pru_regional.xml',
    'han_regional.xml',
    'big_regional.xml',
    'bdn_regional.xml',
    'pie_regional.xml',
    'rhs_regional.xml',
    'arg_regional.xml'
}


def setup_collection():
    """Configura coleção e índices no MongoDB"""
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    target_collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    target_collection.create_index([('id_pk', 1)], unique=True)


def process_xml_files():
    """Processa arquivos XML de forma eficiente usando parsing iterativo"""
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('01_landing_zone', 'tmgl_metrics')
    fs_hook = FSHook(fs_conn_id='TMGL_INPUT_XML_FOLDER')
    xml_folder_path = fs_hook.get_path()

    for filename in os.listdir(xml_folder_path):
        if not (filename.endswith('.xml') and 
               (fnmatch.fnmatch(filename, 'md*_regional_tmgl.xml') or filename in XML_FILES)):
            continue

        logger.info(f"Iniciando processamento de {filename}")
        file_path = os.path.join(xml_folder_path, filename)
        
        batch = []
        batch_size = 1000
        doc_count = 0
        seen_id_pks = set()  # Track IDs in current batch [2]

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
                if in_relevant_doc and 'id_pk' in current_fields:
                    id_pk = current_fields['id_pk']
                    
                    # Checa duplicatas
                    if id_pk not in seen_id_pks:
                        batch.append(ReplaceOne(
                            {'id_pk': id_pk},
                            current_fields.copy(),
                            upsert=True
                        ))
                        seen_id_pks.add(id_pk)
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
        seen_id_pks.clear()


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
    setup_task = PythonOperator(
        task_id='setup_collection',
        python_callable=setup_collection
    )
    process_task = PythonOperator(
        task_id='process_xml_files',
        python_callable=process_xml_files
    )

    setup_task >> process_task