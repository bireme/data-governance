"""
# Data Governance - DG_04_export_xml

Resumo:
-------
Este DAG do Airflow exporta documentos de uma coleção MongoDB (`03_xml_enriched` no banco de dados `data_governance`)
e grava cada documento como um arquivo XML enriquecido separado em um diretório do sistema de arquivos especificado.
Os arquivos XML são formatados para importação no sistema iah-X.

Principais Características:
---------------------------
- Exporta dinamicamente todos os campos de cada documento MongoDB, tratando listas como campos XML repetidos.
- Os arquivos XML de saída são nomeados de acordo com o campo `id_pk` de cada documento.
- O diretório de saída é configurado via uma conexão Filesystem do Airflow (`DG_EXPORT_XML_FOLDER`).
- Desenvolvido para interoperabilidade com o iah-X.

Configuração:
-------------
- Conexão MongoDB: Configure uma conexão no Airflow com o Conn ID `mongo` apontando para sua instância do MongoDB.
- Conexão Filesystem: Configure uma conexão Filesystem no Airflow com o Conn ID `DG_EXPORT_XML_FOLDER` para especificar o diretório de saída.
"""

import os
import re
import logging
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from xml.sax.saxutils import escape
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook


def remove_invalid_xml_chars(text):
    # Remove caracteres de controle não permitidos pelo XML 1.0
    return re.sub(
        r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', 
        text if isinstance(text, str) else str(text)
    )


def export_mongo_to_xml():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('03_xml_enriched', 'data_governance')
    
    fs_hook = FSHook(fs_conn_id='DG_EXPORT_XML_FOLDER')
    output_dir = fs_hook.get_path()
    os.makedirs(output_dir, exist_ok=True)

    # Geração do nome do arquivo com timestamp
    timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
    output_filename = f"data_governance_{timestamp}.xml"
    output_file = os.path.join(output_dir, output_filename)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<add>\n')

        for doc in collection.find({}, {"database": 0, "_id": 0}):
            id_pk = doc.get('id_pk')

            boost_value = str(doc.get('weight', ''))
            root = ET.Element("doc", boost=boost_value)

            # Process all document fields dynamically
            for key in sorted(doc.keys()):
                value = doc[key]

                if isinstance(value, list):
                    for item in value:
                        clean_item = remove_invalid_xml_chars(item)
                        ET.SubElement(root, 'field', name=key).text = clean_item
                else:
                    clean_value = remove_invalid_xml_chars(value)
                    ET.SubElement(root, 'field', name=key).text = clean_value

            # Write individual XML file
            ET.indent(root, space='  ', level=0)
            xml_str = ET.tostring(root, encoding='unicode')
            f.write(xml_str + '\n')

        f.write('</add>')
    
    logger.info(f"Arquivo XML exportado com sucesso: {output_file}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 15),
    'retries': 0
}
with DAG(
    'DG_04_export_xml',
    default_args=default_args,
    description='Data Governance - Exporta a coleção com nomenclatura de XML enriquecido em XML para importação no iah-X',
    tags=["data_governance", "mongodb", "iahx", "xml", "export"],
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    export_task = PythonOperator(
        task_id='export_mongo_to_xml',
        python_callable=export_mongo_to_xml
    )
