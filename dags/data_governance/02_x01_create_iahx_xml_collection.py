import re
import unicodedata
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import UpdateOne

# --- Regras gramaticais e normalização ---
PREPOSICOES = {
    "de", "do", "da", "dos", "das", "e", "em", "no", "na", "nos", "nas",
    "à", "às", "ao", "aos", "por", "para", "com", "sem", "sob", "sobre", "entre"
}

def remove_accents(text):
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_word(word):
    plain = remove_accents(word).lower()
    if plain in PREPOSICOES:
        return word.lower()
    return word.capitalize()

def normalize_country_name(text):
    def normalize_token(token):
        if token.startswith("(") and token.endswith(")"):
            inner = token[1:-1]
            return f"({normalize_country_name(inner)})"
        else:
            parts = token.split("-")
            normalized_parts = [" ".join([normalize_word(w) for w in part.split()]) for part in parts]
            return "-".join(normalized_parts)
    tokens = re.findall(r'\([^\)]+\)|[^\s]+', text)
    return ' '.join([normalize_token(t) for t in tokens])

def normalize_afiliacao_string(pais_str):
    def repl(match):
        prefix = match.group(1)
        content = match.group(2)
        return f"{prefix}{normalize_country_name(content)}"
    return re.sub(r"(\^[a-z])([A-ZÇÁÉÍÓÚÂÊÔÃÕÄËÏÖÜÀÈÌÒÙÑ ()\-\w]+)", repl, pais_str)

# --- Função principal de transformação ---
def transformar_paises_mongodb():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_iahx_xml', 'data_governance')

    # === 1. Atualizar pais_afiliacao (array) ===
    query_afiliacao = {'pais_afiliacao': {'$exists': True, '$ne': []}}
    total_af = collection.count_documents(query_afiliacao)
    logger.info(f"Iniciando atualização de {total_af} documentos com campo pais_afiliacao...")

    batch = []

    for doc in collection.find(query_afiliacao):
        original = doc.get('pais_afiliacao', [])
        updated = []
        changed = False

        for item in original:
            norm = normalize_afiliacao_string(item)
            updated.append(norm)
            if norm != item:
                changed = True

        if changed:
            batch.append(UpdateOne({'_id': doc['_id']}, {'$set': {'pais_afiliacao': updated}}))

        if len(batch) >= 500:
            collection.bulk_write(batch)
            logger.info(f"Atualizados {len(batch)} registros de pais_afiliacao...")
            batch = []

    if batch:
        collection.bulk_write(batch)
        logger.info(f"Atualizados {len(batch)} registros finais de pais_afiliacao.")
    batch.clear()

    # === 2. Atualizar pais_publicacao (string) ===
    query_pub = {'pais_publicacao': {'$exists': True, '$ne': None}}
    total_pub = collection.count_documents(query_pub)
    logger.info(f"Iniciando atualização de {total_pub} documentos com campo pais_publicacao...")

    for doc in collection.find(query_pub):
        original = doc.get('pais_publicacao')
        norm = normalize_afiliacao_string(original)

        if norm != original:
            batch.append(UpdateOne({'_id': doc['_id']}, {'$set': {'pais_publicacao': norm}}))

        if len(batch) >= 500:
            collection.bulk_write(batch)
            logger.info(f"Atualizados {len(batch)} registros de pais_publicacao...")
            batch = []

    if batch:
        collection.bulk_write(batch)
        logger.info(f"Atualizados {len(batch)} registros finais de pais_publicacao.")

# --- Definição da DAG ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 15),
    'retries': 0
}

with DAG(
    dag_id='DG_02_x01_create_iahx_xml_collection',
    default_args=default_args,
    description='Corrige capitalização e gramática dos campos pais_afiliacao e pais_publicacao diretamente na coleção MongoDB',
    schedule=None,
    catchup=False,
    tags=['data_governance', 'fi-admin', 'mongodb']
) as dag:

    transformar_paises_mongo_task = PythonOperator(
        task_id='corrigir_paises_no_mongo',
        python_callable=transformar_paises_mongodb
    )

    transformar_paises_mongo_task
