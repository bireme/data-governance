import re
import unicodedata
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import UpdateOne

# =========================
# Ajustes de matching
# =========================
BATCH_SIZE = 500
CASE_INSENSITIVE = False  # se quiser casar "ai1" e "AI1", troque para True
TRIM_WHITESPACE = True    # remove espaços nas pontas antes de comparar

def _norm_key(s: str):
    if s is None:
        return None
    k = s.strip() if TRIM_WHITESPACE else s
    return k.lower() if CASE_INSENSITIVE else k

# =========================
# Regras gramaticais e normalização (já existentes)
# =========================
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

# =========================
# 1 & 2) Transformação de países em data_governance.02_iahx_xml
# =========================
def transformar_paises_mongodb():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_iahx_xml', 'data_governance')

    # === 1. Atualizar pais_afiliacao (array) ===
    query_afiliacao = {'pais_afiliacao': {'$exists': True, '$ne': []}}
    total_af = collection.count_documents(query_afiliacao)
    logger.info(f"Iniciando atualização de {total_af} docs com pais_afiliacao...")

    batch = []
    for doc in collection.find(query_afiliacao, projection={'pais_afiliacao': 1}):
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

        if len(batch) >= BATCH_SIZE:
            collection.bulk_write(batch, ordered=False)
            logger.info(f"Atualizados {len(batch)} registros de pais_afiliacao (parcial).")
            batch = []

    if batch:
        collection.bulk_write(batch, ordered=False)
        logger.info(f"Atualizados {len(batch)} registros finais de pais_afiliacao.")
    batch.clear()

    # === 2. Atualizar pais_publicacao (string) ===
    query_pub = {'pais_publicacao': {'$exists': True, '$ne': None}}
    total_pub = collection.count_documents(query_pub)
    logger.info(f"Iniciando atualização de {total_pub} docs com pais_publicacao...")

    batch = []
    for doc in collection.find(query_pub, projection={'pais_publicacao': 1}):
        original = doc.get('pais_publicacao')
        norm = normalize_afiliacao_string(original)

        if norm != original:
            batch.append(UpdateOne({'_id': doc['_id']}, {'$set': {'pais_publicacao': norm}}))

        if len(batch) >= BATCH_SIZE:
            collection.bulk_write(batch, ordered=False)
            logger.info(f"Atualizados {len(batch)} registros de pais_publicacao (parcial).")
            batch = []

    if batch:
        collection.bulk_write(batch, ordered=False)
        logger.info(f"Atualizados {len(batch)} registros finais de pais_publicacao.")

# =========================
# 3) Substituição do array "ai" usando mapeamento TABS.brisa_ai_11_17 (ai1 -> ai2)
# =========================
def _carregar_mapa_ai(hook: MongoHook):
    """
    Lê TABS.brisa_ai_11_17 e constrói um dict:
        key = ai1 (normalizado conforme flags)
        val = ai2 (string)
    Ignora registros sem ai1/ai2. Se ai1 vier como lista, inclui cada item.
    """
    logger = logging.getLogger(__name__)
    col_map = hook.get_collection('brisa_ai_11_17', 'TABS')

    cursor = col_map.find(
        {'ai1': {'$exists': True, '$ne': None}, 'ai2': {'$exists': True, '$ne': None}},
        projection={'ai1': 1, 'ai2': 1}
    )

    mapa = {}
    dups = 0
    total = 0
    for m in cursor:
        total += 1
        src = m.get('ai1')
        dst = m.get('ai2')

        if dst is None or (isinstance(dst, str) and dst.strip() == ""):
            continue

        # ai1 pode ser string ou lista de strings
        if isinstance(src, list):
            for s in src:
                if not isinstance(s, str):
                    continue
                k = _norm_key(s)
                if k is None:
                    continue
                if k in mapa and mapa[k] != dst:
                    dups += 1
                mapa[k] = dst
        elif isinstance(src, str):
            k = _norm_key(src)
            if k:
                if k in mapa and mapa[k] != dst:
                    dups += 1
                mapa[k] = dst

    logger.info(f"[mapa ai] carregado: {len(mapa)} chaves (de {total} registros). conflitos: {dups}.")
    return mapa

def substituir_ai_por_mapeamento():
    """
    Para cada doc em data_governance.02_iahx_xml com 'ai' (array de strings):
      - para cada elemento v de 'ai', se existir v em mapa[ai1], substitui por ai2 correspondente.
    """
    logger = logging.getLogger(__name__)
    hook = MongoHook(mongo_conn_id='mongo')

    # 1) carregamos o mapa ai1->ai2 da base TABS
    mapa = _carregar_mapa_ai(hook)

    # 2) percorremos data_governance.02_iahx_xml
    col_xml = hook.get_collection('02_iahx_xml', 'data_governance')
    filtro_ai_arr = {'ai': {'$exists': True, '$type': 'array', '$ne': []}}
    total_docs = col_xml.count_documents(filtro_ai_arr)
    logger.info(f"[iahx_xml] documentos com 'ai' array: {total_docs}")

    batch = []
    atualizados = 0
    processados = 0

    for doc in col_xml.find(filtro_ai_arr, projection={'ai': 1}):
        processados += 1
        original = doc.get('ai', [])
        changed = False
        novo_ai = []

        for v in original:
            if isinstance(v, str):
                k = _norm_key(v)
                if k in mapa:
                    novo_ai.append(mapa[k])
                    changed = True
                else:
                    novo_ai.append(v)
            else:
                # mantém valores não-string como estão
                novo_ai.append(v)

        if changed:
            batch.append(UpdateOne({'_id': doc['_id']}, {'$set': {'ai': novo_ai}}))
            atualizados += 1

        if len(batch) >= BATCH_SIZE:
            col_xml.bulk_write(batch, ordered=False)
            logger.info(f"[iahx_xml] atualizados {len(batch)} docs (parcial).")
            batch = []

    if batch:
        col_xml.bulk_write(batch, ordered=False)
        logger.info(f"[iahx_xml] atualizados {len(batch)} docs (final).")

    logger.info(f"[iahx_xml] processados: {processados}; docs com alteração em 'ai': {atualizados}.")

# =========================
# Definição da DAG
# =========================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 15),
    'retries': 0
}

with DAG(
    dag_id='DG_02_x01_create_iahx_xml_collection',
    default_args=default_args,
    description=(
        'Corrige campos de país e substitui elementos do array ai '
        'usando mapeamento ai1->ai2 de TABS.brisa_ai_11_17'
    ),
    schedule=None,
    catchup=False,
    tags=['data_governance', 'fi-admin', 'mongodb', 'TABS']
) as dag:

    corrigir_paises = PythonOperator(
        task_id='corrigir_paises_no_mongo',
        python_callable=transformar_paises_mongodb
    )

    substituir_ai = PythonOperator(
        task_id='substituir_ai_por_mapeamento_tabs',
        python_callable=substituir_ai_por_mapeamento
    )

    sanear_xml_com_tidy = BashOperator(
        task_id='sanear_xml_com_tidy',
        bash_command='tidy -xml -w 0 -i -utf8 -o /caminho/para/mar_tidy.xml /caminho/para/mar.xml'
    )

    validar_xml = BashOperator(
        task_id='validar_xml_com_xmlstarlet',
        bash_command='xmlstarlet val -e /caminho/para/mar_tidy.xml'
    )

    corrigir_paises >> substituir_ai >> sanear_xml_com_tidy >> validar_xml
