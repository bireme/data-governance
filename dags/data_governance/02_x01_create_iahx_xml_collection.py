import re
import unicodedata
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
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
# Mapeamento de correção do campo "db"
# =========================
DB_MAP = {
    "ColecionaSUS": "colecionaSUS",
    "IndexPsi": "INDEXPSI",
    "MTyCI": "MTYCI",
    "VetIndex": "VETINDEX",
    "REPOSITORIOBVS": "redbvs",
    "INCA": "inca",
}

# =========================
# Mapeamento de correção do campo "database"
# coluna 1 -> coluna 2 do arquivo campo_database_SugestaoDeNormalizado_colunas_1e2
# =========================
DATABASE_MAP = {
    'ACVESES': 'ACVSES',
    'ACVSES': 'ACVSES',
    'AGITASPPROD': 'AGITASPPROD',
    'AHM-Acervo': 'AHM-ACERVO',
    'AHM-Producao': 'AHM-PRODUCAO',
    'ATTI-Producao': 'ATTI-PRODUCAO',
    'CAB-Poducao': 'CAB-PRODUCAO',
    'CAB-Pordução': 'CAB-PRODUCAO',
    'CAB-Produaco': 'CAB-PRODUCAO',
    'CAB-producao': 'CAB-PRODUCAO',
    'CAB- Producao': 'CAB-PRODUCAO',
    'CAB-Producao': 'CAB-PRODUCAO',
    'CAB- Produção': 'CAB-PRODUCAO',
    'CAB-Produção': 'CAB-PRODUCAO',
    'CAB-Proucao': 'CAB-PRODUCAO',
    'CACHOEIRINHA-Acervo': 'CACHOEIRINHA-ACERVO',
    'Cachoeirinha-Producao': 'CACHOEIRINHA-PRODUCAO',
    'CACHOEIRINHA-Producao': 'CACHOEIRINHA-PRODUCAO',
    'CACHOEIRINHA-PRODUCAO': 'CACHOEIRINHA-PRODUCAO',
    'CACHOEIRINHA-Produção': 'CACHOEIRINHA-PRODUCAO',
    'CACHOERINHA-Producao': 'CACHOEIRINHA-PRODUCAO',
    'CAMPOLIMPO-Acervo': 'CAMPOLIMPO-ACERVO',
    'CAMPOLIMPO-Producao': 'CAMPOLIMPO-PRODUCAO',
    'CEINFO-Pordução': 'CEINFO-PRODUCAO',
    'CEInfo-Producao': 'CEINFO-PRODUCAO',
    'CEINFO-Producao': 'CEINFO-PRODUCAO',
    'CEINFO-Producão': 'CEINFO-PRODUCAO',
    'CEInfo-Produção': 'CEINFO-PRODUCAO',
    'CEINFO- Produção': 'CEINFO-PRODUCAO',
    'CEINFO-Produção': 'CEINFO-PRODUCAO',
    'CEINFO-Produçãp': 'CEINFO-PRODUCAO',
    'CEP-Producao': 'CEP-PRODUCAO',
    'CGP-Producao': 'CGP-PRODUCAO',
    'CGP-Producão': 'CGP-PRODUCAO',
    'CGP-Produção': 'CGP-PRODUCAO',
    'COGERH-Producao': 'COGERH-PRODUCAO',
    'COVISA-Acerco': 'COVISA-ACERVO',
    'COVISA-Acervo': 'COVISA-ACERVO',
    'COVISA-ACERVO': 'COVISA-ACERVO',
    'Covisa-Producao': 'COVISA-PRODUCAO',
    'COVISA-Producao': 'COVISA-PRODUCAO',
    'COVISA-Produçao': 'COVISA-PRODUCAO',
    'COVISA-Produção': 'COVISA-PRODUCAO',
    'CRAEA-Producao': 'CRAEA-PRODUCAO',
    'CRATOD': 'CRATOD',
    'CRLeste-Produção': 'CRSLESTE-PRODUCAO',
    'CRSCentroOeste-Producao': 'CRSCENTROOESTE-PRODUCAO',
    'CRSCentroOeste-Produçao': 'CRSCENTROOESTE-PRODUCAO',
    'CRSCentroOeste-Produção': 'CRSCENTROOESTE-PRODUCAO',
    'CRSCentro-Producao': 'CRSCENTRO-PRODUCAO',
    'CRSLeste-Producao': 'CRSLESTE-PRODUCAO',
    'CRSLeste-Produção': 'CRSLESTE-PRODUCAO',
    'CRSNorte-Producao': 'CRSNORTE-PRODUCAO',
    'CRSSudeste-Producao': 'CRSSUDESTE-PRODUCAO',
    'CRSSul-Producao': 'CRSSUL-PRODUCAO',
    'CRSSUL-Producao': 'CRSSUL-PRODUCAO',
    'CRSSul-Produção': 'CRSSUL-PRODUCAO',
    'CRSSUL-Produção': 'CRSSUL-PRODUCAO',
    'CSMRCAA-Producao': 'CSMRCAA-PRODUCAO',
    'CSRLeste-Produção': 'CRSLESTE-PRODUCAO',
    'CTDprod': 'CTDPROD',
    'CTDPROD': 'CTDPROD',
    'CVEacervo': 'CVSACERVO',
    'CVEprod': 'CVEPROD',
    'CVEPROD': 'CVEPROD',
    'CVSacervo': 'CVSACERVO',
    'CVSACERVO': 'CVSACERVO',
    'CVSprod': 'CVSPROD',
    'CVSPROD': 'CVSPROD',
    'DST/Aids-Acervo': 'DST/AIDS-ACERVO',
    'DST/Aids-Producao': 'DST/AIDS-PRODUCAO',
    'DST-Aids-Produção': 'DST/AIDS-PRODUCAO',
    'DST/Aids- Produção': 'DST/AIDS-PRODUCAO',
    'DST/Aids-Produção': 'DST/AIDS-PRODUCAO',
    'DST prod': 'DSTPROD',
    'DST-prod': 'DSTPROD',
    'DSTprod': 'DSTPROD',
    'DSTPROD': 'DSTPROD',
    'EMS-acervo': 'EMS-ACERVO',
    'EMS- Acervo': 'EMS-ACERVO',
    'EMS-Acervo': 'EMS-ACERVO',
    'EMS-ACERVO': 'EMS-ACERVO',
    'EMS-Pordução': 'EMS-PRODUCAO',
    'EMS-Producao': 'EMS-PRODUCAO',
    'EMS-Producão': 'EMS-PRODUCAO',
    'EMS-produção': 'EMS-PRODUCAO',
    'EMS- Produção': 'EMS-PRODUCAO',
    'EMS-Produção': 'EMS-PRODUCAO',
    'ESPECALIZACAOSES': 'ESPECIALIZACAOSESPROD',
    'EspecialiaçãoSES': 'ESPECIALIZACAOSESPROD',
    'Especializacao SES': 'ESPECIALIZACAOSESPROD',
    'EspecializacaoSES': 'ESPECIALIZACAOSESPROD',
    'ESPECIALIZACAOSES': 'ESPECIALIZACAOSESPROD',
    'EspecializacãoSES': 'ESPECIALIZACAOSESPROD',
    'EspecializaçaoSES': 'ESPECIALIZACAOSESPROD',
    'Especialização SES': 'ESPECIALIZACAOSESPROD',
    'EspecializaçãoSES': 'ESPECIALIZACAOSESPROD',
    'ESPECIALIZACAOSESPROD': 'ESPECIALIZACAOSESPROD',
    'ESPECILIZACAOSES': 'ESPECIALIZACAOSESPROD',
    'EspecilizaçãoSES': 'ESPECIALIZACAOSESPROD',
    'ESPECILIZACAOSESPROD': 'ESPECIALIZACAOSESPROD',
    'FOLHETOCTD': 'FOLHETOCTD',
    'HMALMACERVO': 'HMLMBACERVO',
    'HMLBACERVO': 'HMLMBACERVO',
    'HMLBPROD': 'HMLMBPROD',
    'HMLMBACAERVO': 'HMLMBACERVO',
    'HMLMBACEERVO': 'HMLMBACERVO',
    'HMLMBACERO': 'HMLMBACERVO',
    'HMLMBacervo': 'HMLMBACERVO',
    'HMLMBACERVO': 'HMLMBACERVO',
    'HMLMBACEVO': 'HMLMBACERVO',
    'HMLMBAERVO': 'HMLMBACERVO',
    'HMLMBBPROD': 'HMLMBPROD',
    'HMLMBPPROD': 'HMLMBPROD',
    'HMLMBPRODE': 'HMLMBPROD',
    'HMLMBPROd': 'HMLMBPROD',
    'HMLMB/PROD': 'HMLMBPROD',
    'HMLMBPROD': 'HMLMBPROD',
    'HMLMBPRO': 'HMLMBPROD',
    'HMLMBPROS': 'HMLMBPROD',
    'HMLMHACERVO': 'HMLMBACERVO',
    'HMLMHPROD': 'HMLMBPROD',
    'HMLMMBACERVO': 'HMLMBACERVO',
    'HMLMMBPROD': 'HMLMBPROD',
    'HMLMPROD': 'HMLMBPROD',
    'HMMBPROD': 'HMLMBPROD',
    'HMMLMBPROD': 'HMLMBPROD',
    'HNMLMBPROD': 'HMLMBPROD',
    'HSMP-Produção': 'HSPM-PRODUCAO',
    'HSPM-Acervo': 'HSPM-ACERVO',
    'HSPM-Producao': 'HSPM-PRODUCAO',
    'HSPM-Produção': 'HSPM-PRODUCAO',
    'ialacervo': 'IALACERVO',
    'IAlacervo': 'IALACERVO',
    'IALacervo': 'IALACERVO',
    'IALACERVO': 'IALACERVO',
    'IALaprod': 'IALPROD',
    'IALPRD': 'IALPROD',
    'IALprod': 'IALPROD',
    'IAL PROD': 'IALPROD',
    'IALP|ROD': 'IALPROD',
    'IALPROD': 'IALPROD',
    'IAPprod': 'IPPROD',
    'IBacervo': 'IBACERVO',
    'IBACERVO': 'IBACERVO',
    'IBprod': 'IBPROD',
    'IBPROD': 'IBPROD',
    'ICFPROD': 'ICFPROD',
    'IDPCacervo': 'IDPCACERVO',
    'IDPCPROd': 'IDPCPROD',
    'IDPCPROD': 'IDPCPROD',
    'IIERPROD': 'IIERPROD',
    'ilslacervo': 'ILSLACERVO',
    'ILSLacervo': 'ILSLACERVO',
    'ILSL-Acervo': 'ILSLACERVO',
    'ILSLACERVO': 'ILSLACERVO',
    'ilslprod': 'ILSLPROD',
    'ILSLprod': 'ILSLPROD',
    'ILSLPROD': 'ILSLPROD',
    'ILSLSPROD': 'ILSLPROD',
    'IPACERVO': 'IPACERVO',
    'IPGGACERVO': 'IPGGACERVO',
    'IPGGPROD': 'IPGGPROD',
    'IPprod': 'IPPROD',
    'IPPROD': 'IPPROD',
    'ISAcervo': 'ISACERVO',
    'ISACERVO': 'ISACERVO',
    'ISACRVO': 'ISACERVO',
    'ISCAcervo': 'ISACERVO',
    'ISprod': 'ISPROD',
    'ISProd': 'ISPROD',
    'ISPROD': 'ISPROD',
    'LIISACERVO': 'ISACERVO',
    'LSLPROD': 'ILSLPROD',
    'MaePaulistana-Producao': 'MAEPAULISTANA-PRODUCAO',
    'MAEPAULISTANA-Producao': 'MAEPAULISTANA-PRODUCAO',
    'MAEPAULISTANA-Produção': 'MAEPAULISTANA-PRODUCAO',
    'MBoiMirim-Producao': 'MBOIMIRIM-PRODUCAO',
    'MBOIMIRIM-Producao': 'MBOIMIRIM-PRODUCAO',
    'NP-Producao': 'NP-PRODUCAO',
    'NP-Produção': 'NP-PRODUCAO',
    'PAPSESSP': 'PAPSESSP',
    'RARARSAUDE': 'RARASAUDE',
    'RARASAUDEE': 'RARASAUDE',
    'RARASAUDE': 'RARASAUDE',
    'RARASAÚDE': 'RARASAUDE',
    'REPOSITORIOBVS': 'REPOSITORIOBVS',
    'Repositório': 'REPOSITORIOBVS',
    'SAMU-Producao': 'SAMU-PRODUCAO',
    'SAMU-Produção': 'SAMU-PRODUCAO',
    'SaoLuizGonzaga-Producao': 'SAOLUIZGONZAGA-PRODUCAO',
    'SAOLUIZGONZAGA-Producao': 'SAOLUIZGONZAGA-PRODUCAO',
    'SESacervo': 'EMS-ACERVO',
    'SESACERVO': 'EMS-ACERVO',
    'SMS': 'SMS',
    'SPECIALIZACAOSES': 'ESPECIALIZACAOSESPROD',
    'SUCENprod': 'SUCENPROD',
    'SUCENPROD': 'SUCENPROD',
    'TATUAPE-Acervo': 'TATUAPE-ACERVO',
    'TATUAPÉ-Acervo': 'TATUAPE-ACERVO',
    'TATUAPE-Producao': 'TATUAPE-PRODUCAO',
    'TESESESP': 'TESESESSP',
    'Tesesessp': 'TESESESSP',
    'TESEsessp': 'TESESESSP',
    'TESESESSP': 'TESESESSP',
    'Tiradentes-Producao': 'TIRADENTES-PRODUCAO',
    'TIRADENTES-Producao': 'TIRADENTES-PRODUCAO',
    'UILSLPROD': 'ILSLPROD',
    'VilaMaria-Producao': 'VILAMARIA-PRODUCAO',
    'VILAMARIA-Producao': 'VILAMARIA-PRODUCAO',
}

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
# 3) Correção do campo "db"
# =========================
def corrigir_campo_db():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_iahx_xml', 'data_governance')

    query = {'db': {'$exists': True, '$ne': None}}
    total = collection.count_documents(query)
    logger.info(f"Iniciando correção de {total} docs com campo 'db'...")

    map_norm = {_norm_key(k): v for k, v in DB_MAP.items()}

    batch = []
    atualizados = 0
    processados = 0

    for doc in collection.find(query, projection={'db': 1, 'id': 1}):
        processados += 1
        original = doc.get('db')
        changed = False

        if isinstance(original, str):
            key = _norm_key(original)
            if key in map_norm:
                novo_valor = map_norm[key]
                if novo_valor != original:
                    batch.append(UpdateOne(
                        {'_id': doc['_id']},
                        {'$set': {'db': novo_valor}}
                    ))
                    atualizados += 1

        elif isinstance(original, list):
            novo_db = []
            for item in original:
                if isinstance(item, str):
                    key = _norm_key(item)
                    if key in map_norm:
                        novo_item = map_norm[key]
                        if novo_item != item:
                            changed = True
                        novo_db.append(novo_item)
                    else:
                        novo_db.append(item)
                else:
                    novo_db.append(item)

            if changed:
                batch.append(UpdateOne(
                    {'_id': doc['_id']},
                    {'$set': {'db': novo_db}}
                ))
                atualizados += 1

        if len(batch) >= BATCH_SIZE:
            collection.bulk_write(batch, ordered=False)
            logger.info(f"[db] atualizados {len(batch)} docs (parcial).")
            batch = []

    if batch:
        collection.bulk_write(batch, ordered=False)
        logger.info(f"[db] atualizados {len(batch)} docs (final).")

    logger.info(f"[db] processados: {processados}; docs com alteração em 'db': {atualizados}.")



# =========================
# 4) Correção do campo "database" usando DATABASE_MAP
# =========================
def corrigir_campo_database():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_iahx_xml', 'data_governance')
    mapa = {_norm_key(k): v for k, v in DATABASE_MAP.items()}

    query = {'database': {'$exists': True, '$ne': None}}
    total = collection.count_documents(query)
    logger.info(f"Iniciando correção de {total} docs com campo 'database'...")

    batch = []
    atualizados = 0
    processados = 0

    for doc in collection.find(query, projection={'database': 1, 'id': 1}):
        processados += 1
        original = doc.get('database')
        changed = False

        if isinstance(original, str):
            chave = _norm_key(original)
            if chave in mapa:
                novo_valor = mapa[chave]
                if novo_valor != original:
                    batch.append(UpdateOne(
                        {'_id': doc['_id']},
                        {'$set': {'database': novo_valor}}
                    ))
                    atualizados += 1

        elif isinstance(original, list):
            novo_database = []
            for item in original:
                if isinstance(item, str):
                    chave = _norm_key(item)
                    if chave in mapa:
                        novo_item = mapa[chave]
                        if novo_item != item:
                            changed = True
                        novo_database.append(novo_item)
                    else:
                        novo_database.append(item)
                else:
                    novo_database.append(item)

            if changed:
                batch.append(UpdateOne(
                    {'_id': doc['_id']},
                    {'$set': {'database': novo_database}}
                ))
                atualizados += 1

        if len(batch) >= BATCH_SIZE:
            collection.bulk_write(batch, ordered=False)
            logger.info(f"[database] atualizados {len(batch)} docs (parcial).")
            batch = []

    if batch:
        collection.bulk_write(batch, ordered=False)
        logger.info(f"[database] atualizados {len(batch)} docs (final).")

    logger.info(
        f"[database] processados: {processados}; "
        f"docs com alteração em 'database': {atualizados}."
    )


# =========================
# 5) Substituição do array "ai" usando mapeamento TABS.brisa_ai_11_17 (ai1 -> ai2)
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

    mapa = _carregar_mapa_ai(hook)

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
        'Corrige campos de país, corrige os campos db e database e substitui elementos '
        'do array ai usando mapeamento ai1->ai2 de TABS.brisa_ai_11_17'
    ),
    schedule=None,
    catchup=False,
    tags=['data_governance', 'fi-admin', 'mongodb', 'TABS']
) as dag:

    corrigir_paises = PythonOperator(
        task_id='corrigir_paises_no_mongo',
        python_callable=transformar_paises_mongodb
    )

    corrigir_db = PythonOperator(
        task_id='corrigir_campo_db',
        python_callable=corrigir_campo_db
    )

    corrigir_database = PythonOperator(
        task_id='corrigir_campo_database',
        python_callable=corrigir_campo_database
    )

    substituir_ai = PythonOperator(
        task_id='substituir_ai_por_mapeamento_tabs',
        python_callable=substituir_ai_por_mapeamento
    )

    corrigir_paises >> corrigir_db >> corrigir_database >> substituir_ai
