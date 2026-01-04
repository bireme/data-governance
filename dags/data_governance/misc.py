import unicodedata


def _get_title_values(title_doc):
    values = []

    if title_doc.get('title'):
        values.append(title_doc['title'].split('^')[0])

    if title_doc.get('shortened_title'):
        values.append(title_doc['shortened_title'].split('^')[0])

    if title_doc.get('medline_shortened_title'):
        values.append(title_doc['medline_shortened_title'].split('^')[0])

    if title_doc.get('parallel_titles'):
        # Aplica split em cada título da lista
        parallel_titles = [title.split('^')[0] for title in title_doc['parallel_titles']]
        values.extend(parallel_titles)

    if title_doc.get('shortened_parallel_titles'):
        # Aplica split em cada título da lista
        shortened_parallel_titles = [title.split('^')[0] for title in title_doc['shortened_parallel_titles']]
        values.extend(shortened_parallel_titles)

    if title_doc.get('other_titles'):
        other_titles = [other_title.split('^')[0] for other_title in title_doc.get('other_titles')]
        values.extend(other_titles)

    return values


def remove_diacritics(s):
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join([c for c in nfkd if not unicodedata.combining(c)])


def load_title_current(title_col):
    issn_map = {}
    shortened_title_map = {}
    
    for title_doc in title_col.find():
        # Mapeamento por ISSN
        if 'issn' in title_doc and title_doc['issn']:
            issn_key = title_doc['issn'].lower().strip()
            issn_map[issn_key] = _get_title_values(title_doc)
        
        # Mapeamento por título serial (shortened_title)
        if 'shortened_title' in title_doc and title_doc['shortened_title']:
            title_key = title_doc['shortened_title'].lower().strip()
            shortened_title_map[title_key] = _get_title_values(title_doc)
    
    return issn_map, shortened_title_map


def load_title_current_country(title_col):
    shortened_title_map = {}
    
    for title_doc in title_col.find():
        # Mapeamento por título serial (shortened_title)
        if 'shortened_title' in title_doc and title_doc['shortened_title']:
            title_key = title_doc['shortened_title'].lower().strip()
            shortened_title_map[title_key] = title_doc.get('country', [])
    
    return shortened_title_map


def load_tabpais(tabpais_col):
    country_map = {}
    
    for country in tabpais_col.find():
        all_data = country.get('all', {})

        # Mapeia todos os valores e sinônimos
        for lang in ['pt', 'en', 'es', 'fr', 'país_2']:
            lang_value = all_data.get(lang, '')
            if lang_value:
                country_map[lang_value.lower().strip()] = all_data

        if all_data.get('sinonimo'):
            for synonym in all_data.get('sinonimo', []):
                country_map[synonym.lower().strip()] = all_data

    return country_map


def load_decs_descriptors(decs_col):    
    descriptor_map = {}
    for decs_doc in decs_col.find():
        # Processa MFN: remove zeros à esquerda e adiciona prefixo
        raw_mfn = decs_doc['Mfn'].lstrip('0')
        formatted_mfn = f'^d{raw_mfn}' if raw_mfn else None
        
        descriptors = [
            decs_doc.get('Descritor Inglês', ''),
            decs_doc.get('Descritor Português', ''),
            decs_doc.get('Descritor Espanhol', ''),
            decs_doc.get('Descritor Francês', '')
        ]
        descriptors = [remove_diacritics(desc.strip().lower()) for desc in descriptors if desc]

        for desc in descriptors:
            descriptor_map[desc] = formatted_mfn

    return descriptor_map


def load_instanceEcollection(collection):
    """
    Carrega em memória os campos 'instance', 'collection' e 'collection_instance'
    da coleção 'instanceEcollection' (database TABS), retornando um dicionário
    com chave pelo campo 'db' e valores dos campos não vazios.
    """    
    data = {}
    for doc in collection.find({}):
        db_key = doc.get("db")
        if not db_key:
            continue
        
        entry = {}
        if doc.get("instance"):
            entry["instance"] = doc["instance"]
        
        if doc.get("collection"):
            entry["collection"] = doc["collection"]
        
        if doc.get("collection_instance"):
            entry["collection_instance"] = doc["collection_instance"]
        
        if entry:
            data[db_key] = entry
    
    return data


def load_DBinstanceEcollection(collection):
    data = {}
    for doc in collection.find({}):
        db_key = doc.get("database_campo4")
        if not db_key:
            continue
        
        entry = {}
        if doc.get("instance"):
            entry["instance"] = doc["instance"]
        
        if doc.get("collection_instance"):
            entry["collection_instance"] = doc["collection_instance"]

        if doc.get("db"):
            entry["db"] = doc["db"]
        
        if entry:
            data[db_key] = entry
    
    return data