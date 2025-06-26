def _get_title_values(title_doc):
    values = []

    if title_doc.get('title'):
        values.append(title_doc['title'])

    if title_doc.get('shortened_title'):
        values.append(title_doc['shortened_title'])

    if title_doc.get('medline_shortened_title'):
        values.append(title_doc['medline_shortened_title'])

    if title_doc.get('parallel_titles'):
        values.extend(title_doc['parallel_titles'])

    if title_doc.get('shortened_parallel_titles'):
        values.extend(title_doc['shortened_parallel_titles'])

    if title_doc.get('other_titles'):
        other_titles = [other_title.split('^i')[0] for other_title in title_doc.get('other_titles')]
        values.extend(other_titles)

    return values


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
        english_desc = decs_doc.get('Descritor Inglês', '').strip().lower()
        if english_desc:
            # Processa MFN: remove zeros à esquerda e adiciona prefixo
            raw_mfn = decs_doc['Mfn'].lstrip('0')
            formatted_mfn = f'^d{raw_mfn}' if raw_mfn else None
            descriptor_map[english_desc] = formatted_mfn
    
    return descriptor_map