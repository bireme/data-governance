"""
# DG_02_create_iahx_xml_collection

Esta DAG realiza a transformação e padronização dos dados coletados do FI-Admin, preparando-os para importação no sistema **iah-X**. O processo envolve leitura de dados brutos da coleção `01_landing_zone`, transformação dos campos para o formato esperado, e gravação dos resultados na coleção `02_iahx_xml`, ambos no banco MongoDB.

## Objetivos

- **Padronizar campos bibliográficos** (título, resumo, autores, páginas, etc.) para múltiplos idiomas e formatos.
- **Gerar coleção compatível** com o fluxo de importação do iah-X.
- **Evitar duplicidades** usando operações `upsert` no MongoDB.

## Principais Transformações

- **Páginas**: Concatenação do primeiro e último número de página.
- **Títulos e Resumos**: Separação por idioma (ex: `ti_en`, `ti_pt`, `ab_es`).
- **Endereços Eletrônicos**: Extração de URLs.
- **Autores**: Geração de listas de nomes, afiliações e países.
- **Palavras-chave**: Extração de termos de autor.

## Campos Gerados

| Campo                       | Descrição                                            |
|-----------------------------|-----------------------------------------------------|
| vi                          | Volume                                              |
| ip                          | Número do fascículo                                 |
| ta                          | Título do periódico                                 |
| fo                          | Fonte                                               |
| is                          | ISSN                                                |
| cc                          | Código do centro cooperante                         |
| id_pk                       | Identificador primário                              |
| nivel_tratamento            | Nível de tratamento                                 |
| da                          | Data de publicação normalizada                      |
| la                          | Idioma do texto                                     |
| db                          | Base indexada                                       |
| dp                          | Ano de publicação                                   |
| pg                          | Páginas (ex: "123-130")                             |
| ti_xx                       | Título em cada idioma (ex: `ti_en`, `ti_pt`)        |
| ab_xx                       | Resumo em cada idioma (ex: `ab_en`, `ab_es`)        |
| ur                          | Lista de URLs                                       |
| kw                          | Palavras-chave do autor                             |
| au                          | Lista de autores                                    |
| af                          | Lista de afiliações                                 |
| afiliacao_autor             | Lista de autores com afiliação e país               |
| instituicao_pais_afiliacao  | Lista de afiliação + país                           |

## Fluxo da DAG

1. **Leitura**: Busca todos os documentos da coleção `01_landing_zone`.
2. **Transformação**: Aplica funções de padronização e enriquecimento dos campos.
3. **Gravação**: Escreve os documentos transformados na coleção `02_iahx_xml` usando `bulk_write` com `upsert`.
"""


import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo import ReplaceOne


def load_tabpais(tabpais_col):
    country_map = {}
    
    for country in tabpais_col.find():
        all_data = country.get('all', {})

        # Mapeia todos os valores e sinônimos
        for lang in ['pt', 'en', 'es', 'fr']:
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


def standardize_pages(value):
    pg_value = None
    if isinstance(value, list) and len(value) > 0:
        pages = value[0]
        if '_f' in pages and '_l' in pages:
            pg_value = f"{pages['_f']}-{pages['_l']}"
    return pg_value


def standardize_title(value):
    fields = {}
    if isinstance(value, list):
        for entry in value:
            if 'text' in entry:
                lang_code = entry.get('_i', '').lower()
                key = f'ti_{lang_code}' if lang_code else 'ti'
                fields[key] = entry['text']
    return fields


def standardize_abstract(value):
    """Processa abstracts com ou sem especificação de idioma"""
    fields = {}
    if isinstance(value, list):
        for entry in value:
            if 'text' in entry:
                lang_code = entry.get('_i', '').lower()
                key = f'ab_{lang_code}' if lang_code else 'ab'
                fields[key] = entry['text']
    return fields


def standardize_eletronic_address(value):
    fields = {}
    if isinstance(value, list):
        values = [
            entry['_u'] for entry in value
            if isinstance(entry, dict) and '_u' in entry and entry['_u'] and isinstance(entry['_u'], str)
        ]
        fields['ur'] = values

        if fields['ur']:
            fields['fulltext'] = 1
    return fields


def standardize_location(value):
    fields = {}
    if isinstance(value, list):
        values = [
            entry['text'] for entry in value
            if isinstance(entry, dict) and 'text' in entry and entry['text'] and isinstance(entry['text'], str)
        ]
        fields['lo'] = ' / '.join(values)
    return fields


def standardize_author_keyword(value):
    fields = {}
    if isinstance(value, list):
        fields['kw'] = [entry['text'] for entry in value if 'text' in entry]
    return fields


def standardize_fo(source, pages, publication_date, descriptive_information):
    """Processa o campo source e retorna os campos derivados"""
    fields = {}
    if source:
        fo = source
        if pages:
            fo += f": {pages}"
        if publication_date:
            fo += f", {publication_date}."
        if descriptive_information:
            values_di = [entry['_b'] for entry in descriptive_information if '_b' in entry and entry['_b']]
            values_di = ". ".join(values_di)
            fo += f" {values_di}"

        fields['fo'] = fo
    return fields


def standardize_individual_authors(authors):
    """Processa o campo individual_author e retorna os campos derivados"""
    result = {
        'au': [],
        'afiliacao_autor': [],
        'af': [],
        'instituicao_pais_afiliacao': [],
        'auid': [],
        'email': []
    }
    
    if not isinstance(authors, list):
        return result
    
    for author in authors:
        name = author.get('text', '')
        institution = author.get('_1', '')
        country = author.get('_p', '')
        auid = author.get('_k', '')
        email = author.get('_e', '')
        
        # Campo au
        if name:
            result['au'].append(name)
        
            # Campo afiliacao_autor
            parts = []
            if name:
                parts.append(name)
            if institution:
                parts.append(f"; {institution}" if parts else institution)
            if country:
                parts.append(f". {country}" if parts else country)

            if institution and parts:
                result['afiliacao_autor'].append(''.join(parts).lstrip('; '))
            else:
                result['afiliacao_autor'].append('s.af')
        
        # Campo af
        if institution:
            result['af'].append(institution)
        else:
            result['af'].append('s.af')

        if auid:
            result['auid'].append(auid)

        if email:
            result['email'].append(email)
        
        # Campo instituicao_pais_afiliacao
        if institution and country:
            result['instituicao_pais_afiliacao'].append(f"{institution}+{country}")
    
    return result


def standardize_id(id_pk, lilacs_original_id):
    id_value = None
    if lilacs_original_id:
        id_value = f"lil-{lilacs_original_id}"
    else:
        id_value = f"biblio-{id_pk}"
    return {'id': id_value}


def standardize_cp(publication_country, country_map):
    fields = {}
    if publication_country:
        matched = country_map.get(publication_country.lower())
        if matched:
            fields['cp'] = set()

            for lang in ['pt', 'en', 'es', 'fr', 'país_2']:
                value = matched.get(lang)
                if value:
                    fields['cp'].add(value)

            for synonym in matched.get('sinonimo', []):
                if synonym:
                    fields['cp'].add(synonym)

            fields['cp'] = list(fields['cp'])
    return fields


def transform_and_migrate():
    logger = logging.getLogger(__name__)
    
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_col = mongo_hook.get_collection('01_landing_zone', 'data_governance')
    target_col = mongo_hook.get_collection('02_iahx_xml', 'data_governance')

    tabpais_col = mongo_hook.get_collection('tabpais', 'TABS')
    country_map = load_tabpais(tabpais_col)

    decs_col = mongo_hook.get_collection('current', 'DECS')
    decs_map = load_decs_descriptors(decs_col)
    
    batch = []
    for doc in source_col.find():
        # processa paginas
        pg_value = None
        if 'pages' in doc:
            pg_value = standardize_pages(doc['pages'])

        # processa títulos multilíngues
        title_fields = {}
        if 'title' in doc:
            title_fields = standardize_title(doc['title'])

        # processa abstracts multilíngues
        abstract_fields = {}
        if 'abstract' in doc:
            abstract_fields = standardize_abstract(doc['abstract'])

        # processa electronic_address
        eletronic_fields = {}
        if 'electronic_address' in doc:
            eletronic_fields = standardize_eletronic_address(doc['electronic_address'])

        # processa individual_author
        individual_author_fields = {}
        if 'individual_author' in doc:
            individual_author_fields = standardize_individual_authors(doc['individual_author'])

        # processa author_keyword
        author_keyword_fields = {}
        if 'author_keyword' in doc:
            author_keyword_fields = standardize_author_keyword(doc['author_keyword'])

        # processa call_number
        location_fields = {}
        if 'call_number' in doc:
            location_fields = standardize_location(doc['call_number'])

        # processa fo
        fo_fields = {}
        if 'source' in doc:
            fo_fields = standardize_fo(doc.get('source'), pg_value, doc.get('publication_date'), doc.get('descriptive_information'))

        # processa cp
        cp_fields = {}
        if 'publication_country' in doc:
            cp_fields = standardize_cp(doc.get('publication_country'), country_map)

        # processa ct
        ct_values = []
        if 'check_tags' in doc and isinstance(doc['check_tags'], list):
            for tag in doc['check_tags']:
                formatted_mfn = decs_map.get(tag.strip().lower())
                if formatted_mfn:
                    ct_values.append(formatted_mfn)

        id_fields = standardize_id(doc.get('id'), doc.get('LILACS_original_id'))

        descritores_locais = doc.get('local_descriptors', '')
        descritores_locais = descritores_locais.splitlines() if isinstance(descritores_locais, str) else descritores_locais

        transformed = {
            '_id': doc['_id'],
            'ai': doc.get('corporate_author'),
            'aid': doc.get('doi_number'),
            'alternate_id': doc.get('alternate_ids'),
            'book_title': doc.get('title_monographic'),
            'cc': doc.get('cooperative_center_code'),
            'cn_co': doc.get('conferente_country'),
            'cn_cy': doc.get('conference_city'),
            'cn_da': doc.get('conference_nomalized_date'),
            'cn_dt': doc.get('conference_date'),
            'cn_in': doc.get('conference_sponsoring_institution'),
            'cn_na': doc.get('conference_name'),
            'ct': ct_values,
            'cy': doc.get('publication_city'),
            'da': doc.get('publication_date_normalized', '')[:6] if doc.get('publication_date_normalized') else None,
            'db': doc.get('indexed_database'),
            'descritores_locais': descritores_locais,
            'dp': doc.get('publication_date'),
            'ec': 1 if doc.get('clinical_trial_registry_name') else None,
            'ed': doc.get('edition'),
            'entry_date': doc.get('created_time', doc.get('transfer_date_to_database')), 
            'fo': doc.get('source'),
            'id_pk': doc.get('id'),
            'ip': doc.get('issue_number'),
            'is': doc.get('issn'),
            'isbn': doc.get('isbn'),
            'la': doc.get('text_language'),
            'license': doc.get('license'),
            'nivel_tratamento': doc.get('treatment_level'),
            'ot': descritores_locais,
            'pg': pg_value,
            'ta': doc.get('title_serial'),
            'vi': doc.get('volume_serial'),
            **id_fields,
            **title_fields,
            **abstract_fields,
            **eletronic_fields,
            **author_keyword_fields,
            **individual_author_fields,
            **location_fields,
            **fo_fields,
            **cp_fields
        }

        # Remove campos com valor None, '', [], ou {}
        transformed = {k: v for k, v in transformed.items() if v not in (None, '', [], {})}
        
        batch.append(ReplaceOne(
            {'_id': transformed['_id']},
            transformed,
            upsert=True
        ))
        
        if len(batch) >= 1000:
            target_col.bulk_write(batch)
            batch = []
    
    if batch:
        target_col.bulk_write(batch)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 15),
    'retries': 0
}

with DAG(
    'DG_02_create_iahx_xml_collection',
    default_args=default_args,
    description='Data Governance - Transforma a coleção em formato XML para importar no iah-X',
    tags=["data_governance", "fi-admin", "mongodb", "iahx"],
    schedule="0 3 * * *",
    catchup=False,
    doc_md=__doc__
) as dag:
    
    migration_task = PythonOperator(
        task_id='transform_and_migrate',
        python_callable=transform_and_migrate
    )