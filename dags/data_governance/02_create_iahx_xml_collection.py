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


import re
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo import ReplaceOne
from data_governance.dags.data_governance.misc import load_tabpais
from data_governance.dags.data_governance.misc import load_decs_descriptors
from data_governance.dags.data_governance.misc import load_title_current
from data_governance.dags.data_governance.misc import load_title_current_country
from data_governance.dags.data_governance.misc import remove_diacritics
from data_governance.dags.data_governance.misc import get_decs_mfn


def standardize_pages(value):
    pg_value = []
    if isinstance(value, list) and len(value) > 0:
        for page in value:
            if '_f' in page and page['_f'] and '_l' in page and page['_l']:
                pg_value.append(f"{page['_f']}-{page['_l']}")
            elif 'text' in page and page['text']:
                pg_value.append(page['text'])
            elif 'f' in page and page['f']:
                pg_value.append(page['f'])
    return pg_value


def standardize_title(value):
    fields = {}
    if isinstance(value, list):
        for entry in value:
            if 'text' in entry:
                lang_code = entry.get('_i', '').lower()
                key = f'ti_{lang_code}' if lang_code else 'ti'

                if key not in fields:
                    fields[key] = []
                fields[key].append(entry['text'])
    return fields


def standardize_multilingual_title(doc):
    fields = {}
    treatment_level = doc.get('treatment_level', '').lower()
    literature_type = doc.get('literature_type', '').lower()

    def has_en_title(title_list):
        if not isinstance(title_list, list):
            return False
        for entry in title_list:
            if entry.get('_i', '').lower() == 'en':
                return True
        return False

    if treatment_level.startswith('a'):
        title_list = doc.get('title', [])
        fields = standardize_title(title_list)
        if not has_en_title(title_list):
            english_title = doc.get('english_translated_title')
            if english_title:
                if 'ti_en' not in fields:
                    fields['ti_en'] = []
                fields['ti_en'].append(english_title)

    elif treatment_level.startswith('m'):
        title_list = doc.get('title_monographic', [])

        if treatment_level == 'mc' and literature_type in ('mc', 'm'):
            title_list += doc.get('title_collection', [])

        fields = standardize_title(title_list)
        if not has_en_title(title_list):
            english_title = doc.get('english_title_monographic')
            if english_title:
                if 'ti_en' not in fields:
                    fields['ti_en'] = []
                fields['ti_en'].append(english_title)

    elif treatment_level == 'c':
        title_list = doc.get('title_collection', [])
        fields = standardize_title(title_list)
        if not has_en_title(title_list):
            english_title = doc.get('english_title_collection')
            if english_title:
                if 'ti_en' not in fields:
                    fields['ti_en'] = []
                fields['ti_en'].append(english_title)

    return fields


def standardize_abstract(value):
    """Processa abstracts com ou sem especificação de idioma"""
    fields = {}
    if isinstance(value, list):
        for entry in value:
            if 'text' in entry:
                lang_code = entry.get('_i', '').lower()
                key = f'ab_{lang_code}' if lang_code else 'ab'

                abstract_text = entry['text'].replace('\r\n', ' ')
                abstract_text = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', ' ', abstract_text)
                fields[key] = abstract_text
    return fields


def standardize_eletronic_address(value):
    fields = {}
    if isinstance(value, list):
        fields['ur'] = []
        fields['ur_MULTIMEDIA'] = []
        fields['ur_AUDIO'] = []
        fields['ur_meta'] = []

        for entry in value:
            if isinstance(entry, dict) and '_u' in entry and entry['_u'] and isinstance(entry['_u'], str):
                if entry['_u'] and any(part in entry['_u'].lower() for part in ["www", "internet", "http"]):
                    fields['fulltext'] = 1

                fields['ur'].append(entry['_u'])

                if '_y' in entry and 'MULTIM' in entry['_y']:
                    fields['ur_MULTIMEDIA'].append(entry['_u'])

                if '_y' in entry and 'UDIO' in entry['_y']:
                    fields['ur_AUDIO'].append(entry['_u'])

                if '_x' in entry and entry['_x']:
                    fields['ur_meta'].append(entry['_u'])

    return fields


def standardize_location(value):
    def process_entry(entry):
        subfields = [
            ('_d', ', '), ('_e', ', '), ('_f', ', '), ('_g', ', '),
            ('_h', '. '), ('_i', ','), ('_j', '. '), ('_k', '. '),
            ('_l', '. '), ('_m', '. '), ('_n', '. '), ('_o', '. '),
            ('_p', '. '), ('_q', '. '), ('_r', '. '), ('_s', '. '),
            ('_t', '. '), ('_u', '. '), ('_v', '. '), ('_w', '. '),
            ('_x', '. '), ('_y', '. '), ('_z', '. '), ('_0', '. '),
            ('_1', '. '), ('_2', '. '), ('_3', '. '), ('_4', '. '),
            ('_5', '. ')
        ]  # _a, _b, _c tratados abaixo
        last_fields = [
            ('_7', '. '), ('_8', '. '), ('_9', '. ')
        ]  # _6 tratado abaixo

        result = ''
        text = entry.get('text', '').strip()
        if text:
            result = text + ';'

        used_a = False
        used_b = False

        # _a
        a = entry.get('_a', '').strip()
        if a:
            result += (' ' if text else '') + a
            used_a = True

        # _b
        b = entry.get('_b', '').strip()
        if b:
            result += (', ' if used_a else '') + b
            used_b = True

        # _c
        c = entry.get('_c', '').strip()
        if c:
            result += (', ' if used_a or used_b else '') + c

        for key, sep in subfields:
            val = entry.get(key, '').strip()
            if val:
                result += sep + val

        # _6 (antes de _7/_8/_9)
        six = entry.get('_6', '').strip()
        if six:
            if not text:
                result += '. ' + six
            else:
                result += ' ' + six

        # _7, _8, _9
        for key, sep in last_fields:
            val = entry.get(key, '').strip()
            if val:
                result += sep + val

        # Remove o ; final caso ele fique isolado
        if result.strip().endswith(';'):
            result = result.strip()[:-1].rstrip()

        return result.strip()

    fields = {}
    if isinstance(value, list):
        parts = []
        for entry in value:
            if isinstance(entry, dict):
                phrase = process_entry(entry)
                if phrase:
                    parts.append(phrase)
        if parts:
            fields['lo'] = ' / '.join(parts)
    return fields


def standardize_fo(doc):
    def format_fo_as(doc):
        parts = []
        title_serial = '; '.join(doc.get('title_serial', [])) if isinstance(doc.get('title_serial'), list) else doc.get('title_serial')
        if title_serial:
            parts.append(title_serial)
        if doc.get('volume_serial'):
            parts.append(f";{doc['volume_serial']}")
        if doc.get('issue_number'):
            parts.append(f"({doc['issue_number']})")

        # páginas
        pages_f, pages_l, pages_text = None, None, None
        if doc.get('pages'):
            for page in doc['pages']:
                if '_f' in page and page['_f']:
                    pages_f = page['_f']
                if '_l' in page and page['_l']:
                    pages_l = page['_l']
                if 'text' in page and page['text']:
                    pages_text = page['text']
        if pages_f:
            parts.append(f": {pages_f}")
        if pages_l:
            parts.append(f"-{pages_l}")
        if pages_text:
            parts.append(f"{pages_text}")
        if doc.get('publication_date'):
            parts.append(f", {doc['publication_date']}.")

        # descriptive_information _b
        if doc.get('descriptive_information'):
            desc_b = [entry['_b'] for entry in doc['descriptive_information'] if '_b' in entry and entry['_b']]
            if desc_b:
                parts.append(' ' + ', '.join(desc_b))
        return ''.join(parts).strip()

    def format_fo_am(doc):
        parts = []

        has_individual_author = 'individual_author_monographic' in doc and doc['individual_author_monographic']
        has_corporate_author_monographic = 'corporate_author_monographic' in doc and doc['corporate_author_monographic']
        title_serial = '; '.join(doc.get('title_serial', [])) if isinstance(doc.get('title_serial'), list) else doc.get('title_serial')
        symbol = '; '.join(doc.get('symbol', [])) if isinstance(doc.get('symbol'), list) else doc.get('symbol')
        if has_individual_author or has_corporate_author_monographic:
            parts.append('In. ')
            if has_individual_author:
                authors = [a['text'] for a in doc['individual_author_monographic'] if 'text' in a]
                if authors:
                    parts.append('; '.join(authors) + '. ')
            elif has_corporate_author_monographic:
                authors = [a['text'] for a in doc['corporate_author_monographic'] if 'text' in a]
                if authors:
                    parts.append('; '.join(authors) + '. ')
        
        if 'title_monographic' in doc:
            titles = [t['text'] for t in doc['title_monographic'] if 'text' in t]
            if titles:
                parts.append(' / '.join(titles) + '.')
        if doc.get('publication_city'):
            parts.append(f" {doc['publication_city']}, ")
        if doc.get('publisher'):
            publishers = doc['publisher'] if isinstance(doc['publisher'], list) else doc['publisher'].splitlines()
            parts.append('; '.join(publishers) + ', ')
        if doc.get('edition'):
            editions = doc['edition'] if isinstance(doc['edition'], list) else doc['edition'].splitlines()
            parts.append('; '.join(editions) + ', ')
        if doc.get('publication_date'):
            parts.append(doc['publication_date'] + '.')

        # páginas
        pages_f, pages_l, pages_text = None, None, None
        if doc.get('pages'):
            for page in doc['pages']:
                if '_f' in page and page['_f']:
                    pages_f = page['_f']
                if '_l' in page and page['_l']:
                    pages_l = page['_l']
                if 'text' in page and page['text']:
                    pages_text = page['text']
        if pages_f and pages_l:
            parts.append(f" p. {pages_f}-{pages_l}")
        if pages_text:
            parts.append(f" p. {pages_text}")

        if doc.get('descriptive_information'):
            desc_b = [entry['_b'] for entry in doc['descriptive_information'] if '_b' in entry and entry['_b']]
            if desc_b:
                parts.append(', ' + ', '.join(desc_b) + '. ')
        if title_serial:
            parts.append('(' + title_serial)
        if doc.get('volume_serial'):
            parts.append(', ' + doc['volume_serial'])
        if doc.get('issue_number'):
            parts.append(', ' + doc['issue_number'])
        if title_serial:
            parts.append(').')
        if symbol:
            parts.append(' (' + symbol + ').')
        return ''.join(parts).strip()

    def format_fo_m(doc):
        parts = []
        has_pub_city = bool(doc.get('publication_city'))
        has_edition = bool(doc.get('edition'))
        has_publisher = bool(doc.get('publisher'))
        title_serial = '; '.join(doc.get('title_serial', [])) if isinstance(doc.get('title_serial'), list) else doc.get('title_serial')
        symbol = '; '.join(doc.get('symbol', [])) if isinstance(doc.get('symbol'), list) else doc.get('symbol')
        if has_pub_city or has_edition or has_publisher:
            if doc.get('publication_city'):
                parts.append(doc['publication_city'] + '; ')
            if doc.get('publisher'):
                publishers = doc['publisher'] if isinstance(doc['publisher'], list) else doc['publisher'].splitlines()
                parts.append('; '.join(publishers) + '; ')
            if doc.get('edition'):
                editions = doc['edition'] if isinstance(doc['edition'], list) else doc['edition'].splitlines()
                parts.append('; '.join(editions) + '; ')
            if doc.get('publication_date'):
                parts.append(doc['publication_date'] + '. ')
            if doc.get('pages_monographic'):
                pages_m = doc['pages_monographic']
                if 'p' in pages_m:
                    parts.append(pages_m + ' ')
                else:
                    parts.append(pages_m + ' p. ')
            if doc.get('descriptive_information'):
                desc_b = [entry['_b'] for entry in doc['descriptive_information'] if '_b' in entry and entry['_b']]
                if desc_b:
                    parts.append(', '.join(desc_b) + '.')
            if title_serial:
                parts.append('(' + title_serial)
            if doc.get('volume_serial'):
                parts.append(', ' + doc['volume_serial'])
            if doc.get('issue_number'):
                parts.append(', ' + doc['issue_number'])
            if title_serial:
                parts.append(').')
            if symbol:
                parts.append(' (' + symbol + ').')
        else:
            if title_serial:
                parts.append('(' + title_serial)
            if doc.get('volume_serial'):
                parts.append(', ' + doc['volume_serial'])
            if doc.get('issue_number'):
                parts.append(', ' + doc['issue_number'])
            if title_serial:
                parts.append(').')
            if symbol:
                parts.append(' (' + symbol + ').')
        return ''.join(parts).strip()

    def format_fo_c(doc):
        parts = []
        if doc.get('publication_city'):
            parts.append(doc['publication_city'] + '; ')
            if doc.get('publisher'):
                publishers = doc['publisher'] if isinstance(doc['publisher'], list) else doc['publisher'].splitlines()
                parts.append('; '.join(publishers) + '; ')
            if doc.get('edition'):
                editions = doc['edition'] if isinstance(doc['edition'], list) else doc['edition'].splitlines()
                parts.append('; '.join(editions) + '; ')
            if doc.get('publication_date'):
                parts.append(doc['publication_date'] + '. ')
            if doc.get('pages_monographic'):
                pages_m = doc['pages_monographic']
                if 'p' in pages_m:
                    parts.append(pages_m + ' ')
                else:
                    parts.append(pages_m + ' p. ')
            if doc.get('descriptive_information'):
                desc_b = [entry['_b'] for entry in doc['descriptive_information'] if '_b' in entry and entry['_b']]
                if desc_b:
                    parts.append(', '.join(desc_b) + '.')
        return ''.join(parts).strip()

    treatment_level = doc.get('treatment_level', '').lower()
    if treatment_level == 'as':
        return {'fo': format_fo_as(doc)}
    elif treatment_level.startswith('am'):
        return {'fo': format_fo_am(doc)}
    elif treatment_level.startswith('m'):
        return {'fo': format_fo_m(doc)}
    elif treatment_level == 'c':
        return {'fo': format_fo_c(doc)}
    else:
        return {}


def standardize_author_keyword(value):
    fields = {}
    if isinstance(value, list):
        fields['kw'] = [entry['text'] for entry in value if 'text' in entry]
    return fields


def standardize_individual_authors(authors, country_map):
    """Processa o campo individual_author e retorna os campos derivados"""
    result = {
        'au': [],
        'afiliacao_autor': [],
        'af': [],
        'instituicao_pais_afiliacao': [],
        'pais_afiliacao': [],
        'auid': [],
        'email': []
    }
    
    if not isinstance(authors, list):
        return result
    
    for author in authors:
        name = author.get('text', '')
        institution = author.get('_1', '')
        institution2 = author.get('_2', '')
        institution3 = author.get('_3', '')
        country = author.get('_p', '')
        city = author.get('_c', '')
        auid = author.get('_k', author.get('_w', ''))
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
            if institution2:
                parts.append(f". {institution2}" if parts else institution2)
            if institution3:
                parts.append(f". {institution3}" if parts else institution3)
            if city:
                parts.append(f". {city}" if parts else city)
            if country:
                parts.append(f". {country}" if parts else country)

            if institution and parts:
                result['afiliacao_autor'].append(''.join(parts).lstrip('; '))
            else:
                result['afiliacao_autor'].append('s.af')
        
        # Campo af
        if institution:
            if institution not in result['af']:
                result['af'].append(institution)
        else:
            result['af'].append('s.af')

        if institution2 and institution2 not in result['af']:
            result['af'].append(institution2)

        if institution3 and institution3 not in result['af']:
            result['af'].append(institution3)

        if city and city not in result['af']:
            result['af'].append(city)

        if auid:
            result['auid'].append(auid)

        if email:
            result['email'].append(email)

        if country:
            matched = country_map.get(country.lower())
            if matched:
                pais_afiliacao = f'^i{matched.get("en")}^e{matched.get("es")}^p{matched.get("pt")}^f{matched.get("fr")}'
                if pais_afiliacao not in result['pais_afiliacao']:
                    result['pais_afiliacao'].append(pais_afiliacao)

        # Campo instituicao_pais_afiliacao
        if institution and country:
            instituicao_pais_afiliacao = f"{institution}+{country}"
            if instituicao_pais_afiliacao not in result['instituicao_pais_afiliacao']:
                result['instituicao_pais_afiliacao'].append(instituicao_pais_afiliacao)

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


def standardize_pais_publicacao(publication_country, country_map):
    fields = {}
    matched = country_map.get(publication_country.lower())
    if matched:
        fields['pais_publicacao'] = f'^i{matched.get("en")}^e{matched.get("es")}^p{matched.get("pt")}^f{matched.get("fr")}'

    return fields


def standardize_ta_var(doc, issn_map, shortened_title_map):
    ta_var = None

    if doc.get('issn'):
        issn_key = doc['issn'].lower().strip()
        ta_var = issn_map.get(issn_key)
    
    if not ta_var and doc.get('shortened_title'):
        title_key = doc['shortened_title'].lower().strip()
        ta_var = shortened_title_map.get(title_key)

    return ta_var
    

def standardize_ta_fascic(ta_var, volume_serial, issue_number, year):
    if not ta_var:
        return []

    # Monta o sufixo apenas com os campos presentes
    suffix_parts = []
    if volume_serial:
        suffix_parts.append(volume_serial)
    if issue_number:
        suffix_parts.append(f"({issue_number})")
    # Junta os campos presentes com separadores adequados, sem espaços extras
    suffix = '; ' + ' '.join(suffix_parts) + ', ' + year
    return ta_var + [item + suffix for item in ta_var]


def determine_document_type(doc):
    """Identifica os tipos de documento com base em literature_type e outros campos"""
    literature_type = doc.get('literature_type', '').lower()
    electronic_address = doc.get('electronic_address', []) or []
    
    types = set()
    
    # 1. Mapeamento básico por literature_type
    base_types = {
        's': 'article',
        'm': 'monography',
        'n': 'non-conventional',
        't': 'thesis'
    }
    for base_type in base_types.keys():
        if base_type in literature_type:
            types.add(base_types[base_type])
    
    # 2. Tipos para códigos específicos
    congress_codes = {'mc', 'mcp', 'msc', 'nc', 'sc', 'scp'}
    project_codes = {'mcp', 'mp', 'msp', 'np', 'scp', 'sp'}

    if literature_type in congress_codes:
        types.add('congress and conference')
    if literature_type in project_codes:
        types.add('project document')
    
    # 3. Verificação de mídia em electronic_address
    video_extensions = {'.mp4', '.avi', '.wmv', '.mpeg', '.mpe', '.mpg'}
    audio_extensions = {'.wma', '.mp3', '.mp4', '.wav'}
    
    for entry in electronic_address:
        if isinstance(entry, dict):
            y_val = entry.get('_y', '').lower()
            z_val = entry.get('_z', '').lower()
            u_val = entry.get('_u', '').lower()
            q_val = entry.get('_q', '').lower()
            
            # Verificação de vídeo
            if any((
                'multim' in y_val,
                'deo' in z_val,
                any(ext in u_val for ext in video_extensions)
            )):
                types.add('video')

            # Verificação de podcast
            if any((
                'UDIO' in y_val,
                any(ext in q_val for ext in audio_extensions),
                any(ext in u_val for ext in audio_extensions)
            )):
                types.add('podcast')
    
    return list(types)


def calculate_weight(doc):
    """Calcula o score baseado em múltiplos critérios do documento"""
    score = 0

    # 1. Valor base pelo tipo
    tipo = doc.get('literature_type', '').lower()
    if tipo.startswith('s'):
        score += 15
    elif tipo.startswith('t'):
        score += 10
    elif tipo.startswith('m'):
        score += 5
    elif tipo.startswith('n'):
        score += 2

    # 2. Diferença de anos (20 - (ano_atual - ano_publicacao))
    try:
        publication_year = int(doc.get('publication_date_normalized', '')[:4])
        current_year = datetime.now().year
        diff_anos = current_year - publication_year
        score += (20 - diff_anos)
    except (TypeError, ValueError):
        pass

    # 3. Bônus por resumo
    if doc.get('abstract'):
        score += 5

    # 4. Bônus por URLs
    if doc.get('electronic_address'):
        score += 5

    return max(score, 0)


def setup_iahx_xml_collection():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    db = mongo_hook.get_conn()
    collection_name = '02_iahx_xml'
    db_name = 'data_governance'

    # Deleta a coleção se existir
    if collection_name in db[db_name].list_collection_names():
        db[db_name].drop_collection(collection_name)

    # Cria a coleção
    new_collection = db[db_name][collection_name]
    new_collection.create_index([('id', 1)])
    new_collection.create_index([('id_pk', 1)])


def extract_susdigital_theme(value, target_path):
    """
    Extrai o tema SUS Digital baseado no community_collection_path.
    
    Args:
        value (str): Campo com string multilíngue separada por '|'
        target_path (str): 'Programas' ou 'Alvo'
    
    Returns:
        dict: {'tema_susdigital_programas': str} ou {'tema_susdigital_publico_alvo': str}
    """
    if not isinstance(value, str) or not value.strip():
        return {}
    
    occurrences = value.split('|')
    
    # Procura pela versão pt-br ou pt
    pt_version = None
    for occurrence in occurrences:
        if 'pt-br' in occurrence or 'pt' in occurrence:
            pt_version = occurrence.strip()
            break
    
    if not pt_version:
        return {}
    
    # Pega a string após o último '/'
    parts = pt_version.split('/')
    if parts:
        theme = parts[-1].strip()
        field_name = 'tema_susdigital_programas' if target_path == 'Programas' else 'tema_susdigital_publico_alvo'
        return {field_name: theme}
    
    return {}


def transform_and_migrate():
    logger = logging.getLogger(__name__)
    
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_col = mongo_hook.get_collection('01_landing_zone', 'data_governance')
    target_col = mongo_hook.get_collection('02_iahx_xml', 'data_governance')

    tabpais_col = mongo_hook.get_collection('tabpais', 'TABS')
    country_map = load_tabpais(tabpais_col)

    decs_col = mongo_hook.get_collection('current', 'DECS')
    decs_map = load_decs_descriptors(decs_col)

    # Carrega TITLE.current e cria mapeamentos
    title_col = mongo_hook.get_collection('current', 'TITLE')
    issn_map, shortened_title_map = load_title_current(title_col)
    shortened_title_country = load_title_current_country(title_col)

    batch = []

    query = {
        "status": {"$in": [0, 1, -2, -3]},
        "treatment_level": {"$exists": True, "$nin": [None, ""]}
    }
    for doc in source_col.find(query):
        # processa paginas
        pg_value = None
        if 'pages' in doc:
            pg_value = standardize_pages(doc['pages'])
        elif 'pages_monographic' in doc:
            pg_value = doc.get('pages_monographic')

        # processa títulos multilíngues
        title_fields = standardize_multilingual_title(doc)

        # processa abstracts multilíngues
        abstract_fields = {}
        if 'abstract' in doc:
            abstract_fields = standardize_abstract(doc['abstract'])

        # processa electronic_address
        eletronic_fields = {}
        if 'electronic_address' in doc:
            eletronic_fields = standardize_eletronic_address(doc['electronic_address'])

        # processa individual_author
        author_fields = {}
        if 'individual_author' in doc:
            author_fields = standardize_individual_authors(doc['individual_author'], country_map)
        elif 'corporate_author' in doc:
            author_fields = standardize_individual_authors(doc['corporate_author'], country_map)
        elif 'individual_author_monographic' in doc:
            author_fields = standardize_individual_authors(doc['individual_author_monographic'], country_map)
        elif 'corporate_author_monographic' in doc:
            author_fields = standardize_individual_authors(doc['corporate_author_monographic'], country_map)
        elif 'individual_author_collection' in doc:
            author_fields = standardize_individual_authors(doc['individual_author_collection'], country_map)
        elif 'corporate_author_collection' in doc:
            author_fields = standardize_individual_authors(doc['corporate_author_collection'], country_map)

        # processa author_keyword
        author_keyword_fields = {}
        if 'author_keyword' in doc:
            author_keyword_fields = standardize_author_keyword(doc['author_keyword'])

        # processa call_number
        location_fields = {}
        if 'call_number' in doc:
            location_fields = standardize_location(doc['call_number'])

        # processa fo
        fo_fields = standardize_fo(doc)

        # processa cp e pais_publicacao
        cp_fields = {}
        pais_publicacao_fields = {}
        if 'publication_country' in doc:
            publication_country = doc.get('publication_country')
        elif 'title_serial' in doc:
            publication_country = shortened_title_country.get(doc.get('title_serial').lower().strip(), [])
            if publication_country:
                publication_country = publication_country[0]

        if publication_country:
            cp_fields = standardize_cp(publication_country, country_map)
            pais_publicacao_fields = standardize_pais_publicacao(publication_country, country_map)

        # processa ct
        ct_values = []
        if 'check_tags' in doc and isinstance(doc['check_tags'], list):
            for tag in doc['check_tags']:
                formatted_mfn = get_decs_mfn(tag, decs_map)
                if formatted_mfn:
                    ct_values.append(formatted_mfn)

        # processa pt
        pt_values = []
        if 'publication_type' in doc and isinstance(doc['publication_type'], list):
            for tag in doc['publication_type']:
                formatted_mfn = get_decs_mfn(tag, decs_map)
                if formatted_mfn:
                    pt_values.append(formatted_mfn)

        # processa mj
        mj_values = []
        if 'descriptors_primary' in doc and isinstance(doc['descriptors_primary'], list):
            for tag in doc['descriptors_primary']:
                if 'text' in tag:
                    tag = tag['text']

                    formatted_mfn = get_decs_mfn(tag.replace('^d', ''), decs_map)
                    if formatted_mfn:
                        mj_values.append(formatted_mfn)

        # processa mh
        mh_values = []
        if 'descriptors_secondary' in doc and isinstance(doc['descriptors_secondary'], list):
            for tag in doc['descriptors_secondary']:
                if 'text' in tag:
                    tag = tag['text']

                    formatted_mfn = get_decs_mfn(tag.replace('^d', ''), decs_map)
                    if formatted_mfn:
                        mh_values.append(formatted_mfn)

        # Extrai temas SUS Digital baseado em tag_colecao
        susdigital_fields = {}
        community_collection_path = doc.get('community_collection_path', '')
        if 'Programas' in community_collection_path:
            susdigital_fields = extract_susdigital_theme(community_collection_path, 'Programas')
        elif 'Alvo' in community_collection_path:
            susdigital_fields = extract_susdigital_theme(community_collection_path, 'Alvo')

        id_fields = standardize_id(doc.get('id'), doc.get('LILACS_original_id'))

        descritores_locais = doc.get('local_descriptors', '')
        descritores_locais = descritores_locais.splitlines() if isinstance(descritores_locais, str) else descritores_locais

        ta_var = standardize_ta_var(doc, issn_map, shortened_title_map)

        status_map = {
            -3: "Migrado",
            -2: "Coletado",
            -1: "Rascunho",
            0: "LILACS-Express",
            1: "Publicado",
            2: "Recusado",
            3: "Apagado"
        }

        transformed = {
            '_id': doc['_id'],
            'ai': [corp.get('text') for corp in doc.get('corporate_author', []) + doc.get('corporate_author_monographic', [])],
            'aid': doc.get('doi_number'),
            'alternate_id': [alternate_id for alternate_id in doc.get('alternate_ids', []) if alternate_id and alternate_id != id_fields['id']],
            'book_title': (
                next(
                    (
                        tm.get('text')
                        for tm in doc.get('title_monographic', [])
                    ),
                    None
                )
                if doc.get('title_monographic')
                    and not (
                        doc.get('literature_type', '').upper() in ['T', 'N']
                        and not doc.get('treatment_level', '').lower().startswith('a')
                    )
                else None
            ),
            'cc': doc.get('cooperative_center_code'),
            'cn_co': doc.get('conference_country'),
            'cn_cy': doc.get('conference_city'),
            'cn_da': doc.get('conference_normalized_date'),
            'cn_dt': doc.get('conference_date'),
            'cn_in': doc.get('conference_sponsoring_institution'),
            'cn_na': doc.get('conference_name'),
            'ct': ct_values,
            'cy': doc.get('publication_city'),
            'da': doc.get('publication_date_normalized', '')[:6] if doc.get('publication_date_normalized') else None,
            'database': doc.get('database'),
            'db': doc.get('indexed_database'),
            'descritores_locais': descritores_locais,
            'dp': doc.get('publication_date'),
            'ec': 1 if doc.get('clinical_trial_registry_name') else None,
            'ed': doc.get('edition'),
            'entry_date': doc.get('created_time', doc.get('transfer_date_to_database', ''))[:10].replace('-', ''), 
            'id_pk': doc.get('id'),
            'ip': doc.get('issue_number'),
            'is': doc.get('issn'),
            'isbn': doc.get('isbn'),
            'la': doc.get('text_language'),
            'license': doc.get('license'),
            'mh': mh_values,
            'mj': mj_values,
            'nivel_tratamento': doc.get('treatment_level'),
            'no_indexing': 1 if not doc.get('descriptors_primary') and not doc.get('descriptors_secondary') else None,
            'non_decs_region': doc.get('non_decs_region'),
            'ntv': doc.get('total_number_of_volumes'),
            'ot': descritores_locais,
            'pg': pg_value,
            'pr_in': doc.get('project_sponsoring_institution'),
            'pr_na': doc.get('project_name'),
            'pr_nu': doc.get('project_number'),
            'pt': pt_values,
            'pu': doc.get('publisher'),
            'related_research': [str(related) for related in doc.get('related_research', [])] if isinstance(doc.get('related_research', []), list) else doc.get('related_research', ''),
            'related_resource': [str(related) for related in doc.get('related_resource', [])] if isinstance(doc.get('related_resource', []), list) else doc.get('related_resource', ''),
            'status_fiadmin': status_map.get(doc.get('status')),
            'ta': doc.get('title_serial'),
            'ta_fascic': standardize_ta_fascic(ta_var, doc.get('volume_serial'), doc.get('issue_number'), doc.get('publication_date_normalized', '')[:4]),
            'ta_var': ta_var,
            'tag_comunidade': doc.get('community'),
            'tag_colecao': doc.get('community_collection_path'),
            'th_in': doc.get('thesis_dissertation_institution'),
            'th_le': [leader['text'] for leader in doc.get('thesis_dissertation_leader', []) if 'text' in leader],
            'th_ti': doc.get('thesis_dissertation_academic_title'),
            'tombo': doc.get('inventory_number'),
            'type': determine_document_type(doc),
            'update_date': doc.get('updated_time', '')[:10].replace('-', ''),
            'vi': [v for v in [doc.get('volume_monographic'), doc.get('volume_serial')] if v],
            'weight': calculate_weight(doc),
            **id_fields,
            **title_fields,
            **abstract_fields,
            **eletronic_fields,
            **author_keyword_fields,
            **author_fields,
            **location_fields,
            **fo_fields,
            **cp_fields,
            **pais_publicacao_fields,
            **susdigital_fields,
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
    schedule=None,
    catchup=False,
    doc_md=__doc__
) as dag:
    setup_iahx_xml_collection_task = PythonOperator(
        task_id='setup_iahx_xml_collection',
        python_callable=setup_iahx_xml_collection
    )
    migration_task = PythonOperator(
        task_id='transform_and_migrate',
        python_callable=transform_and_migrate
    )

    setup_iahx_xml_collection_task >> migration_task
