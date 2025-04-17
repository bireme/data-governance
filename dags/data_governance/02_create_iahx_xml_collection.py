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


from datetime import datetime
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo import UpdateOne


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
            if '_u' in entry and isinstance(entry['_u'], str)
        ]
        fields['ur'] = values
    return fields


def standardize_author_keyword(value):
    fields = {}
    if isinstance(value, list):
        fields['kw'] = [entry['text'] for entry in value if 'text' in entry]
    return fields


def standardize_individual_authors(authors):
    """Processa o campo individual_author e retorna os campos derivados"""
    result = {
        'au': [],
        'afiliacao_autor': [],
        'af': [],
        'instituicao_pais_afiliacao': []
    }
    
    if not isinstance(authors, list):
        return result
    
    for author in authors:
        name = author.get('text', '')
        institution = author.get('_1', '')
        country = author.get('_p', '')
        
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
            if parts:
                result['afiliacao_autor'].append(''.join(parts).lstrip('; '))
        
        # Campo af
        if institution:
            result['af'].append(institution)
        
        # Campo instituicao_pais_afiliacao
        if institution and country:
            result['instituicao_pais_afiliacao'].append(f"{institution}+{country}")
    
    return result


def transform_and_migrate():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    source_col = mongo_hook.get_collection('01_landing_zone', 'data_governance')
    target_col = mongo_hook.get_collection('02_iahx_xml', 'data_governance')
    
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

        transformed = {
            '_id': doc['_id'],
            'vi': doc.get('volume_serial'),
            'ip': doc.get('issue_number'),
            'ta': doc.get('title_serial'),
            'fo': doc.get('source'),
            'is': doc.get('issn'),
            'cc': doc.get('cooperative_center_code'),
            'id_pk': doc.get('id'),
            'nivel_tratamento': doc.get('treatment_level'),
            'da': doc.get('publication_date_normalized'),
            'la': doc.get('text_language'),
            'db': doc.get('indexed_database'),
            'dp': doc.get('publication_date_normalized', '')[:4] if doc.get('publication_date_normalized') else None,
            'pg': pg_value,
            **title_fields,
            **abstract_fields,
            **eletronic_fields,
            **author_keyword_fields,
            **individual_author_fields
        }
        
        batch.append(UpdateOne(
            {'_id': transformed['_id']},
            {'$set': transformed},
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
    
    migration_task = PythonOperator(
        task_id='transform_and_migrate',
        python_callable=transform_and_migrate
    )