import re


def get_tmgl_country_query(country_name):
    """
    Gera uma query MongoDB para buscar documentos de um país específico.
    
    :param country_name: Nome do país a ser buscado.
    :return: Query MongoDB.
    """
    escaped_country = re.escape(country_name)
    escaped_country_underscore = country_name.replace(" ", "_")
    
    # Construir query com regex case-insensitive
    query = {
        "$or": [
            {"pais_afiliacao": {"$regex": f"\\^i{escaped_country}", "$options": "i"}},
            {"cp": escaped_country},
            {"who_regions": {"$regex": f"/{escaped_country_underscore}$", "$options": "i"}}
        ]
    }
    return query


def get_tmgl_country_query_no_subject(country_name):
    """
    Gera uma query MongoDB para buscar documentos de um país específico.
    
    :param country_name: Nome do país a ser buscado.
    :return: Query MongoDB.
    """
    escaped_country = re.escape(country_name)
    escaped_country_underscore = country_name.replace(" ", "_")
    
    # Construir query com regex case-insensitive
    query = {
        "$or": [
            {"pais_afiliacao": {"$regex": f"\\^i{escaped_country}", "$options": "i"}},
            {"cp": escaped_country}
        ]
    }
    return query