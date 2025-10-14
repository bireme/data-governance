import re


def get_regions(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$who_region",
                "countries": {
                    "$addToSet": {
                        "$cond": [
                            {"$ifNull": ["$pais_tmgl", False]},
                            {"$toLower": "$pais_tmgl"},
                            {"$toLower": "$pais_en"}
                        ]
                    }
                }
            }
        }
    ]

    result = collection.aggregate(pipeline)
    regions = {doc['_id']: doc['countries'] for doc in result if doc['countries']}
    return regions


def get_country_data(collection, country_name):
    """
    Busca o primeiro documento onde pais_en ou pais_tmgl correspondam ao country_name.
    
    :param collection: coleção MongoDB
    :param country_name: nome do país para buscar
    :return: o primeiro documento que atender ao filtro ou None se não encontrado
    """
    escaped_country = re.escape(country_name)
    query = {
        "$or": [
            {"pais_en": {"$regex": f"^{escaped_country}$", "$options": "i"}},
            {"pais_tmgl": {"$regex": f"^{escaped_country}$", "$options": "i"}}
        ]
    }
    result = collection.find_one(query)
    return result


def load_areas(areas_col):
    """Mapeia códigos DECS para descritores em inglês"""
    areas_map = {}
    for area_doc in areas_col.find():
        code = area_doc.get('code_xml', '').strip()
        english_desc = area_doc.get('label_en', '').strip()
        if code and english_desc:
            areas_map[code] = english_desc
    return areas_map