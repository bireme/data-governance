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