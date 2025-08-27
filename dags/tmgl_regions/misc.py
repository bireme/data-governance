def get_regions(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$region_nome",
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