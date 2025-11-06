import logging
from airflow.providers.mongo.hooks.mongo import MongoHook
from data_governance.dags.tmgl_regions.misc import get_regions


def get_eligible_countries():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    who_region_collection = mongo_hook.get_collection('who_region', 'TABS')
    regions = get_regions(who_region_collection)
    eligible_countries = [[country] for countries in regions.values() for country in countries]

    return eligible_countries


def get_eligible_countries_for_export():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')
    eligible_countries = collection.find(
        {"country": {"$exists": True, "$ne": None}}, {'country': 1}
    ).distinct('country')

    return [[country] for country in eligible_countries]