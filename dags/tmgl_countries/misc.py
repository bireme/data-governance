import logging
from airflow.providers.mongo.hooks.mongo import MongoHook


def get_eligible_countries():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('02_metrics_countries', 'tmgl_charts')
    eligible_countries = collection.find(
        {"country": {"$exists": True, "$ne": None}}, {'country': 1}
    ).distinct('country')

    return [[country] for country in eligible_countries]