from datetime import datetime
import logging
import requests
from functools import wraps
import time

from flask import current_app

from udata.models import db, CommunityResource, Dataset, Resource, Reuse, Organization
from udata.tasks import job


log = logging.getLogger(__name__)

def log_timing(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        model = func.__name__.removeprefix('update_')
        log.info(f"Processing {model}…")
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        total_time = time.perf_counter() - start_time
        log.info(f'Done in {total_time:.4f} seconds.')
        return result
    return timeit_wrapper

def save_model(model: db.Document, model_id: str, key: str, value: int) -> None:
    try:
        result = model.objects(id=model_id).update(**{f'set__metrics__{key}': value})

        if result is None:
            log.debug(f'{model.__name__} not found', extra={
                'id': model_id
            })
    except Exception as e:
        log.exception(e)


def iterate_on_metrics(target: str, value_key: str, page_size: int = 50) -> dict:
    '''
    paginate on target endpoint
    '''
    url = f'{current_app.config["METRICS_API"]}/{target}_total/data/'
    url += f'?{value_key}__greater=1&page_size={page_size}'

    with requests.Session() as session:
        while url is not None:
            r = session.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            
            for row in data['data']:
                yield row

            url = data['links'].get('next')

@log_timing
def update_resources_and_community_resources():
    sum_of_resources_downloads = {}

    for data in iterate_on_metrics("resources", "download_resource"):
        if data['dataset_id'] is None:
            save_model(CommunityResource, data['resource_id'], 'views', data['download_resource'])
        else:
            sum_of_resources_downloads.setdefault(data['dataset_id'], 0)
            sum_of_resources_downloads[data['dataset_id']] += data['download_resource']

            Dataset.objects(resources__id=data['resource_id']).update(
                **{f'set__resources__$__metrics__views': data['download_resource']}
            )

    for dataset_id, sum in sum_of_resources_downloads.items():
        save_model(Dataset, dataset_id, 'number_of_resources_downloads', sum)

@log_timing
def update_datasets():
    for data in iterate_on_metrics("datasets", "visit"):
        save_model(Dataset, data['dataset_id'], 'views', data['visit'])

@log_timing
def update_reuses():
    for data in iterate_on_metrics("reuses", "visit"):
        save_model(Reuse, data['reuse_id'], 'views', data['visit'])

@log_timing
def update_organizations():
    # We're currently using visit_dataset as global metric for an orga
    for data in iterate_on_metrics("organizations", "visit"):
        save_model(Organization, data['organization_id'], 'views', data['visit'])


def update_metrics_for_models():
    update_datasets()
    update_resources_and_community_resources()
    update_reuses()
    update_organizations()


@job('update-metrics', route='low.metrics')
def update_metrics(self):
    '''Update udata objects metrics'''
    if not current_app.config['METRICS_API']:
        log.error('You need to set METRICS_API to run update-metrics')
        exit(1)
    update_metrics_for_models()
