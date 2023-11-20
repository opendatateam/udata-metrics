from datetime import datetime
import logging
import requests

from flask import current_app

from udata.models import db, CommunityResource, Dataset, Resource, Reuse, Organization
from udata.tasks import job


log = logging.getLogger(__name__)


def save_model(model: db.Document, model_id: str, value: int) -> None:
    model_result = model.objects.filter(id=model_id).first()
    if not model_result:
        log.debug(f'{model.__name__} not found', extra={
            'id': model_id
        })
        return
    if model_result.metrics.get('views', 0) == value:
        # Metric hasn't changed, skip update (useful for objects that are slow when saving)
        return
    model_result.metrics['views'] = value
    try:
        model_result.save(signal_kwargs={'ignores': ['post_save']})
    except Exception as e:
        log.exception(e)


def iterate_on_metrics(target: str, value_key: str) -> dict:
    '''
    paginate on target endpoint
    '''
    with requests.Session() as session:
        url = f'{current_app.config["METRICS_API"]}/{target}_total/data/'
        url += f'?{value_key}__greater=1&page_size=50'
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        log.info(f'{data["meta"]["total"]} objects found')
        for row in data['data']:
            yield row
        while data['links'].get('next'):
            r = session.get(data['links'].get('next'), timeout=15)
            r.raise_for_status()
            data = r.json()
            for row in data['data']:
                yield row


def process_metrics_result(target_endpoint: str,
                           model: db.Document,
                           id_key: str,
                           value_key: str = 'visit') -> None:
    '''
    Fetch metrics and update udata objects with the total metrics count
    '''
    log.info(f'Processing model {model}')
    start = datetime.now()
    for data in iterate_on_metrics(target_endpoint, value_key):
        if model.__name__ == 'Resource':
            # Specific case for resource:
            # - it could either be a Dataset Resource embedded document or a CommunityResource
            # - it requires special performance improvement to prevent saving the entire document
            modified_count = Dataset.objects(resources__id=data[id_key]).update(
                **{'set__resources__$__metrics__views': data[value_key]}
            )
            if not modified_count:
                # No embedded resource found with this id, could be a CommunityResource
                save_model(CommunityResource, model_id=data[id_key], value=data[value_key])
        else:
            save_model(model, model_id=data[id_key], value=data[value_key])
    log.info(f'Done in {datetime.now() - start}')


def update_metrics_for_models():
    for target, model, id_key, value_key in [
        ('datasets', Dataset, 'dataset_id', 'visit'),
        ('resources', Resource, 'resource_id', 'download_resource'),
        ('reuses', Reuse, 'reuse_id', 'visit'),
        # We're currently using visit_dataset as global metric for an orga
        ('organizations', Organization, 'organization_id', 'visit_dataset')
    ]:
        process_metrics_result(target, model, id_key, value_key)


@job('update-metrics', route='low.metrics')
def update_metrics(self):
    '''Update udata objects metrics'''
    if not current_app.config['METRICS_API']:
        log.error('You need to set METRICS_API to run update-metrics')
        exit(1)
    update_metrics_for_models()
