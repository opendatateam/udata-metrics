import logging

from flask import current_app

from udata.models import Dataset, Resource, Reuse, Organization
from udata.tasks import job

from udata_metrics.metrics import process_metrics_result

log = logging.getLogger(__name__)


def update_metrics_for_models():
    for target, model, id_key, value_key in [
        ('datasets', Dataset, 'dataset_id', 'visit'),
        ('resources', Resource, 'resource_id', 'visit_resource'),
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
