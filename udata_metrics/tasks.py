import logging

from udata.models import Dataset, Resource, Reuse, Organization
from udata.tasks import job

from udata_metrics.metrics import process_metrics_result

log = logging.getLogger(__name__)


def update_metrics_for_models():
    for target, model, id_key, value_key in [
        ('datasets', Dataset, 'dataset_id', 'visit'),  # 62239it [16:46, 61.87it/s]
        ('resources', Resource, 'resource_id', 'visit_resource'),  # 214417it [16:36, 215.28it/s]
        ('reuses', Reuse, 'reuse_id', 'visit'),  # 2997it [00:33, 90.47it/s]
        ('organizations', Organization, 'organization_id', 'visit_dataset'),  # 2033it [00:21, 95.19it/s]
    ]:
        process_metrics_result(target, model, id_key, value_key)


@job('update-metrics', route='low.metrics')
def update_metrics(self):
    '''Update udata objects metrics'''
    update_metrics_for_models()
