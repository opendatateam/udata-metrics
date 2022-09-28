import logging

from udata.tasks import job
from udata_metrics.client import metrics_client_factory

log = logging.getLogger(__name__)


@job('aggregate-metrics-last-day', route='low.metrics')
def aggregate_metrics_last_day(self):
    measurements_dict = {
        "reuse": "reuse.reuse_id",
        "resource": "resource.resource_id",
        "dataset": "dataset.dataset_id",
        "organization": "organization.organization_id",
        "resource_hit": "resource_hit.resource_id",
    }
    client = metrics_client_factory()
    for _, measurement in measurements_dict.items():
        client.aggregate_metrics(measurement)
