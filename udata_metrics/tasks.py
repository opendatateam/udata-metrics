import logging

from udata_metrics.client import metrics_client_factory

log = logging.getLogger(__name__)


def aggregate_last_day():
    measurements_dict = {
        "reuse": "reuse.reuse_id",
        "resource": "resource.resource_id",
        "dataset": "dataset.dataset_id",
        "organization": "organization.organization_id",
        "resource_hit": "resource_hit.resource_id",
    }
    for _, measurement in measurements_dict.items():
        client = metrics_client_factory()
        client.aggregate_metrics(measurement)
