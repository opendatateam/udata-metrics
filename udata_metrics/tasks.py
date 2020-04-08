import logging

from udata_metrics.client import InfluxClient
from udata.models import Dataset
from udata.tasks import job, task
from udata.core.metrics.signals import on_site_metrics_computed

from udata_metrics.client import metrics_client_factory

log = logging.getLogger(__name__)


@on_site_metrics_computed.connect
def site_metrics_send(document, **kwargs):
    write_object_metrics.delay(document)


@task
def write_object_metrics(document):
    client = metrics_client_factory()
    metrics = document.get_metrics()
    client.insert(metrics)
