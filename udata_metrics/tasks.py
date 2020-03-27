import logging

from udata_metrics.client import InfluxClient
from udata.models import Dataset
from udata.tasks import job, task
from udata.core.metrics.signals import on_site_metrics

from udata_metrics.client import metrics_client_factory

log = logging.getLogger(__name__)


@on_site_metrics.connect
def site_metrics_send(document, **kwargs):
    write_object_metrics.delay(
        metrics_client_factory(),
        document.get_metrics()
    )


@task
def write_object_metrics(client, metrics):
    client.insert(metrics_client_factory)


@job('metrics-test-job')
def my_job(self):
    # Implement you first job here
    log.info('Currently working')
