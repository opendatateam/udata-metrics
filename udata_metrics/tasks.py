import logging

from udata.tasks import job

log = logging.getLogger(__name__)


@job('metrics-test-job')
def my_job(self):
    # Implement you first job here
    log.info('Currently working')
