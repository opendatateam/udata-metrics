'''
udata-metrics

Connexion handler to metrics service for udata
'''

from flask import _app_ctx_stack

from udata_metrics.client import metrics_client_factory


__version__ = '0.1.0.dev'
__description__ = 'Connexion handler to metrics service for udata'


def init_app(app):
    # Do whatever you want to initialize your plugin
    pass
