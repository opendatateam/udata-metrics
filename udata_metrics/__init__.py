'''
udata-metrics

Connexion handler to metrics service for udata
'''

from flask import _app_ctx_stack

from udata_metrics.client import InfluxClient


__version__ = '0.1.0.dev'
__description__ = 'Connexion handler to metrics service for Udata'


def init_app(app):
    client = InfluxClient(app.config['METRICS_DSN'])
    ctx = _app_ctx_stack.top
    if ctx is not None and not hasattr(ctx, 'influx_db'):
        ctx.influx_db = client
