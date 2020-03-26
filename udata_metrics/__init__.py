'''
udata-metrics

Connexion handler to metrics service for Udata
'''

from udata-metrics.client import InfluxClient


__version__ = '0.1.0.dev'
__description__ = 'Connexion handler to metrics service for Udata'



def init_app(app):
    InfluxClient(app.config['METRICS_DSN'])

