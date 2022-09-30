import pytest

from udata.settings import Defaults, Testing
from udata.app import create_app
from udata_metrics.client import metrics_client_factory


class MetricsSettings(Testing):
    METRICS_INFLUX_DSN = {
        "url": "http://localhost:8086/",
        "org": "etalab",
        "token": "A2jqhzcRPZeT3bAF",
    }
    PLUGINS = ['metrics']
    METRICS_VECTOR_BUCKET = "udata-metrics-test"


@pytest.fixture
def app():
    app = create_app(Defaults, override=MetricsSettings)
    return app


@pytest.fixture
def client():
    return metrics_client_factory()
