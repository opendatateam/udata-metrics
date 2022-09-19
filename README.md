# udata-metrics

This plugin handles the connexion to an InfluxDB service for udata.
It provides a client to store and to retrieve views and hits generated by users before injecting them in udata's objects metrics.

## Installation

Install [udata](https://github.com/opendatateam/udata).  

Remain in the same virtual environment (for Python).

Install **udata-metrics**:

```shell
pip install udata-metrics
```

Modify your local configuration file of **udata** (typically, `udata.cfg`) as following:

```python
PLUGINS = ['metrics']
METRICS_INFLUX_DSN = {
    "url": "http://localhost:8086/",
    "org": "etalab",
    "token": "...",
}
METRICS_VECTOR_BUCKET = "vector-bucket"
```

Schedule the `aggregate-metrics-last-day` job with the following command:
```
udata job schedule aggregate-metrics-last-day "30 5 * * *"
```
See [the docs](https://udata.readthedocs.io/en/latest/administrative-tasks/) for more about scheduling tasks.