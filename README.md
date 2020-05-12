# udata-metrics

This plugin handles the connexion to an InfluxDB service for udata.
It provides a client to store and to retrieve views and hits generated by users before injecting them in udata's objects metrics.

## Installation

Install [udata](https://github.com/opendatateam/udata).  
Install [udata-piwik](https://github.com/opendatateam/udata-piwik).

Remain in the same virtual environment (for Python).

Install **udata-metrics**:

```shell
pip install udata-metrics
```

Modify your local configuration file of **udata** (typically, `udata.cfg`) as following:

```python
PLUGINS = ['metrics']
METRICS_DSN = {
    'host': 'localhost',
    'port': '8086',
    'username': 'johndoe',
    'password': 'youwillneverguess',
    'database': 'metrics_db'
}
```
