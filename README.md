# udata-metrics

This plugin adds views to display metrics on datasets, reuses, organizations and site dashboard pages.
It uses hook logic based on udata-front hooks.
We feed these views with data from:
    * an optional metrics API for traffic (views, download, external links)
    * mongo for stock values (with an aggregation pipeline)
A job can be scheduled to inject traffic metrics in udata's objects metrics.

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
METRICS_API = 'http://localhost:8005/api'
```
