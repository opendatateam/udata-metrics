# udata-metrics

A job is scheduled to inject traffic metrics in udata's objects metrics.

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
