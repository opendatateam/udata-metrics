from collections import OrderedDict
from datetime import datetime, timedelta
import logging
import requests
from typing import Union, List, Dict

from bson import ObjectId
from dateutil.rrule import rrule, MONTHLY
from flask import current_app
from pymongo.command_cursor import CommandCursor
from mongoengine import QuerySet

from udata.app import cache
from udata.models import db, CommunityResource, Dataset


log = logging.getLogger(__name__)

METRICS_CACHE_DURATION = 60 * 60  # in seconds


def get_last_13_months() -> List[str]:
    dstart = datetime.today().replace(day=1) - timedelta(days=365)
    months = rrule(freq=MONTHLY, count=13, dtstart=dstart)
    return [month.strftime('%Y-%m') for month in months]


def compute_monthly_metrics(metrics_data: List[Dict], metrics_labels: List[str]) -> OrderedDict:
    # Initialize default monthly_metrics
    monthly_metrics = OrderedDict(
        (month, {label: 0 for label in metrics_labels}) for month in get_last_13_months()
    )
    # Update monthly_metrics with metrics_data values
    for entry in metrics_data:
        entry_month = entry['metric_month']
        if entry_month in monthly_metrics:
            for metric_label in metrics_labels:
                label = f'monthly_{metric_label}'
                monthly_metrics[entry_month][metric_label] = entry.get(label) or 0
    return monthly_metrics


def metrics_by_label(monthly_metrics: Dict, metrics_labels: List[str]) -> List[OrderedDict]:
    metrics_by_label = []
    for label in metrics_labels:
        metrics_by_label.append(
            OrderedDict((month, monthly_metrics[month][label]) for month in monthly_metrics))
    return metrics_by_label


@cache.memoize(METRICS_CACHE_DURATION)
def get_metrics_for_model(
            model: str,
            id: Union[str, ObjectId, None],
            metrics_labels: List[str]
        ) -> List[OrderedDict]:
    '''
    Get distant metrics for a particular model object
    This uses @cache.cached decorator w/ short lived cache
    '''
    if not current_app.config['METRICS_API']:
        # TODO: How to best deal with no METRICS_API, prevent calling or return empty?
        # raise ValueError("missing config METRICS_API to use this function")
        return [{} for _ in range(len(metrics_labels))]
    models = model + 's' if id else model  # TODO: not clean of a hack
    model_metrics_api = f'{current_app.config["METRICS_API"]}/{models}/data/'
    try:
        params = {
            'metric_month__sort': 'desc'
        }
        if id:
            params[f'{model}_id__exact'] = id
        res = requests.get(model_metrics_api, params)
        res.raise_for_status()
        monthly_metrics = compute_monthly_metrics(res.json()['data'], metrics_labels)
        return metrics_by_label(monthly_metrics, metrics_labels)
    except requests.exceptions.RequestException as e:
        log.exception(f'Error while getting metrics for {model}({id}): {e}')
        return [{} for _ in range(len(metrics_labels))]


def get_download_url_for_model(model: str, id: Union[str, ObjectId, None]) -> str:
    models = model + 's' if id else model  # TODO: not clean of a hack
    base_url = f'{current_app.config["METRICS_API"]}/{models}/data/csv/'
    if id:
        return f'{base_url}?{model}_id__exact={id}'
    return base_url


def compute_monthly_aggregated_metrics(aggregation_res: CommandCursor) -> OrderedDict:
    monthly_metrics = OrderedDict((month, 0) for month in get_last_13_months())
    for monthly_count in aggregation_res:
        year, month = monthly_count['_id'].split('-')
        monthly_label = year + '-' + month.zfill(2)
        if monthly_label in monthly_metrics:
            monthly_metrics[monthly_label] = monthly_count['count']
    return monthly_metrics


@cache.memoize(METRICS_CACHE_DURATION)
def get_stock_metrics(objects: QuerySet, date_label: str = 'created_at') -> OrderedDict:
    '''
    Get stock metrics for a particular model object
    This uses @cache.cached decorator w/ short lived cache

    TODO: check memoization https://flask-caching.readthedocs.io/en/latest/
    > Using mutable objects (classes, etc) as part of the cache key can become tricky [...]
    '''
    pipeline = [
        {
            '$match': {
                date_label: {'$gte': datetime.now() - timedelta(days=365)}
            }
        },
        {
            '$group': {
                '_id': {'$concat': [
                    {'$substr': [{'$year': f'${date_label}'}, 0, 4]},
                    '-',
                    {'$substr': [{'$month': f'${date_label}'}, 0, 12]}
                ]},
                'count': {'$sum': 1}
            }
        }
    ]
    aggregation_res = objects.aggregate(*pipeline)

    return compute_monthly_aggregated_metrics(aggregation_res)


def save_model(model: db.Document, model_id: str, value: int) -> None:
    model_result = model.objects.filter(id=model_id).first()
    if not model_result:
        log.debug(f'{model.__name__} not found', extra={
            'id': model_id
        })
        return
    if model_result.metrics.get('views', 0) == value:
        # Metric hasn't changed, skip update (useful for objects that are slow when saving)
        return
    model_result.metrics['views'] = value
    try:
        model_result.save(signal_kwargs={'ignores': ['post_save']})
    except Exception as e:
        log.exception(e)


def iterate_on_metrics(target: str, value_key: str) -> dict:
    '''
    paginate on target endpoint
    '''
    with requests.Session() as session:
        url = f'{current_app.config["METRICS_API"]}/{target}_total/data/?{value_key}__greater=1'
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        log.info(f'{data["meta"]["total"]} objects found')
        for row in data['data']:
            yield row
        while data['links'].get('next'):
            r = session.get(data['links'].get('next'), timeout=15)
            r.raise_for_status()
            data = r.json()
            for row in data['data']:
                yield row


def process_metrics_result(target_endpoint: str,
                           model: db.Document,
                           id_key: str,
                           value_key: str = 'visit') -> None:
    '''
    Fetch metrics and update udata objects with the total metrics count
    '''
    log.info(f'Processing model {model}')
    start = datetime.now()
    for data in iterate_on_metrics(target_endpoint, value_key):
        if model.__name__ == 'Resource':
            # Specific case for resource:
            # - it could either be a Dataset Resource embedded document or a CommunityResource
            # - it requires special performance improvement to prevent saving the entire document
            modified_count = Dataset.objects(resources__id=data[id_key]).update(
                **{'set__resources__$__metrics__views': data[value_key]}
            )
            if not modified_count:
                # No embedded resource found with this id, could be a CommunityResource
                save_model(CommunityResource, model_id=data[id_key], value=data[value_key])
        else:
            save_model(model, model_id=data[id_key], value=data[value_key])
    log.info(f'Done in {datetime.now() - start}')
