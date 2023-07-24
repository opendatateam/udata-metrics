from collections import OrderedDict
from datetime import date, datetime, timedelta
import logging
import requests
import tqdm
from typing import Union, List, Dict
from uuid import UUID

from bson import ObjectId
from dateutil.rrule import rrule, MONTHLY
from flask import current_app
from pymongo.command_cursor import CommandCursor
from mongoengine import QuerySet
from werkzeug.exceptions import NotFound

from udata.app import cache
from udata.core.dataset.models import get_resource
from udata.models import db


log = logging.getLogger(__name__)

METRICS_CACHE_DURATION = 60 * 60  # in seconds


def monthly_labels():
    return [month.strftime('%Y-%m') for month in rrule(
            MONTHLY,
            dtstart=date.today() - timedelta(days=365),
            until=date.today()
            )]


def compute_monthly_metrics(metrics_data: List[Dict], metrics_labels: List[str]) -> Dict:
    # Initialize default monthly_metrics
    monthly_metrics = OrderedDict(
        (month, {label: 0 for label in metrics_labels}) for month in monthly_labels()
    )
    # Update monthly_metrics with metrics_data values
    for entry in metrics_data:
        entry_month = entry['metric_month']
        if entry_month in monthly_metrics:
            for metric_label in metrics_labels:
                label = f'monthly_{metric_label}'
                monthly_metrics[entry_month][metric_label] = entry.get(label) or 0
    return monthly_metrics


def metrics_by_label(monthly_metrics: Dict, metrics_labels: List[str]) -> List[List[int]]:
    metrics_by_label = []
    for label in metrics_labels:
        metrics_by_label.append({month: monthly_metrics[month][label] for month in monthly_metrics})
    return metrics_by_label


@cache.memoize(METRICS_CACHE_DURATION)
def get_metrics_for_model(
            model: str,
            id: Union[str, ObjectId, None],
            metrics_labels: List[str]
        ) -> List[Dict[str, int]]:
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


def compute_monthly_aggregated_metrics(aggregation_res: CommandCursor):
    monthly_metrics = OrderedDict((month, 0) for month in monthly_labels())
    for monthly_count in aggregation_res:
        year, month = monthly_count['_id'].split('-')
        monthly_label = year + '-' + month.zfill(2)
        if monthly_label in monthly_metrics:
            monthly_metrics[monthly_label] = monthly_count['count']
    return monthly_metrics


@cache.memoize(METRICS_CACHE_DURATION)
def get_stock_metrics(objects: QuerySet, date_label: str = 'created_at') -> List[int]:
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


def iterate_on_metrics(target: str, value_key: str):
    '''
    paginate on target endpoint
    '''
    url = f'{current_app.config["METRICS_API"]}/{target}_total/data/?{value_key}__greater=1'  # TODO: create view
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    log.info(f'{data["meta"]["total"]} objects found')
    for row in data['data']:
        yield row
    while data['links'].get('next'):
        r = requests.get(current_app.config["METRICS_API"].replace('/api', '') + data['links'].get('next'))
        r.raise_for_status()
        data = r.json()
        for row in data['data']:
            yield row


def process_metrics_result(target_endpoint: str,
                           model: db.Document,
                           id_key: str,
                           value_key: str = 'visit'):
    log.info(f'Processing model {model}')
    for data in tqdm.tqdm(iterate_on_metrics(target_endpoint, value_key)):
        try:
            if model.__name__ == 'Resource':
                model_result = get_resource(UUID(data[id_key]))
                if not model_result:
                    raise NotFound()
            else:
                model_result = model.get(data[id_key])
        except NotFound:
            log.debug(f'{model.__name__} not found', extra={
                'id': data[id_key]
            })
            continue
        if model_result.metrics['views'] === data[value_key]:
            # Metric hasn't changed, skip update (useful for objects that are slow when saving)
            continue
        model_result.metrics['views'] = data[value_key]
        try:
            model_result.save(signal_kwargs={'ignores': ['post_save']})
        except Exception as e:
            log.exception(e)
            continue
