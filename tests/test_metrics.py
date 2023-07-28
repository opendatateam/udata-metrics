from datetime import datetime, timedelta
import pytest

from udata.core.dataset.factories import CommunityResourceFactory, DatasetFactory, ResourceFactory
from udata.core.organization.factories import OrganizationFactory
from udata.core.reuse.factories import ReuseFactory
from udata.models import Dataset, Organization, Resource, Reuse

from udata_metrics.metrics import (
    iterate_on_metrics, process_metrics_result, get_metrics_for_model, get_stock_metrics
)


def mock_metrics_payload(app, rmock, target, value_key, data, url=None, next=None, total=10):
    if not url:
        url = f'{app.config["METRICS_API"]}/{target}_total/data/?{value_key}__greater=1'
    rmock.get(url, json={
        'data': [
            {
                f'{target}_id': key,
                value_key: value
            } for key, value in data
        ],
        'links': {
            'next': next
        },
        'meta': {
            'total': total
        }
    })


def mock_monthly_metrics_payload(app, rmock, target, data, url=None):
    if not url:
        url = f'{app.config["METRICS_API"]}/{target}s/data/' + \
              f'?metric_month__sort=desc&{target}_id__exact=id'
    current_month = datetime.now().strftime('%Y-%m')
    last_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    rmock.get(url, json={
        'data': [
            {
                'metric_month': current_month,
                **{
                    f'monthly_{key}': len(key)*value+1
                    for key, value in data
                }
            },
            {
                'metric_month': last_month,
                **{
                    f'monthly_{key}': len(key)*value
                    for key, value in data
                }
            }
        ],
        'meta': {
            'total': 2
        }
    })


def test_iterate_on_metrics(app, rmock):
    target = 'dataset'
    value_key = 'visit'
    mock_metrics_payload(app, rmock, target, value_key, [('id1', 1), ('id2', 2)],
                         next=f'{app.config["METRICS_API"]}/{target}_total/data/'
                              f'?{value_key}__greater=1&page=2&page_size=10',
                         total=3)
    mock_metrics_payload(app, rmock, target, value_key, [('id3', 3)],
                         url=f'{app.config["METRICS_API"]}/{target}_total/data/'
                             f'?{value_key}__greater=1&page=2&page_size=10',
                         next=None,
                         total=3)
    metrics_data = list(iterate_on_metrics(target, value_key))
    assert metrics_data == [
        {
            'dataset_id': 'id1',
            'visit': 1
        }, {
            'dataset_id': 'id2',
            'visit': 2
        }, {
            'dataset_id': 'id3',
            'visit': 3
        }
    ]


@pytest.mark.parametrize('target,value_key,model,factory', [
    ('dataset', 'visit', Dataset, DatasetFactory),
    ('reuse', 'visit', Reuse, ReuseFactory),
    ('organization', 'visit_dataset', Organization, OrganizationFactory)
])
def test_process_metrics_result_generic(app, rmock, target, value_key, model, factory):
    '''
    Test process_metrics_result for generic models : Dataset, Reuse and Organization.
    Objects should be updated with metrics views accordingly.
    '''
    model_objects = [factory() for i in range(10)]
    mock_metrics_payload(app, rmock, target, value_key,
                         data=[(str(obj.id), i) for i, obj in enumerate(model_objects)])
    process_metrics_result(target, model, f'{target}_id', value_key)

    [obj.reload() for obj in model_objects]
    assert [obj.metrics.get('views', 0) for obj in model_objects] == list(range(len(model_objects)))


def test_process_metrics_result_resource(app, rmock):
    '''
    Test process_metrics_result for embedded resource objects.
    Embedded resources should be updated with metrics views accordingly.
    '''
    target = 'resource'
    value_key = 'visit_resource'
    resources = [ResourceFactory() for i in range(10)]
    dataset = DatasetFactory(resources=resources)
    mock_metrics_payload(app, rmock, target, value_key,
                         data=[(str(obj.id), i) for i, obj in enumerate(resources)])
    process_metrics_result(target, Resource, f'{target}_id', value_key)

    dataset.reload()
    assert [res.metrics.get('views', 0) for res in dataset.resources] == list(range(len(resources)))


def test_process_metrics_result_community_resource(app, rmock):
    '''
    Test process_metrics_result for community resource objects.
    Since no embedded resource should be find by id, community objects should be updated.
    '''
    target = 'resource'
    value_key = 'visit_resource'
    resources = [CommunityResourceFactory() for i in range(10)]
    mock_metrics_payload(app, rmock, target, value_key,
                         data=[(str(obj.id), i) for i, obj in enumerate(resources)])
    process_metrics_result(target, Resource, f'{target}_id', value_key)

    [resource.reload() for resource in resources]
    assert [res.metrics.get('views', 0) for res in resources] == list(range(len(resources)))


@pytest.mark.parametrize('target,value_keys', [
    ('dataset', ['visit', 'visit_resource']),
    ('reuse', ['visit']),
    ('organization', ['visit_dataset', 'visit_resource', 'visit_reuse'])
])
def test_get_metrics_for_model(app, rmock, target, value_keys):
    mock_monthly_metrics_payload(app, rmock, target,
                                 data=[(value_key, 2403) for value_key in value_keys])
    res = get_metrics_for_model(target, 'id', value_keys)
    for i, key in enumerate(value_keys):
        assert len(res[i]) == 13  # The current month as well as last year's are included
        assert list(res[i].values())[-1] == len(key)*2403+1
        assert list(res[i].values())[-2] == len(key)*2403


def test_get_metrics_for_site(app, rmock):
    value_keys = ['visit_dataset', 'visit_resource', ]
    url = f'{app.config["METRICS_API"]}/site/data/?metric_month__sort=desc'
    mock_monthly_metrics_payload(app, rmock, 'site',
                                 data=[(value_key, 2403) for value_key in value_keys], url=url)
    res = get_metrics_for_model('site', None, value_keys)
    for i, key in enumerate(value_keys):
        assert len(res[i]) == 13  # The current month as well as last year's are included
        assert list(res[i].values())[-1] == len(key)*2403+1
        assert list(res[i].values())[-2] == len(key)*2403


@pytest.mark.parametrize('model,factory,date_label', [
    (Dataset, DatasetFactory, 'created_at_internal'),
    (Reuse, ReuseFactory, 'created_at'),
    (Organization, OrganizationFactory, 'created_at')
])
def test_get_stock_metrics(app, clean_db, model, factory, date_label):
    [factory() for i in range(10)]
    [factory(**{date_label: datetime.now().replace(day=1) - timedelta(days=1)}) for i in range(8)]
    res = get_stock_metrics(model.objects(), date_label)
    assert list(res.values())[-1] == 10
    assert list(res.values())[-2] == 8
    assert list(res.values())[-3] == 0
