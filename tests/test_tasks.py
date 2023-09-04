import pytest

from udata.core.dataset.factories import CommunityResourceFactory, DatasetFactory, ResourceFactory
from udata.core.organization.factories import OrganizationFactory
from udata.core.reuse.factories import ReuseFactory
from udata.models import Dataset, Organization, Resource, Reuse

from udata_metrics.tasks import (
    iterate_on_metrics, process_metrics_result
)
from .helpers import mock_metrics_payload


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
