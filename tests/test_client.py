from udata.core.dataset.factories import DatasetFactory, ResourceFactory
from udata.core.reuse.factories import ReuseFactory
from udata.core.organization.factories import OrganizationFactory
from udata.utils import faker


def mock_query_aggregated_results(id_key, views):
    return [[{id_key: str(id), '_value': views[id]} for id in views]]


def test_metrics_dataset_update(app, client, mocker):
    datasets = [DatasetFactory(metrics={}) for _ in range(8)]

    # Only the 4 first objects have aggregated views metrics computed
    views = {dat.id: faker.pyint() for dat in datasets[:4]}
    # Add a view metric for an unknown object
    views.update({'626bccb9697a12204fb22ea3': faker.pyint()})

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('dataset_id', views)

    client.update_aggregated_metrics_in_udata_models('dataset', 'dataset_id')
    for dat in datasets:
        dat.reload()
        if dat.id in views:
            assert dat.metrics['views'] == views[dat.id]
        else:
            assert 'views' not in dat.metrics


def test_metrics_reuse_update(app, client, mocker):
    reuses = [ReuseFactory(metrics={}) for _ in range(8)]

    # Only the 4 first objects have aggregated views metrics computed
    views = {reuse.id: faker.pyint() for reuse in reuses[:4]}
    # Add a view metric for an unknown object
    views.update({'626bccb9697a12204fb22ea3': faker.pyint()})

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('reuse_id', views)

    client.update_aggregated_metrics_in_udata_models('reuse', 'reuse_id')
    for reuse in reuses:
        reuse.reload()
        if reuse.id in views:
            assert reuse.metrics['views'] == views[reuse.id]
        else:
            assert 'views' not in reuse.metrics


def test_metrics_organization_update(app, client, mocker):
    organizations = [OrganizationFactory(metrics={}) for _ in range(8)]

    # Only the 4 first objects have aggregated views metrics computed
    views = {org.id: faker.pyint() for org in organizations[:4]}
    # Add a view metric for an unknown object
    views.update({'626bccb9697a12204fb22ea3': faker.pyint()})

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('organization_id', views)

    client.update_aggregated_metrics_in_udata_models('organization', 'organization_id')
    for org in organizations:
        org.reload()
        if org.id in views:
            assert org.metrics['views'] == views[org.id]
        else:
            assert 'views' not in org.metrics


def test_metrics_resource_update(app, client, mocker):
    resources = [ResourceFactory(metrics={}) for _ in range(8)]
    datasets = [DatasetFactory(resources=resources[:3]),
                DatasetFactory(resources=resources[3:6]),
                DatasetFactory(resources=resources[6:])]

    # Only the 4 first objects have aggregated views metrics computed
    views = {res.id: faker.pyint() for res in resources[:4]}
    # Add a view metric for an unknown object
    views.update({faker.uuid4(): faker.pyint()})

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('resource_id', views)

    client.update_aggregated_metrics_in_udata_models('resource', 'resource_id')

    for dat in datasets:
        dat.reload()
        for res in dat.resources:
            if res.id in views:
                assert res.metrics['views'] == views[res.id]
            else:
                assert 'views' not in res.metrics
