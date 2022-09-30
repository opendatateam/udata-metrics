from udata.core.dataset.factories import DatasetFactory, ResourceFactory
from udata.core.reuse.factories import ReuseFactory
from udata.core.organization.factories import OrganizationFactory
from udata.utils import faker


def mock_query_aggregated_results(id_key, views):
    return [[[{id_key: str(id), '_value': views[id]} for id in views]]]


def test_metrics_dataset_update(app, client, mocker):
    datasets = [DatasetFactory(metrics={}) for _ in range(8)]
    views = {dat.id: faker.pyint() for dat in datasets}

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('dataset_id', views)

    client.update_aggregated_metrics_in_udata_models('dataset', 'dataset_id')
    for dat in datasets:
        dat.reload()
        assert dat.metrics['views'] == views[dat.id]


def test_metrics_reuse_update(app, client, mocker):
    reuses = [ReuseFactory(metrics={}) for _ in range(8)]
    views = {reuse.id: faker.pyint() for reuse in reuses}

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('reuse_id', views)

    client.update_aggregated_metrics_in_udata_models('reuse', 'reuse_id')
    for reuse in reuses:
        reuse.reload()
        assert reuse.metrics['views'] == views[reuse.id]


def test_metrics_organization_update(app, client, mocker):
    organizations = [OrganizationFactory(metrics={}) for _ in range(8)]
    views = {org.id: faker.pyint() for org in organizations}

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('organization_id', views)

    client.update_aggregated_metrics_in_udata_models('organization', 'organization_id')
    for org in organizations:
        org.reload()
        assert org.metrics['views'] == views[org.id]


def test_metrics_resource_update(app, client, mocker):
    resources = [ResourceFactory(metrics={}) for _ in range(8)]
    datasets = [DatasetFactory(resources=resources[:4]), DatasetFactory(resources=resources[4:])]
    views = {res.id: faker.pyint() for res in resources}

    m = mocker.patch('udata_metrics.client.InfluxClient.retrieve_aggregated_metrics')
    m.return_value = mock_query_aggregated_results('resource_id', views)

    client.update_aggregated_metrics_in_udata_models('resource', 'resource_id')

    for dat in datasets:
        dat.reload()
        for res in dat.resources:
            assert res.metrics['views'] == views[res.id]
