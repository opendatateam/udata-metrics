import pytest

from udata.core.dataset.factories import CommunityResourceFactory, DatasetFactory, ResourceFactory
from udata.core.organization.factories import OrganizationFactory
from udata.core.reuse.factories import ReuseFactory

from udata_metrics.tasks import (
    iterate_on_metrics, update_datasets, update_organizations, update_resources_and_community_resources, update_reuses
)
from .helpers import mock_metrics_api


def test_iterate_on_metrics(app, rmock):
    mock_metrics_api(app, rmock, "test_model", "test_key", [
        { 'id': 1 },
        { 'id': 2 },
        { 'id': 3 },
    ], page_size=2)

    metrics_data = list(iterate_on_metrics("test_model", "test_key", page_size=2))

    assert metrics_data == [
        { 'id': 1 },
        { 'id': 2 },
        { 'id': 3 },
    ]

def test_update_datasets_metrics(app, rmock):
    datasets = [DatasetFactory() for i in range(15)]
    mock_metrics_api(app, rmock, "datasets", "visit", [
        { 'dataset_id': str(datasets[1].id), 'visit': 42 },
        { 'dataset_id': str(datasets[3].id), 'visit': 1337 },
        { 'dataset_id': str(datasets[4].id), 'visit': 2 },
    ])

    update_datasets()
    [dataset.reload() for dataset in datasets]

    assert datasets[1].metrics.get('views') == 42
    assert datasets[3].metrics.get('views') == 1337
    assert datasets[4].metrics.get('views') == 2


def test_update_reuses_metrics(app, rmock):
    reuses = [ReuseFactory() for i in range(15)]
    mock_metrics_api(app, rmock, "reuses", "visit", [
        { 'reuse_id': str(reuses[1].id), 'visit': 42 },
        { 'reuse_id': str(reuses[3].id), 'visit': 1337 },
        { 'reuse_id': str(reuses[4].id), 'visit': 2 },
    ])

    update_reuses()
    [reuse.reload() for reuse in reuses]

    assert reuses[1].metrics.get('views') == 42
    assert reuses[3].metrics.get('views') == 1337
    assert reuses[4].metrics.get('views') == 2

def test_update_organizations_metrics(app, rmock):
    organizations = [OrganizationFactory() for i in range(15)]
    mock_metrics_api(app, rmock, "organizations", "visit", [
        { 'organization_id': str(organizations[1].id), 'visit': 42 },
        { 'organization_id': str(organizations[3].id), 'visit': 1337 },
        { 'organization_id': str(organizations[4].id), 'visit': 2 },
    ])

    update_organizations()
    [organization.reload() for organization in organizations]

    assert organizations[1].metrics.get('views') == 42
    assert organizations[3].metrics.get('views') == 1337
    assert organizations[4].metrics.get('views') == 2


def test_update_resources_metrics(app, rmock):
    resources = [ResourceFactory() for i in range(5)]
    dataset_a_with_resources = DatasetFactory(resources=resources)
    dataset_b_with_resource = DatasetFactory(resources=[ResourceFactory()])

    community_resources = [CommunityResourceFactory() for i in range(10)]

    dataset_without_resource = DatasetFactory()

    mock_metrics_api(app, rmock, "resources", "download_resource", [
        { 'resource_id': str(dataset_a_with_resources.resources[0].id), 'dataset_id': str(dataset_a_with_resources.id), 'download_resource': 42 },
        { 'resource_id': str(community_resources[3].id), 'dataset_id': None, 'download_resource': 16 },
        { 'resource_id': str(community_resources[5].id), 'dataset_id': None, 'download_resource': 111 },
        { 'resource_id': str(dataset_a_with_resources.resources[1].id), 'dataset_id': str(dataset_a_with_resources.id), 'download_resource': 1337 },
        { 'resource_id': str(dataset_b_with_resource.resources[0].id), 'dataset_id': str(dataset_b_with_resource.id), 'download_resource': 1404 },
        { 'resource_id': str(dataset_a_with_resources.resources[4].id), 'dataset_id': str(dataset_a_with_resources.id), 'download_resource': 2 },
        { 'resource_id': str(community_resources[9].id), 'dataset_id': None, 'download_resource': 1 },
    ])

    update_resources_and_community_resources()

    dataset_a_with_resources.reload()
    dataset_b_with_resource.reload()
    dataset_without_resource.reload()
    [community_resource.reload() for community_resource in community_resources]

    assert community_resources[3].metrics.get('views') == 16
    assert community_resources[5].metrics.get('views') == 111
    assert community_resources[9].metrics.get('views') == 1

    assert dataset_a_with_resources.resources[0].metrics.get('views') == 42
    assert dataset_a_with_resources.resources[1].metrics.get('views') == 1337
    assert dataset_a_with_resources.resources[4].metrics.get('views') == 2
    assert dataset_a_with_resources.metrics.get('number_of_resources_downloads') == 42 + 1337 + 2

    assert dataset_b_with_resource.resources[0].metrics.get('views') == 1404
    assert dataset_b_with_resource.metrics.get('number_of_resources_downloads') == 1404

    assert dataset_without_resource.metrics.get('number_of_resources_downloads') is None
