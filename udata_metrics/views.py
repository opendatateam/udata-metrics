from udata_front import theme
from udata.app import cache
from udata.frontend import template_hook
from udata.harvest.models import HarvestSource
from udata.i18n import I18nBlueprint
from udata.models import Reuse, Follow, Dataset, User, Discussion, Organization


from udata_metrics.metrics import (
    get_metrics_for_model, get_stock_metrics, get_download_url
)


METRICS_CACHE_DURATION = 60 * 60  # in seconds


blueprint = I18nBlueprint('metrics', __name__, template_folder='templates')


@cache.memoize(METRICS_CACHE_DURATION)
def get_dataset_metrics(dataset_id: str):
    '''
    This uses @cache.memoize decorator w/ short lived cache
    '''
    visit, download_resource = get_metrics_for_model(
        'dataset', dataset_id, ['visit', 'download_resource'])
    reuses_metrics = get_stock_metrics(
        Reuse.objects(datasets=dataset_id).visible())
    followers_metrics = get_stock_metrics(Follow.objects(following=dataset_id),
                                          date_label='since')
    return {
        'visit': visit,
        'download_resource': download_resource,
        'reuses_metrics': reuses_metrics,
        'followers_metrics': followers_metrics
    }


@cache.memoize(METRICS_CACHE_DURATION)
def get_reuse_metrics(reuse_id: str):
    '''
    This uses @cache.memoize decorator w/ short lived cache
    '''
    visit, = get_metrics_for_model('reuse', reuse_id, ['visit'])
    followers_metrics = get_stock_metrics(Follow.objects(following=reuse_id), date_label='since')
    return {
        'visit': visit,
        'followers_metrics': followers_metrics
    }


@cache.memoize(METRICS_CACHE_DURATION)
def get_organization_metrics(organization_id: str):
    '''
    This uses @cache.memoize decorator w/ short lived cache
    '''
    visit_dataset, download_resource, visit_reuse = get_metrics_for_model(
        'organization', organization_id, ['visit_dataset', 'download_resource', 'visit_reuse'])
    dataset_metrics = get_stock_metrics(
        Dataset.objects(organization=organization_id).visible(),
        date_label='created_at_internal')
    reuse_metrics = get_stock_metrics(Reuse.objects(organization=organization_id).visible())

    dataset_follower_metrics = get_stock_metrics(
        Follow.objects(following__in=Dataset.objects(organization=organization_id)),
        date_label='since')
    reuse_follower_metrics = get_stock_metrics(
        Follow.objects(following__in=Reuse.objects(organization=organization_id)),
        date_label='since')
    dataset_reuse_metrics = get_stock_metrics(
        Reuse.objects(datasets__in=Dataset.objects(organization=organization_id)).visible())
    return {
        'visit_dataset': visit_dataset,
        'download_resource': download_resource,
        'visit_reuse': visit_reuse,
        'dataset_metrics': dataset_metrics,
        'reuse_metrics': reuse_metrics,
        'dataset_follower_metrics': dataset_follower_metrics,
        'reuse_follower_metrics': reuse_follower_metrics,
        'dataset_reuse_metrics': dataset_reuse_metrics
    }


@cache.memoize(METRICS_CACHE_DURATION)
def get_site_metrics():
    '''
    This uses @cache.memoize decorator w/ short lived cache
    '''
    visit_dataset, download_resource = get_metrics_for_model(
        'site', None, ['visit_dataset', 'download_resource'])
    user_metrics = get_stock_metrics(User.objects())
    dataset_metrics = get_stock_metrics(Dataset.objects().visible(),
                                        date_label='created_at_internal')
    harvest_metrics = get_stock_metrics(HarvestSource.objects())
    reuse_metrics = get_stock_metrics(Reuse.objects().visible())
    organization_metrics = get_stock_metrics(Organization.objects().visible())
    discussion_metrics = get_stock_metrics(Discussion.objects(), date_label='created')
    return {
        'visit_dataset': visit_dataset,
        'download_resource': download_resource,
        'user_metrics': user_metrics,
        'dataset_metrics': dataset_metrics,
        'harvest_metrics': harvest_metrics,
        'reuse_metrics': reuse_metrics,
        'organization_metrics': organization_metrics,
        'discussion_metrics': discussion_metrics,
        'visit_dataset': visit_dataset
    }


@template_hook('dataset.display.metrics')
def dataset_metrics(ctx):
    dataset = ctx['dataset']
    dataset_metrics = get_dataset_metrics(dataset.id)
    metric_csv_url = get_download_url('dataset', dataset.id)
    return theme.render('dataset-metrics.html',
                        dataset=dataset,
                        metric_csv_url=metric_csv_url,
                        **dataset_metrics
                        )


@template_hook('reuse.display.metrics')
def reuse_metrics(ctx):
    reuse = ctx['reuse']
    reuse_metrics = get_reuse_metrics(reuse.id)
    metric_csv_url = get_download_url('reuse', reuse.id)
    return theme.render('reuse-metrics.html',
                        reuse=reuse,
                        metric_csv_url=metric_csv_url,
                        **reuse_metrics
                        )


@template_hook('organization.display.metrics')
def organization_metrics(ctx):
    org = ctx['org']
    organization_metrics = get_organization_metrics(org.id)
    metric_csv_url = get_download_url('organization', org.id)
    return theme.render('organization-metrics.html',
                        org=org,
                        metric_csv_url=metric_csv_url,
                        **organization_metrics
                        )


@template_hook('site.display.metrics')
def site_metrics(ctx):
    site_metrics = get_site_metrics()
    metric_csv_url = get_download_url('site', None)
    return theme.render('site-metrics.html',
                        metric_csv_url=metric_csv_url,
                        **site_metrics
                        )
