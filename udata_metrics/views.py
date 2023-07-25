from udata_front import theme
from udata.frontend import template_hook
from udata.harvest.models import HarvestSource
from udata.i18n import I18nBlueprint
from udata.models import Reuse, Follow, Dataset, User, Discussion, Post, Organization


from udata_metrics.metrics import get_metrics_for_model, get_stock_metrics, get_download_url_for_model


blueprint = I18nBlueprint('metrics', __name__, template_folder='templates')


@template_hook('dataset.display.metrics')
def dataset_metrics(ctx):

    dataset = ctx['dataset']

    visit, visit_resource = get_metrics_for_model(
        'dataset', dataset.id, ['visit', 'visit_resource'])
    reuses_metrics = get_stock_metrics(
        Reuse.objects(datasets=dataset).visible())
    followers_metrics = get_stock_metrics(Follow.objects(following=dataset),
                                          date_label='since')
    metric_csv_url = get_download_url_for_model('dataset', dataset.id)

    return theme.render('dataset-metrics.html',
                        dataset=dataset,
                        visit=visit,
                        visit_resource=visit_resource,
                        reuses_metrics=reuses_metrics,
                        followers_metrics=followers_metrics,
                        metric_csv_url=metric_csv_url
                        )


@template_hook('reuse.display.metrics')
def reuse_metrics(ctx):

    reuse = ctx['reuse']
    visit, outlink_metrics = get_metrics_for_model(
        'reuse', reuse.id, ['visit', 'outlink'])
    followers_metrics = get_stock_metrics(Follow.objects(following=reuse), date_label='since')
    metric_csv_url = get_download_url_for_model('reuse', reuse.id)

    return theme.render('reuse-metrics.html',
                        reuse=reuse,
                        visit=visit,
                        outlink_metrics=outlink_metrics,
                        followers_metrics=followers_metrics,
                        metric_csv_url=metric_csv_url
                        )


@template_hook('organization.display.metrics')
def organization_metrics(ctx):
    org = ctx['org']
    visit_dataset, visit_resource, visit_reuse, outlink_metrics = get_metrics_for_model(
        'organization', org.id, ['visit_dataset', 'visit_resource', 'visit_reuse', 'outlink'])
    dataset_metrics = get_stock_metrics(
        Dataset.objects(organization=org).visible(),
        date_label='created_at_internal')
    reuse_metrics = get_stock_metrics(Reuse.objects(organization=org).visible())

    dataset_follower_metrics = get_stock_metrics(
        Follow.objects(following__in=Dataset.objects(organization=org)),
        date_label='since')
    reuse_follower_metrics = get_stock_metrics(
        Follow.objects(following__in=Reuse.objects(organization=org)),
        date_label='since')
    dataset_reuse_metrics = get_stock_metrics(
        Reuse.objects(datasets__in=Dataset.objects(organization=org)).visible())
    metric_csv_url = get_download_url_for_model('organization', org.id)

    return theme.render('organization-metrics.html',
                        org=org,
                        visit_dataset=visit_dataset,
                        visit_resource=visit_resource,
                        visit_reuse=visit_reuse,
                        outlink_metrics=outlink_metrics,
                        dataset_metrics=dataset_metrics,
                        reuse_metrics=reuse_metrics,
                        dataset_follower_metrics=dataset_follower_metrics,
                        reuse_follower_metrics=reuse_follower_metrics,
                        dataset_reuse_metrics=dataset_reuse_metrics,
                        metric_csv_url=metric_csv_url
                        )


@template_hook('site.display.metrics')
def site_metrics(ctx):
    visit_dataset, visit_resource, outlink_metrics = get_metrics_for_model(
        'site', None, ['visit_dataset', 'visit_resource', 'outlink'])
    metric_csv_url = get_download_url_for_model('site', None)

    return theme.render('site-metrics.html',
                        update_date=Dataset.objects.filter(badges__kind='spd'),
                        recent_datasets=Dataset.objects.visible(),
                        recent_reuses=Reuse.objects(featured=True).visible(),
                        last_post=Post.objects.published().first(),
                        user_metrics=get_stock_metrics(User.objects()),
                        dataset_metrics=get_stock_metrics(Dataset.objects().visible(),
                                                          date_label='created_at_internal'),
                        harvest_metrics=get_stock_metrics(HarvestSource.objects()),
                        reuse_metrics=get_stock_metrics(Reuse.objects().visible()),
                        organization_metrics=get_stock_metrics(Organization.objects().visible()),
                        discussion_metrics=get_stock_metrics(Discussion.objects(),
                                                             date_label='created'),
                        visit_dataset=visit_dataset,
                        visit_resource=visit_resource,
                        outlink_metrics=outlink_metrics,
                        metric_csv_url=metric_csv_url
                        )
