import pytest

from flask import render_template_string, g

from udata.core.dataset.factories import DatasetFactory
from udata.core.organization.factories import OrganizationFactory
from udata.core.reuse.factories import ReuseFactory

from .helpers import mock_monthly_metrics_payload


def render_hook(hook, **kwargs):
    g.lang_code = 'en'
    return render_template_string(
        '{{ hook("' + hook + '") }}',
        **kwargs
    )


@pytest.mark.frontend
@pytest.mark.usefixtures('clean_db')
@pytest.mark.options(THEME='gouvfr')
@pytest.mark.options(plugins=['front', 'metrics'])
class MetricsBlueprintTest:
    @pytest.mark.parametrize('target,factory,value_keys', [
        ('dataset', DatasetFactory, ['visit', 'download_resource']),
        ('reuse', ReuseFactory, ['visit']),
        ('organization', OrganizationFactory, ['visit_dataset', 'download_resource', 'visit_reuse'])
    ])
    def test_render_metrics_for_model(self, app, rmock, target, factory, value_keys):
        '''It should render the model metrics'''
        model = factory()
        data = [(value_key, 2403) for value_key in value_keys]
        mock_monthly_metrics_payload(app, rmock, target, data=data,
                                     target_id=model.id)
        context_target = 'org' if target == 'organization' else target
        response = render_hook(f'{target}.display.metrics', **{context_target: model})
        assert 'Download traffic metrics as CSV' in response

    def test_render_site_display(self, app, rmock):
        '''It should render the site metrics'''
        data = [('visit_dataset', 337), ('download_resource', 42)]
        url = f'{app.config["METRICS_API"]}/site/data/?metric_month__sort=desc'
        mock_monthly_metrics_payload(app, rmock, 'site', data=data, url=url)
        response = render_hook('site.display.metrics')
        assert 'Download traffic metrics as CSV' in response
