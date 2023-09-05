from datetime import datetime, timedelta


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


def mock_monthly_metrics_payload(app, rmock, target, data, target_id='id', url=None):
    if not url:
        url = f'{app.config["METRICS_API"]}/{target}s/data/' + \
              f'?metric_month__sort=desc&{target}_id__exact={target_id}'
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
