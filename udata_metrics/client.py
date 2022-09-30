import logging
from datetime import date, timedelta
import uuid
from flask import current_app
from influxdb_client import InfluxDBClient

from udata.core.dataset.models import Dataset, get_resource
from udata.models import Reuse, Organization

log = logging.getLogger(__name__)


class InfluxClient:

    def __init__(self, dsn):
        self.client = InfluxDBClient(**dsn, timeout=600_000)

        today_dt = date.today()
        yesterday_dt = today_dt - timedelta(days=1)
        self.today = today_dt.strftime('%Y-%m-%dT%H:%M:%S')
        self.yesterday = yesterday_dt.strftime('%Y-%m-%dT%H:%M:%S')
        self.timezone = '000Z'  # TODO: Replace with right timezone for logs in the query

    def compute_aggregated_metrics(self, page_type: str, measurement: str, id_key: str):
        '''
        Run an influx query to compute and store aggregated metrics.
        Sum the number of entries per object for the previous day in influxdb.
        '''
        log.info(f'Running metrics aggregation for {page_type}')

        agg_query = f"""
            import "date"
            from(bucket: "{current_app.config['METRICS_VECTOR_BUCKET']}")
                |> range(start: {self.yesterday}.{self.timezone},
                         stop: {self.today}.{self.timezone})
                |> filter(fn: (r) => r._measurement == "{measurement}")
                |> map(fn: (r) => ({{r with day: date.truncate(t: r._time, unit: 1d)}}))
                |> group(columns: ["{id_key}", "day"])
                |> count()
                |> duplicate(column: "day", as: "_time")
                |> map(fn: (r) => ({{r with _field: "{page_type}", _measurement: "count"}}))
                |> to(
                    bucket: "{current_app.config['METRICS_VECTOR_BUCKET']}",
                    host: "{current_app.config['METRICS_INFLUX_DSN']['url']}",
                    org: "{current_app.config['METRICS_INFLUX_DSN']['org']}",
                    token: "{current_app.config['METRICS_INFLUX_DSN']['token']}"
                )
            """
        self.client.query_api().query(agg_query)

    def delete_metrics_points(self, measurement: str):
        '''
        Run an influx query to delete individual metrics points for the previous day
        '''
        self.client.delete_api().delete(
            start=f'{self.yesterday}.{self.timezone}',
            stop=f'{self.today}.{self.timezone}',
            predicate=f'_measurement="{measurement}"',
            bucket=current_app.config['METRICS_VECTOR_BUCKET'],
            org=current_app.config['METRICS_INFLUX_DSN']['org']
        )

    def retrieve_aggregated_metrics(self, page_type: str, id_key: str):
        '''
        Query influx to retrieve aggregated metrics for all objects of page_type
        '''
        retrieve_metrics_query = f"""
            from(bucket: "{current_app.config['METRICS_VECTOR_BUCKET']}")
                |> range(start: 0, stop: {self.today}.{self.timezone})
                |> filter(fn: (r) => (r._field == "{page_type}") and (r._measurement == "count"))
                |> group(columns: ["{id_key}"])
                |> sum()
                |> yield(name: "views-{page_type}")
        """
        return self.client.query_api().query(retrieve_metrics_query)

    def update_aggregated_metrics_in_udata_models(self, page_type: str, id_key: str):
        '''
        Update aggregated metrics in udata models for all objects of page_type
        '''
        results = self.retrieve_aggregated_metrics(page_type, id_key)
        for result in results:
            for table in result:
                for record in table:
                    # Currently we ignore resource hits and compute resource downloads only
                    if page_type == 'resource_hit':
                        continue
                    elif page_type == 'resource':
                        # Since resource is not a model per se but part of the dataset model, we
                        # need to treat it separately.
                        try:
                            model_id = uuid.UUID(record[id_key])
                            model_result = get_resource(model_id)
                        except Exception as e:
                            log.exception(e)
                            continue
                    else:
                        model = {
                            'dataset': Dataset,
                            'reuse': Reuse,
                            'organization': Organization
                        }[page_type]

                        model_id = record[id_key]
                        model_result = model.get(model_id)

                    if model_result:
                        log.debug(f'Found {page_type} {model_result.id}')
                        model_result.metrics['views'] = record['_value']
                        try:
                            model_result.save(signal_kwargs={'ignores': ['post_save']})
                        except Exception as e:
                            log.exception(e)
                            continue
                    else:
                        log.error(f'{page_type} not found', extra={
                            'id': model_id
                        })

    def aggregate_metrics(self, measurement: str):
        """
        Aggregate metrics on latest metrics entries, remove individual metrics points and
        update udata objects with updated aggregated metrics.

        measurement is one of reuse.reuse_id, resource.resource_id,
        dataset.dataset_id, organization.organization_id,
        resource_hit.resource_id"""
        page_type, id_key = f'{measurement}'.split('.')

        self.compute_aggregated_metrics(page_type, measurement, id_key)
        self.delete_metrics_point(measurement)
        self.update_aggregated_metrics_in_udata_models(page_type, id_key)


def metrics_client_factory():
    dsn = current_app.config['METRICS_INFLUX_DSN']
    return InfluxClient(dsn)
