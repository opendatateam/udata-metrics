from datetime import datetime, timedelta
from flask import current_app
from influxdb import InfluxDBClient


class InfluxClient:

    def __init__(self, dsn):
        self.client = InfluxDBClient(**dsn)

    def get_previous_day_measurements(self, collection, tag):
        query = f'select * from {collection}_views where time > now() - 24h group by {tag};'
        result = self.client.query(query)
        return result

    def sum_views_from_specific_model(self, collection, tag, model_id):
        query = f"select sum(*) from {collection}_views where {tag}='{model_id}';"
        result = self.client.query(query)
        return result

    def insert(self, body):
        self.client.write_points([body])

    def aggregate_metrics(self, measurement: str):
        """measurement one of reuse.reuse_id, resource.resource_id,
        dataset.dataset_id, organization.organization_id,
        resource_hit.resource_id"""
        dt = datetime.now()
        id_key = f"{measurement}".split(".")[1]
        # TODO: Replace with right timezone for logs in the query
        query = f"""
                from(bucket: "vector-bucket")
                    |> range(start: {dt.strftime("%Y-%m-%d") - timedelta(days=1)}T00:00:00.00Z, end: {dt.strftime("%Y-%m-%d")}T00:00:00.00Z)
                    |> filter(fn: (r) => r._measurement == "{measurement}")
                    |> map(fn: (r) => ({{r with day: r._time.strftime("%Y-%m-%d")}}))
                    |> group(columns: ["{id_key}"])
                    |> count()
                    |> map(fn: (r) => ({{r with _time: r._start, _field: "resource", _measurement: "count"}}))
                    |> to(
                        bucket: "{current_app.config["METRICS_INFLUX"]['INFLUX_BUCKET']}",
                        host: "{current_app.config["METRICS_INFLUX"]['INFLUX_URL']}",
                        org: "{current_app.config["METRICS_INFLUX"]['INFLUX_ORG']}",
                        token: "{current_app.config["METRICS_INFLUX"]['INFLUX_TOKEN']}"
                    )
                """
        self.client.query_api().query(query)
        # TODO: Delete unaggregated data for the day


def metrics_client_factory():
    dsn = current_app.config['METRICS_DSN']
    return InfluxClient(dsn)
