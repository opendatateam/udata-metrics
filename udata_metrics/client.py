from datetime import datetime, timedelta
from flask import current_app
from influxdb_client import InfluxDBClient


class InfluxClient:

    def __init__(self, dsn):
        self.client = InfluxDBClient(**dsn, timeout=100_000)

    def aggregate_metrics(self, measurement: str):
        """measurement one of reuse.reuse_id, resource.resource_id,
        dataset.dataset_id, organization.organization_id,
        resource_hit.resource_id"""
        dt = datetime.now()
        page_type = f"{measurement}".split(".")[0]
        id_key = f"{measurement}".split(".")[1]
        # TODO: Replace with right timezone for logs in the query
        query = f"""
                import "date"
                from(bucket: "vector-bucket")
                    |> range(start: 2022-06-02T00:00:00.000Z, stop: 2022-06-03T00:00:00.000Z)
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
        print(query)
        self.client.query_api().query(query)
        # TODO: Ensure deletion works as intended
        # self.client.delete_api().delete(
        #     start=f"{dt.strftime('%Y-%m-%d') - timedelta(days=1)}T00:00:00.00Z",
        #     stop=f"{dt.strftime('%Y-%m-%d')}T00:00:00.00Z",
        #     bucket=current_app.config['METRICS_VECTOR_BUCKET'],
        #     org=current_app.config['METRICS_INFLUX_DSN']['org']
        # )


def metrics_client_factory():
    dsn = current_app.config["METRICS_INFLUX_DSN"]
    return InfluxClient(dsn)
