import logging
from datetime import datetime, timedelta
from flask import current_app
from influxdb_client import InfluxDBClient

log = logging.getLogger(__name__)

class InfluxClient:

    def __init__(self, dsn):
        self.client = InfluxDBClient(**dsn, timeout=600_000)

    def aggregate_metrics(self, measurement: str):
        """measurement one of reuse.reuse_id, resource.resource_id,
        dataset.dataset_id, organization.organization_id,
        resource_hit.resource_id"""
        dt = datetime.now()
        today = dt.strftime('%Y-%m-%d')
        yesterday = today - timedelta(days=1)
        page_type = f"{measurement}".split(".")[0]
        log.info(f"Running metrics aggregation for {page_type}")
        id_key = f"{measurement}".split(".")[1]
        # TODO: Replace with right timezone for logs in the query
        query = f"""
                import "date"
                from(bucket: "vector-bucket")
                    |> range(start: {yesterday}T00:00:00.000Z, stop: {today}T00:00:00.000Z)
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
        self.client.query_api().query(query)
        self.client.delete_api().delete(
            start=f"{yesterday}T00:00:00.000Z",
            stop=f"{today}T00:00:00.000Z",
            predicate=f"_measurement=\"{measurement}\"",
            bucket=current_app.config['METRICS_VECTOR_BUCKET'],
            org=current_app.config['METRICS_INFLUX_DSN']['org']
        )


def metrics_client_factory():
    dsn = current_app.config["METRICS_INFLUX_DSN"]
    return InfluxClient(dsn)
