import logging
from datetime import datetime, timedelta
import uuid
from flask import current_app
from influxdb_client import InfluxDBClient

from udata.core.dataset.models import Dataset, get_resource
from udata.models import Reuse, Organization

log = logging.getLogger(__name__)

class InfluxClient:

    def __init__(self, dsn):
        self.client = InfluxDBClient(**dsn, timeout=600_000)

    def aggregate_metrics(self, measurement: str):
        """measurement one of reuse.reuse_id, resource.resource_id,
        dataset.dataset_id, organization.organization_id,
        resource_hit.resource_id"""
        today_dt = datetime.now()
        yesterday_dt = today_dt - timedelta(days=1)
        today = today_dt.strftime("%Y-%m-%d")
        yesterday = yesterday_dt.strftime("%Y-%m-%d")
        page_type = f"{measurement}".split(".")[0]
        log.info(f"Running metrics aggregation for {page_type}")
        id_key = f"{measurement}".split(".")[1]
        # TODO: Replace with right timezone for logs in the query
        agg_query = f"""
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
        self.client.query_api().query(agg_query)
        self.client.delete_api().delete(
            start=f"{yesterday}T00:00:00.000Z",
            stop=f"{today}T00:00:00.000Z",
            predicate=f"_measurement=\"{measurement}\"",
            bucket=current_app.config['METRICS_VECTOR_BUCKET'],
            org=current_app.config['METRICS_INFLUX_DSN']['org']
        )

        retrieve_metrics_query = f"""
            from(bucket: "vector-bucket")
                |> range(start: 0, stop: {today}T00:00:00.000Z)
                |> filter(fn: (r) => (r._field == "{page_type}") and (r._measurement == "count"))
                |> group(columns: ["{id_key}"])
                |> sum()
                |> yield(name: "views-{page_type}")
        """
        result = self.client.query_api().query(retrieve_metrics_query)
        for table in result:
            for record in table:
                # Currently we ignore resource hits as they are different from resource downloads.
                if page_type == "resource_hit":
                    continue
                elif page_type == "resource":
                    # Since resource is not a model per se but part of the dataset model, we need to
                    # treat it separately.
                    try:
                        model_id = uuid.UUID(record[id_key])
                        model_result = get_resource(model_id)
                    except Exception as e:
                        log.exception(e)
                        continue
                else:
                    model = {
                        "dataset": Dataset,
                        "reuse": Reuse,
                        "organization": Organization
                    }[page_type]

                    model_id = record[id_key]
                    model_result = model.objects.filter(id=model_id).first()

                if model_result:
                    log.debug(f"Found {page_type} {model_result.id}")
                    model_result.metrics["views"] = record["_value"]
                    try:
                        model_result.save(signal_kwargs={"ignores": ["post_save"]})
                    except Exception as e:
                        log.exception(e)
                        continue
                else:
                    log.error(f"{page_type} not found", extra={
                        "id": model_id
                    })


def metrics_client_factory():
    dsn = current_app.config["METRICS_INFLUX_DSN"]
    return InfluxClient(dsn)
