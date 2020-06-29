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


def metrics_client_factory():
    dsn = current_app.config['METRICS_DSN']
    return InfluxClient(dsn)
