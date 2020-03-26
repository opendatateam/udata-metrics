from flask import current_app
from influxdb import InfluxDBClient


class InfluxClient:

    def __init__(self, dsn):
        self.client = InfluxDBClient(**dsn)

    def get_views_from_all_datasets(self):
        query = 'select sum(*) from dataset_views group by dataset;'
        result = self.client.query(query)
        return result

    def insert(self, body):
        self.client.write_points([body])


