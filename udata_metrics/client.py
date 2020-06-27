from flask import current_app
from influxdb import InfluxDBClient


class InfluxClient:

    def __init__(self, dsn):
        self.client = InfluxDBClient(**dsn)

    def get_views_from_all_datasets(self):
        query = 'select sum(*) from dataset_views where time > now() - 24h group by dataset;'
        result = self.client.query(query)
        return result
    
    def get_views_from_all_resources(self):
        query = 'select sum(*) from resource_views where time > now() - 24h group by resource;'
        result = self.client.query(query)
        return result
    
    def get_views_from_all_community_resources(self):
        query = 'select sum(*) from community_resource_views where time > now() - 24h group by communityresource;'
        result = self.client.query(query)
        return result
    
    def get_views_from_all_reuses(self):
        query = 'select sum(*) from reuse_views where time > now() - 24h group by reuse;'
        result = self.client.query(query)
        return result
    
    def get_views_from_all_organizations(self):
        query = 'select sum(*) from organization_views where time > now() - 24h group by organization;'
        result = self.client.query(query)
        return result
    
    def get_views_from_all_users(self):
        query = 'select sum(*) from user_views where time > now() - 24h group by user_view;'
        result = self.client.query(query)
        return result
    
    def sum_views_from_specific_model(self, model_name, model_id):
        query = f"select sum(*) from {model_name}_views where {model_name}='{model_id}';"
        result = self.client.query(query)
        return result.items()[0]
    
    def sum_views_from_specific_ressource(self, resource_id):
        query = f"select sum(*) from resource_views where resource='{resource_id}';"
        result = self.client.query(query)
        return result.items()[0]
    
    def sum_views_from_specific_com_ressource(self, com_resource_id):
        query = f"select sum(*) from community_resource_views where communityresource='{com_resource_id}';"
        result = self.client.query(query)
        return result.items()[0]
    
    def sum_views_from_specific_user(self, user_id):
        query = f"select sum(*) from user_views where user_view='{user_id}';"
        result = self.client.query(query)
        return result.items()[0]

    def insert(self, body):
        self.client.write_points([body])


def metrics_client_factory():
    dsn = current_app.config['METRICS_DSN']
    return InfluxClient(dsn)
