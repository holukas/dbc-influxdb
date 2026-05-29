from influxdb_client import InfluxDBClient


def get_client(conf_db: dict):
    client = InfluxDBClient(url=conf_db['url'], token=conf_db['token'], org=conf_db['org'],
                            timeout=999_000, enable_gzip=True)
    return client


def get_query_api(client):
    query_api = client.query_api()
    return query_api

def get_delete_api(client):
    delete_api = client.delete_api()
    return delete_api
