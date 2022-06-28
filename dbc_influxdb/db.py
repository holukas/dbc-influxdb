from influxdb_client import InfluxDBClient


# from influxdb_client import WriteOptions


def get_client(conf_db: dict):
    client = InfluxDBClient(url=conf_db['url'], token=conf_db['token'], org=conf_db['org'],
                            timeout=20_000, enable_gzip=True)
    return client


def get_query_api(client):
    query_api = client.query_api()
    return query_api

# def get_write_api(client):
#     write_api = client.write_api(write_options=WriteOptions(
#         batch_size=5000, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000,
#         max_retries=5, max_retry_delay=30_000, exponential_base=2))
#     return write_api
