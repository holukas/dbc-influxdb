def dropstring():
    return f'|> drop(columns: ["_start", "_stop", "_measurement"])'


def pivotstring():
    return f'|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'


def bucketstring(bucket: str) -> str:
    return f'from(bucket: "{bucket}")'


def rangestring(start: str, stop: str) -> str:
    return f'|> range(start: {start}, stop: {stop})'


def filterstring(queryfor: str, querylist: list, type: str) -> str:
    filterstring = ''  # Query string
    for ix, var in enumerate(querylist):
        if ix == 0:
            filterstring += f'|> filter(fn: (r) => r["{queryfor}"] == "{var}"'
        else:
            filterstring += f' {type} r["{queryfor}"] == "{var}"'
    filterstring = f"{filterstring})"  # Needs bracket at end
    return filterstring


def fields_in_measurement(bucket: str, measurement: str, days: int = 9999) -> str:
    """
    Show all available fields in measurement

    By default, the FluxQL function returns results from the
    last 30d so it is necessary to set the 'start' parameter
    to get ALL fields. Therefore, the start parameter is set
    to -9999d to get all fields available for the last 9999 days.

    Args:
        bucket: bucket name in InfluxDB
        measurement: name of the measurement, e.g. 'TA'
        days: show fields of the last *days* days

    Returns:
        query string for FluxQL
    """
    query = f'''
    import "influxdata/influxdb/schema"
    schema.measurementFieldKeys(
    bucket: "{bucket}",
    measurement: "{measurement}",
    start: -{days}d
    )
    '''
    return query


def fields_in_bucket(bucket: str) -> str:
    query = f'''
    import "influxdata/influxdb/schema"
    schema.fieldKeys(bucket: "{bucket}")
    '''
    return query


def measurements_in_bucket(bucket: str) -> str:
    query = f'''
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "{bucket}")
    '''
    return query


def buckets() -> str:
    query = '''
    buckets()    
    '''
    return query
