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


def fields_in_measurement(bucket: str, measurement: str) -> str:
    query = f'''
    import "influxdata/influxdb/schema"
    schema.measurementFieldKeys(
    bucket: "{bucket}",
    measurement: "{measurement}")
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
