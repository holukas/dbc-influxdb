"""Construction of Flux query strings.

Note:
    These helpers build Flux queries by interpolating the given bucket,
    measurement, field and data-version names directly into the query string
    without escaping. They assume *trusted* input (names originate from the
    local config files / the caller, not from untrusted external sources). Do
    not pass unsanitised user input.
"""


def pivotstring():
    return f'|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'


def bucketstring(bucket: str) -> str:
    return f'from(bucket: "{bucket}")'


def rangestring(start: str, stop: str) -> str:
    return f'|> range(start: {start}, stop: {stop})'


def filterstring(queryfor: str, querylist: list, logic: str) -> str:
    """Build a Flux ``filter()`` that matches *queryfor* against any value in
    *querylist*, combined with the given *logic* operator (e.g. ``'or'``)."""
    filterstring = ''  # Query string
    for ix, var in enumerate(querylist):
        if ix == 0:
            filterstring += f'|> filter(fn: (r) => r["{queryfor}"] == "{var}"'
        else:
            filterstring += f' {logic} r["{queryfor}"] == "{var}"'
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


def data_versions_in_bucket(bucket: str, days: int = 9999) -> str:
    """Distinct values of the ``data_version`` tag in *bucket*.

    Uses a long lookback (``-days`` days) because ``schema.tagValues`` defaults
    to the last 30 days, which would miss versions of older data.
    """
    query = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "{bucket}", tag: "data_version", start: -{days}d)
    '''
    return query


def measurements_for_version(bucket: str, data_version: str, days: int = 9999) -> str:
    """Distinct measurements that carry *data_version* in *bucket*.

    Filters the ``_measurement`` tag values by a predicate on the
    ``data_version`` tag, so only measurements stored under that version are
    returned.
    """
    query = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
        bucket: "{bucket}",
        tag: "_measurement",
        predicate: (r) => r["data_version"] == "{data_version}",
        start: -{days}d
    )
    '''
    return query


def fields_for_version(bucket: str, measurement: str, data_version: str,
                       days: int = 9999) -> str:
    """Distinct fields of *measurement* that carry *data_version* in *bucket*."""
    query = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
        bucket: "{bucket}",
        tag: "_field",
        predicate: (r) => r["_measurement"] == "{measurement}" and r["data_version"] == "{data_version}",
        start: -{days}d
    )
    '''
    return query


def units_in_field(bucket: str, measurement: str, field: str,
                   data_version: str = None, days: int = 9999) -> str:
    """Distinct values of the ``units`` tag for one *field* of one *measurement*,
    optionally narrowed to one *data_version*.

    ``schema.tagValues`` supports a ``predicate`` over ``_measurement`` /
    ``_field`` (per the InfluxDB schema docs), so this returns the unit(s)
    actually stored for that specific variable.
    """
    predicate = f'r["_measurement"] == "{measurement}" and r["_field"] == "{field}"'
    if data_version:
        predicate += f' and r["data_version"] == "{data_version}"'
    query = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
        bucket: "{bucket}",
        tag: "units",
        predicate: (r) => {predicate},
        start: -{days}d
    )
    '''
    return query
