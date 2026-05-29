import pytz

# Column names of columns that are used as tags
tags = [
    'site',
    'varname',
    'units',
    'raw_varname',
    'raw_units',
    'hpos',
    'vpos',
    'repl',
    'data_raw_freq',
    'freq',
    # 'freqfrom',
    'filegroup',
    'config_filetype',
    'data_version',
    'gain',
    'offset'
]


def convert_ts_to_timezone(timezone_offset_to_utc_hours: int | float,
                           timestamp_index):
    """Convert a UTC-aware timestamp to a fixed offset from UTC.

    All data in the database are stored in UTC. This converts a UTC-aware
    timestamp (Series/DatetimeIndex) to the timezone given as a fixed offset to
    UTC in hours, e.g. ``1`` for CET (winter time), ``-5`` for US Eastern
    (winter time), or ``5.5`` for India.

    A *fixed* offset is applied exactly as given (no daylight-saving
    transitions). Negative and fractional offsets are supported; fractional
    hours are rounded to the nearest minute.

    Note:
        Previously this used pytz' ``Etc/GMT*`` zones, which carry a
        counter-intuitive reversed sign convention. ``pytz.FixedOffset`` is used
        instead — it gives identical results for whole-hour offsets but is
        clearer and also handles fractional offsets.

    Args:
        timezone_offset_to_utc_hours: offset to UTC in hours (may be negative or
            fractional), e.g. ``1`` for UTC+01:00.
        timestamp_index: a pandas Series/DatetimeIndex with tz-aware (UTC)
            timestamps.

    Returns:
        The timestamps converted to the requested fixed offset.
    """
    offset_minutes = round(timezone_offset_to_utc_hours * 60)
    return timestamp_index.dt.tz_convert(pytz.FixedOffset(offset_minutes))
