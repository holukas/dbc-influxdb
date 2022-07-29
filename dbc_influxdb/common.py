import pytz

# Column names of columns that are used as tags
tags = [
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
    'gain'
]

def convert_ts_to_timezone(timezone_offset_to_utc_hours:int,
                           timestamp_index):
    """Convert timestamp index to timezone

    Convert 'TIMESTAMP_END' to desired timezone, e.g. to CET,
    using the pytz package. pytz is quite flexible with GMT and fixed offsets,
    and GMT is the same as UTC (no offset to UTC).

    From: https://pvlib-python.readthedocs.io/en/v0.3.0/timetimezones.html#fixed-offsets:
      "The 'Etc/GMT*' time zones mentioned above provide fixed offset
      specifications, but watch out for the counter-intuitive sign convention."

    :param timezone_offset_to_utc_hours:
    :param timestamp_index:
    :return:
    """

    # Sign convention in pytz is reversed: '+1' for CET must be '-1' when used with GMT here
    sign = '-' if timezone_offset_to_utc_hours >= 0 else '+'

    # Specify pytz timezone in relation to the GMT timezone (same as UTC)
    requested_timezone_pytz = f'Etc/GMT{sign}{timezone_offset_to_utc_hours}'

    # Convert TIMESTAMP_END to requested timezone
    timezoned_ix = timestamp_index.dt.tz_convert(pytz.timezone(requested_timezone_pytz))

    return timezoned_ix