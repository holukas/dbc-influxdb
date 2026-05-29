"""Example / manual runner for dbc-influxdb.

These are hand-run examples that show how to use the public API
(:meth:`dbcInflux.download`, :meth:`dbcInflux.upload_singlevar`,
:meth:`dbcInflux.delete`). They are NOT tests and are not imported by the
package.

Adjust the hardcoded paths (``DIRCONF``, output CSV paths, ...) to your own
environment before running, then enable the desired call in ``__main__``.
"""
from dbc_influxdb import dbcInflux

# Folder with configuration files (adjust to your environment).
# A sibling folder "<DIRCONF>_secret" must contain dbconf.yaml. See example_configs/.
DIRCONF = r'L:\Sync\luhk_work\20 - CODING\22 - POET\configs'


def download():
    """Download data from the database."""
    SITE = 'ch-dav'  # Site name
    BUCKET = f'{SITE}_processed'
    DATA_VERSION = ['meteoscreening_diive', 'meteoscreening_mst']
    MEASUREMENTS = ['SWC']  # Measurement name(s); True downloads all measurements
    FIELDS = None  # None means download all fields from measurements
    START = '2006-01-01 00:00:01'  # Download data starting with this date
    STOP = '2025-01-01 00:00:01'  # Download data before this date (stop date itself is not included)
    TIMEZONE_OFFSET_TO_UTC_HOURS = 1  # "1" -> "UTC+01:00" (CET, winter time)

    dbc = dbcInflux(dirconf=DIRCONF)

    data_simple, data_detailed, assigned_measurements = dbc.download(
        bucket=BUCKET,
        measurements=MEASUREMENTS,
        fields=FIELDS,
        start=START,
        stop=STOP,
        timezone_offset_to_utc_hours=TIMEZONE_OFFSET_TO_UTC_HOURS,
        data_version=DATA_VERSION,
    )

    print(data_simple)


def download_and_reupload():
    """Download data, adjust tags, then re-upload to a different bucket."""
    SITE = 'ch-cha'  # Site name

    dbc = dbcInflux(dirconf=DIRCONF)

    download_settings = dict(
        bucket=f'{SITE}_processed',
        start='2023-01-01 00:00:01',  # Download data starting with this date
        stop='2023-02-01 00:00:01',  # Download data before this date (stop date itself is not included)
        data_version='meteoscreening_diive',
        timezone_offset_to_utc_hours=1,  # "1" -> "UTC+01:00" (CET, winter time)
    )

    data_simple, data_detailed, assigned_measurements = dbc.download(**download_settings)

    dkeys = data_detailed.keys()

    # Rename columns where needed
    oldcols = [oldkey for oldkey in list(dkeys) if '_T1B2_' in oldkey]
    for oldcol in oldcols:
        newcol = str(oldcol).replace('_T1B2_', '_T1_')
        assigned_measurements[newcol] = assigned_measurements.pop(oldcol)
        data_detailed[newcol] = data_detailed.pop(oldcol)
        data_detailed[newcol] = data_detailed[newcol].rename(columns={oldcol: newcol}, inplace=False)
        data_detailed[newcol]['hpos'] = 'T1'
        data_detailed[newcol]['varname'] = newcol

    # Update available dict keys
    dkeys = data_detailed.keys()

    # Update tags for all variables
    for var in dkeys:
        data_detailed[var]['site'] = SITE
        data_detailed[var]['offset'] = 0.0  # float
        data_detailed[var]['gain'] = 1.0  # float
        data_detailed[var]['data_version'] = 'meteoscreening_diive'
        # Convert frequency strings to current pandas convention
        for f in ['freq', 'data_raw_freq']:
            data_detailed[var][f] = data_detailed[var][f].replace('30T', '30min')
            data_detailed[var][f] = data_detailed[var][f].replace('10T', '10min')
            data_detailed[var][f] = data_detailed[var][f].replace('T', 'min')
            data_detailed[var][f] = data_detailed[var][f].replace('10S', '10s')
            data_detailed[var][f] = data_detailed[var][f].replace('S', 's')
            data_detailed[var][f] = data_detailed[var][f].replace('H', 'h')

    for var in dkeys:
        to_measurement = assigned_measurements[var]
        dbc.upload_singlevar(
            var_df=data_detailed[var],
            to_bucket='ch-cha_processed',
            to_measurement=to_measurement,
            timezone_offset_to_utc_hours=1,
            delete_from_db_before_upload=False,
        )


def delete():
    """Delete data from the database."""
    BUCKET = 'ch-dav_raw'
    DATA_VERSION = 'raw'
    MEASUREMENTS = True
    FIELDS = True
    START = '2025-07-01 00:00:01'  # Delete data starting with this date
    STOP = '2026-01-01 00:00:01'  # Delete data before this date (stop date itself is not included)
    TIMEZONE_OFFSET_TO_UTC_HOURS = 1  # "1" -> "UTC+01:00" (CET, winter time)

    dbc = dbcInflux(dirconf=DIRCONF)

    dbc.delete(
        bucket=BUCKET,
        measurements=MEASUREMENTS,
        fields=FIELDS,
        start=START,
        stop=STOP,
        timezone_offset_to_utc_hours=TIMEZONE_OFFSET_TO_UTC_HOURS,
        data_version=DATA_VERSION,
    )


if __name__ == '__main__':
    import pandas as pd

    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)

    # Enable the example you want to run:
    # download()
    # download_and_reupload()
    # delete()
