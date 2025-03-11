# Ignore future warnings for pandas 3.0
import warnings
from pathlib import Path

from dbc_influxdb.main import dbcInflux

warnings.simplefilter(action='ignore', category=FutureWarning)


def upload_specific_file():
    """
    Upload specific file to database
    """

    dir_base = r'L:\Sync\luhk_work\40 - DATA\DATASETS\FLUXNET-WW2020_RELEASE-2022-1 Swiss sites'

    # CH-AWS
    # to_bucket = 'ch-aws_processing'
    # dir_site = r'FLX_CH-Aws_FLUXNET2015_FULLSET_2006-2020_beta-3'
    # file_site = r'FLX_CH-Aws_FLUXNET2015_FULLSET_HH_2006-2020_beta-3.csv'

    # CH-CHA
    # to_bucket = 'ch-cha_processing'
    # dir_site = r'FLX_CH-Cha_FLUXNET2015_FULLSET_2005-2020_beta-3'
    # file_site = r'FLX_CH-Cha_FLUXNET2015_FULLSET_HH_2005-2020_beta-3.csv'

    # CH-DAV
    # to_bucket = 'ch-dav_processing'
    # dir_site = r'FLX_CH-Dav_FLUXNET2015_FULLSET_1997-2020_beta-3'
    # file_site = r'FLX_CH-Dav_FLUXNET2015_FULLSET_HH_1997-2020_beta-3.csv'

    # CH-FRU
    # to_bucket = 'ch-fru_processing'
    # dir_site = r'FLX_CH-Fru_FLUXNET2015_FULLSET_2005-2020_beta-3'
    # file_site = r'FLX_CH-Fru_FLUXNET2015_FULLSET_HH_2005-2020_beta-3.csv'

    # CH-LAE
    to_bucket = 'ch-lae_processing'
    dir_site = r'FLX_CH-Lae_FLUXNET2015_FULLSET_2004-2020_beta-3'
    file_site = r'FLX_CH-Lae_FLUXNET2015_FULLSET_HH_2004-2020_beta-3.csv'

    # # CH-OE2
    # to_bucket = 'ch-oe2_processing'
    # dir_site = r'FLX_CH-Oe2_FLUXNET2015_FULLSET_2004-2020_beta-3'
    # file_site = r'FLX_CH-Oe2_FLUXNET2015_FULLSET_HH_2004-2020_beta-3.csv'

    # to_bucket = 'test'
    # dir_base = r'F:\Downloads'
    # dir_site = r'_temp'
    # file_site = r'CH-DAV_iDL_T1_35_1_TBL1_2022_10_25_0000.dat.csv'

    # Prepare upload settings
    filepath = str(Path(dir_base) / Path(dir_site) / file_site)

    # Important tag
    data_version = 'FLUXNET-WW2020_RELEASE-2022-1'
    # data_version = 'raw'

    # Configurations
    dirconf = r'L:\Sync\luhk_work\20 - CODING\22 - POET\configs'

    # Filetype
    filetype = 'PROC-FLUXNET-FULLSET-HH-CSV-30MIN'
    # filetype = 'DAV10-RAW-TBL1-201802281101-TOA5-DAT-10S'

    # Instantiate class
    dbc = dbcInflux(dirconf=dirconf)

    # Read datafile
    df, filetypeconf, fileinfo = dbc.readfile(filepath=filepath,
                                              filetype=filetype,
                                              nrows=None,
                                              timezone_of_timestamp='UTC+01:00')

    # Upload file data to database
    varscanner_df, freq, freqfrom = dbc.upload_filetype(
        to_bucket=to_bucket,
        file_df=df[0],
        filetypeconf=filetypeconf,
        fileinfo=fileinfo,
        data_version=data_version,
        parse_var_pos_indices=filetypeconf['data_vars_parse_pos_indices'],
        timezone_of_timestamp='UTC+01:00',
        data_vars=filetypeconf['data_vars'])

    print(varscanner_df)


def download():
    """
    Download data from database
    """

    # TA_T1_2_1

    # Settings
    SITE = 'ch-dav'  # Site name
    BUCKET = f'{SITE}_processed'
    # VAR1 = 'TA_T1_2_1'
    DATA_VERSION = ['meteoscreening_diive', 'meteoscreening_mst']
    DIRCONF = r'L:\Sync\luhk_work\20 - CODING\22 - POET\configs'  # Folder with configurations
    # MEASUREMENTS = True  # True downloads all measurements
    MEASUREMENTS = ['SWC']  # Measurement name
    FIELDS = None  # None means download all fields from measurements
    # FIELDS = [VAR1]  # Variable name; InfluxDB stores variable names as '_field'
    START = '2006-01-01 00:00:01'  # Download data starting with this date
    STOP = '2025-01-01 00:00:01'  # Download data before this date (the stop date itself is not included)
    TIMEZONE_OFFSET_TO_UTC_HOURS = 1  # Timezone, e.g. "1" is translated to timezone "UTC+01:00" (CET, winter time)

    # Instantiate class
    dbc = dbcInflux(dirconf=DIRCONF)

    # Data download
    data_simple, data_detailed, assigned_measurements = \
        dbc.download(
            bucket=BUCKET,
            measurements=MEASUREMENTS,
            fields=FIELDS,
            start=START,
            stop=STOP,
            timezone_offset_to_utc_hours=TIMEZONE_OFFSET_TO_UTC_HOURS,
            data_version=DATA_VERSION
        )

    data_simple.to_csv(r"F:\TMP\del.csv")
    print(data_simple)


def download_and_reupload():
    """
    Download data from database bucket, adjust tags and then re-upload to different bucket
    """

    # Settings
    SITE = 'ch-cha'  # Site name
    DIRCONF = r'F:\Sync\luhk_work\20 - CODING\22 - POET\configs'  # Folder with configurations

    # Instantiate class
    dbc = dbcInflux(dirconf=DIRCONF)

    download_settings = dict(
        bucket=f'{SITE}_processed',
        # fields=['TA_T1_2_1'],  # Variable name; InfluxDB stores variable names as '_field'
        # measurements=['TA'],  # Measurement name
        start='2023-01-01 00:00:01',  # Download data starting with this date
        stop='2023-02-01 00:00:01',  # Download data before this date (the stop date itself is not included)
        data_version='meteoscreening_diive',
        timezone_offset_to_utc_hours=1  # Timezone, e.g. "1" is translated to timezone "UTC+01:00" (CET, winter time)
    )

    # Data download
    data_simple, data_detailed, assigned_measurements = \
        dbc.download(**download_settings)

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

    # for c in data_detailed[VAR1].columns:
    #     print(data_detailed[VAR1][c])

    for var in dkeys:
        to_measurement = assigned_measurements[var]
        dbc.upload_singlevar(
            var_df=data_detailed[var],
            to_bucket='ch-cha_processed',
            to_measurement=to_measurement,
            timezone_offset_to_utc_hours=1,
            delete_from_db_before_upload=False
        )

    # data_simple.to_csv("F:\Downloads\_temp\del.csv")
    # print(data_simple)


def delete():
    """
    Delete data from database
    """

    # Settings
    BUCKET = f'ch-dav_processed'
    # BUCKET = f'ch-tan_raw'
    # VAR1 = 'TRH_M1_2_1'
    # VAR2 = 'TRH_T1_4_1'
    # DATA_VERSION = 'raw'
    DATA_VERSION = 'meteoscreening_mst'
    # DATA_VERSION = 'fluxnet_v2024'
    # DATA_VERSION = 'eddypro_level-0'
    # DATA_VERSION = 'ms_maier2022'
    DIRCONF = r'L:\Sync\luhk_work\20 - CODING\22 - POET\configs'  # Folder with configurations
    MEASUREMENTS = True
    # MEASUREMENTS = ['NETRAD']  # Measurement name
    FIELDS = True
    # FIELDS = [VAR1, VAR2]  # Variable name; InfluxDB stores variable names as '_field'
    START = '2021-01-01 00:00:01'  # Delete data starting with this date
    STOP = '2022-01-01 00:00:01'  # Delete data before this date (the stop date itself is not included)
    TIMEZONE_OFFSET_TO_UTC_HOURS = 1  # Timezone, e.g. "1" is translated to timezone "UTC+01:00" (CET, winter time)

    # Instantiate class
    dbc = dbcInflux(dirconf=DIRCONF)

    # Delete data
    dbc.delete(
        bucket=BUCKET,
        measurements=MEASUREMENTS,
        fields=FIELDS,
        start=START,
        stop=STOP,
        timezone_offset_to_utc_hours=TIMEZONE_OFFSET_TO_UTC_HOURS,
        data_version=DATA_VERSION
    )


if __name__ == '__main__':
    import pandas as pd

    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    # upload_specific_file()
    download()
    # delete()
    # download_and_reupload()
