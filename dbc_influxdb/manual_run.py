from pathlib import Path

from dbc_influxdb.main import dbcInflux


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

    # Settings
    SITE = 'ch-tan'  # Site name
    VAR1 = 'Power_Tot_X_X_X'
    DATA_VERSION = 'raw'
    DIRCONF = r'L:\Sync\luhk_work\20 - CODING\22 - POET\configs'  # Folder with configurations
    MEASUREMENTS = ['_instrumentmetrics']  # Measurement name
    FIELDS = [VAR1]  # Variable name; InfluxDB stores variable names as '_field'
    START = '2024-03-05 00:00:01'  # Download data starting with this date
    STOP = '2024-03-15 00:00:01'  # Download data before this date (the stop date itself is not included)
    TIMEZONE_OFFSET_TO_UTC_HOURS = 1  # Timezone, e.g. "1" is translated to timezone "UTC+01:00" (CET, winter time)

    # Instantiate class
    dbc = dbcInflux(dirconf=DIRCONF)

    # Data download
    data_simple, data_detailed, assigned_measurements = \
        dbc.download(
            bucket=f"{SITE}_raw",
            measurements=MEASUREMENTS,
            fields=FIELDS,
            start=START,
            stop=STOP,
            timezone_offset_to_utc_hours=TIMEZONE_OFFSET_TO_UTC_HOURS,
            data_version=DATA_VERSION
        )

    # data_simple.to_csv("F:\Downloads\_temp\del.csv")
    print(data_simple)


if __name__ == '__main__':
    # upload_specific_file()
    download()
