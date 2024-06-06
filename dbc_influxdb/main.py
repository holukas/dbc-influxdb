# https://www.geeksforgeeks.org/getter-and-setter-in-python/
import fnmatch
import os
from pathlib import Path

import dateutil.parser as parser
import dbc_influxdb.fluxql as fluxql
import pandas as pd
import yaml
from dbc_influxdb.common import tags, convert_ts_to_timezone
from dbc_influxdb.db import get_client, get_query_api
from dbc_influxdb.varscanner import VarScanner
from influxdb_client import WriteOptions
from pandas import DataFrame


class dbcInflux:
    script_id = "dbc"

    def __init__(self,
                 dirconf: str):

        self.dirconf = Path(dirconf)

        self.conf_filetypes, \
            self.conf_unitmapper, \
            self.conf_dirs, \
            self.conf_db = self._read_configs()

        self._test_connection_to_db()

        # self.client = get_client(self.conf_db)
        # self.query_api = get_query_api(client=self.client)

        self._bucket = None
        self._measurements = None
        self._fields = None

    def upload_singlevar(self,
                         var_df: DataFrame,
                         to_bucket: str,
                         to_measurement: str,
                         timezone_of_timestamp: str):
        """Upload single variable to database

        The database needs to know the timezone because all data in the db are
        stored in UTC/GMT.

        :param var_df: contains measured variable data and tags (data_detailed)
        :param to_bucket: name of database bucket
        :param to_measurement: name of measurement, e.g. 'TA'
        :param timezone_of_timestamp: e.g. 'UTC+01:00', see docstring in `.add_timezone_info' for more details
        :return:
        """

        # Add timezone info
        var_df.index = self._add_timezone_info(timestamp_index=var_df.index,
                                               timezone_of_timestamp=timezone_of_timestamp)

        # Database clients
        print("Connecting to database ...")
        client = get_client(conf_db=self.conf_db)

        # The WriteApi in batching mode (default mode) is suppose to run as a singleton.
        # To flush all your data you should wrap the execution using with
        # client.write_api(...) as write_api: statement or call write_api.close()
        # at the end of your script.
        # https://influxdb-client.readthedocs.io/en/stable/usage.html#write
        with client.write_api(write_options=WriteOptions(batch_size=5000,
                                                         flush_interval=10_000,
                                                         jitter_interval=2_000,
                                                         retry_interval=5_000,
                                                         max_retries=5,
                                                         max_retry_delay=30_000,
                                                         exponential_base=2)) as write_api:

            data_cols = var_df.columns.to_list()

            # Check if data contain all tag columns
            cols_not_in_data = [l for l in tags if l not in data_cols]
            if len(cols_not_in_data) > 0:
                raise Exception(f"Data do not contain required tag columns: {cols_not_in_data}")

            # Detect field name (variable name)
            # The field name is the name of the column that is not part of the tags
            field = [l for l in data_cols if l not in tags]
            if len(field) > 1:
                raise Exception("Only one field (variable name) allowed.")

            # Write to db
            # Output also the source file to log
            print(f"--> UPLOAD TO DATABASE BUCKET {to_bucket}:  {field}")

            write_api.write(to_bucket,
                            record=var_df,
                            data_frame_measurement_name=to_measurement,
                            data_frame_tag_columns=tags,
                            write_precision='s')

            print("Upload finished.")

    def upload_filetype(self,
                        file_df: DataFrame,
                        data_vars: dict,
                        data_raw_freq: str,
                        freq: str,
                        to_bucket: str,
                        config_filetype: str,
                        filetypeconf: dict,
                        timezone_of_timestamp: str,
                        ingest: bool=True,
                        logger=None) -> DataFrame:
        """
        Upload data from file

        Primary method to automatically upload data from files to
        the database with the 'dataflow' package.

        Args:
            file_df:
            data_version:
            data_vars:
            fileinfo:
            to_bucket:
            filetypeconf:
            timezone_of_timestamp: e.g. 'UTC+01:00', see docstring in `.add_timezone_info' for more details
            parse_var_pos_indices:
            ingest: If False, no data will be uploaded to database, but VarScanner will still run.
                Implemented for testing purposes.
            logger:

        Returns:

        """

        # Add timezone info
        if not file_df.index.tzinfo:
            file_df.index = self._add_timezone_info(timestamp_index=file_df.index,
                                                    timezone_of_timestamp=timezone_of_timestamp)

        varscanner = VarScanner(file_df=file_df,
                                data_vars=data_vars,
                                data_raw_freq=data_raw_freq,
                                freq=freq,
                                config_filetype=config_filetype,
                                filetypeconf=filetypeconf,
                                conf_unitmapper=self.conf_unitmapper,
                                to_bucket=to_bucket,
                                conf_db=self.conf_db,
                                ingest=ingest,
                                logger=logger)
        varscanner.run()
        return varscanner.get_results()

    def download(self,
                 bucket: str,
                 measurements: list,
                 fields: list,
                 start: str,
                 stop: str,
                 timezone_offset_to_utc_hours: int,  # v0.3.0
                 data_version: str,
                 verify_freq: str = False) -> tuple[DataFrame, dict, dict]:
        """
        Get data from database between 'start' and 'stop' dates

        The exact 'stop' date is NOT included.

        The args 'start' and 'stop' dates are in relation to the same timezone
        as defined in 'timezone_offset_to_utc_hours'. This means that if the offset
        is e.g. '1' for CET, then the dates are also understood to be in the same
        timezone, e.g. CET.

        Args:
            bucket: name of bucket in database
            measurements: list of measurements in database, e.g. ['TA', 'SW']
            fields: list of fields (variable names)
            start: start date, e.g. '2022-07-04 00:30:00'
            stop: stop date, e.g. '2022-07-05 12:00:00'
            timezone_offset_to_utc_hours: convert the UTC timestamp from the
                database to this timezone offset, e.g. if '1' then data are downloaded
                and returned with the timestamp 'UTC+01:00', i.e. UTC + 1 hour, which
                corresponds to CET (winter time)
            data_version: version ID of the data that should be downloaded,
                e.g. 'meteoscreening'
            verify_freq: checks if the downloaded data has the expected frequency, given
                as str in the format of pandas frequency strings, e.g., '30T' for 30-minute
                data.

        """

        print(f"Downloading from bucket {bucket}:\n"
              f"    variables {fields} from measurements {measurements}\n"
              f"    between {start} and {stop}\n"
              f"    in data version {data_version}\n"
              f"    with timezone offset to UTC of {timezone_offset_to_utc_hours}")

        # InfluxDB needs ISO 8601 date format (in requested timezone) for query
        start_iso = self._convert_datestr_to_iso8601(datestr=start,
                                                     timezone_offset_to_utc_hours=timezone_offset_to_utc_hours)
        stop_iso = self._convert_datestr_to_iso8601(datestr=stop,
                                                    timezone_offset_to_utc_hours=timezone_offset_to_utc_hours)

        # Assemble query
        bucketstring = fluxql.bucketstring(bucket=bucket)
        rangestring = fluxql.rangestring(start=start_iso, stop=stop_iso)
        measurementstring = fluxql.filterstring(queryfor='_measurement', querylist=measurements, type='or')
        fieldstring = fluxql.filterstring(queryfor='_field', querylist=fields, type='or')
        dropstring = fluxql.dropstring()
        pivotstring = fluxql.pivotstring()

        if data_version:
            dataversionstring = fluxql.filterstring(queryfor='data_version', querylist=[data_version], type='or')
            querystring = f"{bucketstring} {rangestring} {measurementstring} " \
                          f"{dataversionstring} {fieldstring} {dropstring} {pivotstring}"
        else:
            # keepstring = f'|> keep(columns: ["_time", "_field", "_value", "units", "freq"])'
            querystring = f"{bucketstring} {rangestring} {measurementstring} " \
                          f"{fieldstring} {dropstring} {pivotstring}"

        # Run database query
        client = get_client(self.conf_db)
        query_api = get_query_api(client)
        tables = query_api.query_data_frame(query=querystring)  # List of DataFrames
        client.close()

        # In case only one single variable is downloaded, the query returns
        # a single dataframe. If multiple variables are downloaded, the query
        # returns a list of dataframes. To keep these two options consistent,
        # single dataframes are converted to a list, in which case the list
        # contains only one element: the dataframe of the single variable.
        tables = [tables] if not isinstance(tables, list) else tables

        # # Check units and frequencies
        # units, freq = self._check_if_same_units_freq(results=results, field=field)

        # Each table in tables contains data for one variable
        data_detailed = {}  # Stores variables and their tags
        data_simple = DataFrame()  # Stores variables
        for ix, table in enumerate(tables):

            table.drop(columns=['result', 'table'], inplace=True)

            # Queries are always returned w/ UTC timestamp
            # Create timestamp columns
            table.rename(columns={"_time": "TIMESTAMP_UTC_END"}, inplace=True)
            table['TIMESTAMP_END'] = table['TIMESTAMP_UTC_END'].copy()

            # TIMEZONE: convert timestamp index to required timezone
            table['TIMESTAMP_END'] = convert_ts_to_timezone(
                timezone_offset_to_utc_hours=timezone_offset_to_utc_hours,
                timestamp_index=table['TIMESTAMP_END'])

            # Remove timezone info in timestamp from TIMESTAMP_END
            # -> download clean timestamp without timestamp info
            table['TIMESTAMP_END'] = table['TIMESTAMP_END'].dt.tz_localize(None)  # Timezone!

            # Set TIMESTAMP_END as the main index
            table.set_index("TIMESTAMP_END", inplace=True)
            table.sort_index(inplace=True)

            # Remove duplicated index entries, v0.4.1
            # This can happen if the variable is logged in a new file, but the
            # old file is still active and also contains data for the var.
            # In this case, keep the last data entry.
            table = table[~table.index.duplicated(keep='last')]

            # Remove timezone info from UTC timestamp, header already states it's UTC
            table['TIMESTAMP_UTC_END'] = table['TIMESTAMP_UTC_END'].dt.tz_localize(None)  # Timezone!

            # Remove UTC timestamp from columns
            table.drop('TIMESTAMP_UTC_END', axis=1, inplace=True)

            # Detect of which variable the frame contains data
            field_in_table = [f for f in fields if f in table.columns]

            key = field_in_table[0]

            # Collect variables without tags in a separate (simplified) dataframe.
            # This dataframe only contains the timestamp and the data column of each var.
            # :: refactored in v0.7.0
            # Add new column if column does not exist in current df
            incomingdata = pd.DataFrame(table[key])
            data_simple = data_simple.combine_first(incomingdata)
            data_simple = data_simple[~data_simple.index.duplicated(keep='last')]
            # if ix == 0:
            #     data_simple = table[[key]].copy()
            # else:
            #     if key not in data_simple.columns:
            #         data_simple[key] = table[[key]].copy()
            #     else:
            #         # If var already exists as column in df, merge
            #         # incoming data with the data that are already in df.
            #         incomingdata = pd.DataFrame(table[key])
            #         data_simple = data_simple.combine_first(incomingdata)
            #
            #         # Remove duplicates from incoming data
            #         data_simple = data_simple[~data_simple.index.duplicated(keep='last')]

            # Store frame in dict with the field (variable name) as key
            # This way the table (data) of each variable can be accessed by
            # field name, i.e., variable name.
            # Important: variables with different sets of tags are downloaded
            # in their own table. Therefore, if a variable TA_T1_X_1 has e.g.
            # different time resolutions it is downloaded as multiple tables.
            # Since the table is stored with the name of the variable, it is
            # thus necessary to check whether a table with the name of the
            # var already exists in the dict 'data_detailed'. If yes, the table
            # is added (.combine_first) to the already existing table. It is also
            # necessary to check whether there are index duplicated present
            # after the table merging.
            # :: added in v0.7.0
            if key not in data_detailed:
                # Add table df as new dict entry
                data_detailed[key] = table
            else:
                data_detailed[key] = data_detailed[key].combine_first(table)
                data_detailed[key] = data_detailed[key][~data_detailed[key].index.duplicated(keep='last')]

        # Info
        print(f"Downloaded data for {len(data_detailed)} variables:")
        for key, val in data_detailed.items():
            num_records = len(data_detailed[key])
            first_date = data_detailed[key].index[0]
            last_date = data_detailed[key].index[-1]
            print(f"    {key}   ({num_records} records)     "
                  f"first date: {first_date}    last date: {last_date}")

        assigned_measurements = self._detect_measurement_for_field(bucket=bucket,
                                                                   measurementslist=measurements,
                                                                   varnameslist=list(data_detailed.keys()))

        # TODO hier weiter check verify frequency
        if verify_freq:
            from varscanner import infer_freq
            freq, freqfrom = infer_freq(df_index=data_simple.index)

        return data_simple, data_detailed, assigned_measurements

    def show_configs_unitmapper(self) -> dict:
        return self.conf_unitmapper

    def show_configs_dirs(self) -> dict:
        return self.conf_dirs

    def show_configs_filetypes(self) -> dict:
        return self.conf_filetypes

    def show_config_for_filetype(self, filetype: str) -> dict:
        return self.conf_filetypes[filetype]

    def show_fields_in_measurement(self, bucket: str, measurement: str, days: int = 9999) -> list:
        """Show fields (variable names) in measurement"""
        query = fluxql.fields_in_measurement(bucket=bucket, measurement=measurement, days=days)
        client = get_client(self.conf_db)
        query_api = get_query_api(client)
        results = query_api.query_data_frame(query=query)
        client.close()
        fieldslist = results['_value'].tolist()
        print(f"{'=' * 40}\nFields in measurement {measurement} of bucket {bucket}:")
        for ix, f in enumerate(fieldslist, 1):
            print(f"#{ix}  {bucket}  {measurement}  {f}")
        print(f"Found {len(fieldslist)} fields in measurement {measurement} of bucket {bucket}.\n{'=' * 40}")
        return fieldslist

    def show_fields_in_bucket(self, bucket: str) -> list:
        """Show fields (variable names) in bucket"""
        query = fluxql.fields_in_bucket(bucket=bucket)
        client = get_client(self.conf_db)
        query_api = get_query_api(client)
        results = query_api.query_data_frame(query=query)
        client.close()
        fieldslist = results['_value'].tolist()
        print(f"{'=' * 40}\nFields in bucket {bucket}:")
        for ix, f in enumerate(fieldslist, 1):
            print(f"#{ix}  {bucket}  {f}")
        print(f"Found {len(fieldslist)} measurements in bucket {bucket}.\n{'=' * 40}")
        return fieldslist

    def show_measurements_in_bucket(self, bucket: str) -> list:
        """Show measurements in bucket"""
        query = fluxql.measurements_in_bucket(bucket=bucket)
        client = get_client(self.conf_db)
        query_api = get_query_api(client)
        results = query_api.query_data_frame(query=query)
        client.close()
        measurements = results['_value'].tolist()
        print(f"{'=' * 40}\nMeasurements in bucket {bucket}:")
        for ix, m in enumerate(measurements, 1):
            print(f"#{ix}  {bucket}  {m}")
        print(f"Found {len(measurements)} measurements in bucket {bucket}.\n{'=' * 40}")
        return measurements

    def show_buckets(self) -> list:
        """Show all buckets in the database"""
        query = fluxql.buckets()
        client = get_client(self.conf_db)
        query_api = get_query_api(client)
        results = query_api.query_data_frame(query=query)
        client.close()
        results.drop(columns=['result', 'table'], inplace=True)
        bucketlist = results['name'].tolist()
        bucketlist = [x for x in bucketlist if not x.startswith('_')]
        for ix, b in enumerate(bucketlist, 1):
            print(f"#{ix}  {b}")
        print(f"Found {len(bucketlist)} buckets in database.")
        return bucketlist

    @staticmethod
    def _add_timezone_info(timestamp_index, timezone_of_timestamp: str):
        """Add timezone info to timestamp index

        No data are changed, only the timezone info is added to the timestamp.

        :param: timezone_of_timestamp: If 'None', no timezone info is added. Otherwise
            can be `str` that describes the timezone in relation to UTC in the format:
            'UTC+01:00' (for CET), 'UTC+02:00' (for CEST), ...
            InfluxDB uses this info to upload data (always) in UTC/GMT.

        see: https://www.atmos.albany.edu/facstaff/ktyle/atm533/core/week5/04_Pandas_DateTime.html#note-that-the-timezone-is-missing-the-read-csv-method-does-not-provide-a-means-to-specify-the-timezone-we-can-take-care-of-that-though-with-the-tz-localize-method

        """
        return timestamp_index.tz_localize(timezone_of_timestamp)  # v0.3.1

    def readfile(self, filepath: str, filetypeconf: str, nrows=None, logger=None, timezone_of_timestamp=None):
        # Read data of current file
        logtxt = f"[{self.script_id}] Reading file {filepath} ..."
        logger.info(logtxt) if logger else print(logtxt)
        # filetypeconf = self.conf_filetypes[filetype]
        df_list, fileinfo, missed_ids = FileTypeReader(filepath=filepath,
                                                       filetype=filetype,
                                                       filetypeconf=filetypeconf,
                                                       nrows=nrows).get_data()
        return df_list, fileinfo, missed_ids

    def _read_configs(self):

        # # Search in this file's folder
        # _dir_main = Path(__file__).parent.resolve()

        # Config locations
        _dir_filegroups = self.dirconf / 'filegroups'
        _file_unitmapper = self.dirconf / 'units.yaml'
        _file_dirs = self.dirconf / 'dirs.yaml'
        _file_dbconf = Path(f"{self.dirconf}_secret") / 'dbconf.yaml'

        # Read configs
        conf_filetypes = get_conf_filetypes(folder=_dir_filegroups)
        conf_unitmapper = read_configfile(config_file=_file_unitmapper)
        conf_dirs = read_configfile(config_file=_file_dirs)
        conf_db = read_configfile(config_file=_file_dbconf)
        print("Reading configuration files was successful.")
        return conf_filetypes, conf_unitmapper, conf_dirs, conf_db

    def _test_connection_to_db(self):
        """Connect to database"""
        client = get_client(self.conf_db)
        client.ping()
        client.close()
        print("Connection to database works.")

    @staticmethod
    def _convert_datestr_to_iso8601(datestr: str, timezone_offset_to_utc_hours: int) -> str:
        """Convert date string to ISO 8601 format

        Needed for InfluxDB query.

        InfluxDB stores data in UTC (same as GMT). We want to be able to specify a start/stop
        time range in relation to the timezone we want to have the data in. For example, if
        we want to download data in CET, then we want to specify the range also in CET.

        This method converts the requested timerange to the needed timezone.

        :param datestr: in format '2022-05-27 00:00:00'
        :param timezone_offset_to_utc_hours: relative to UTC, e.g. 1 for CET (winter time)
        :return:
            e.g. with 'timezone_offset_to_utc_hours=1' the datestr'2022-05-27 00:00:00'
                is converted to '2022-05-27T00:00:00+01:00', which corresponds to CET
                (Central European Time, winter time, without daylight savings)
        """
        _datetime = parser.parse(datestr)
        _isostr = _datetime.isoformat()
        # Needs to be in format '2022-05-27T00:00:00Z' for InfluxDB:
        sign = '+' if timezone_offset_to_utc_hours >= 0 else '-'
        timezone_offset_to_utc_hours = str(timezone_offset_to_utc_hours).zfill(2) \
            if timezone_offset_to_utc_hours < 10 \
            else timezone_offset_to_utc_hours
        isostr_influx = f"{_isostr}{sign}{timezone_offset_to_utc_hours}:00"
        # isostr_influx = f"{_isostr}Z"  # Needs to be in format '2022-05-27T00:00:00Z' for InfluxDB
        return isostr_influx

    @staticmethod
    def _read_configfile(config_file) -> dict:
        """
        Load configuration from YAML file

        kudos: https://stackoverflow.com/questions/57687058/yaml-safe-load-special-character-from-file

        :param config_file: YAML file with configuration
        :return: dict
        """
        with open(config_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            # data = yaml.load(f, Loader=SafeLoader)
        return data

    def _detect_measurement_for_field(self, bucket: str, measurementslist: list, varnameslist: list) -> dict:
        """Detect measurement group of variable

        Helper function because the query in FluxQL (InfluxDB query language) does not return
        the measurement group of the field. Used e.g. in diive meteoscreening, where info
        about the measurement group is important.

        :param bucket: name of database bucket, e.g. "ch-dav_raw"
        :param measurementslist: list of measurements, e.g. "['TA', 'SW', 'LW']"
        :param varnameslist: list of variable names, e.g. "[TA_T1_35_1, SW_IN_T1_35_1]"
        :return:
        """
        assigned_measurements = {}
        for m in measurementslist:
            fieldslist = self.show_fields_in_measurement(bucket=bucket, measurement=m)
            for var in varnameslist:
                if var in fieldslist:
                    assigned_measurements[var] = m
        return assigned_measurements


def get_conf_filetypes(folder: Path, ext: str = 'yaml') -> dict:
    """Search config files with file extension *ext* in folder *dir*"""
    folder = str(folder)  # Required as string for os.walk
    conf_filetypes = {}
    for root, dirs, files in os.walk(folder):
        for f in files:
            if fnmatch.fnmatch(f, f'*.{ext}'):
                _filepath = Path(root) / f
                _dict = read_configfile(config_file=_filepath)
                _key = list(_dict.keys())[0]
                _vals = _dict[_key]
                conf_filetypes[_key] = _vals
    return conf_filetypes


def read_configfile(config_file) -> dict:
    """
    Load configuration from YAML file

    kudos: https://stackoverflow.com/questions/57687058/yaml-safe-load-special-character-from-file

    :param config_file: YAML file with configuration
    :return: dict
    """
    with open(config_file, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
        # data = yaml.load(f, Loader=SafeLoader)
    return data

# def show_settings(self):
#     print("Currently selected:")
#     print(f"    Bucket: {self.bucket}")
#     print(f"    Measurements: {self.measurements}")
#     print(f"    Fields: {self.fields}")

# @property
# def bucket(self):
#     """Getter function for database bucket"""
#     if not isinstance(self._bucket, str):
#         raise Exception('No bucket selected.')
#     return self._bucket
#
# @bucket.setter
# def bucket(self, bucket: str):
#     """Setter function for database bucket"""
#     self._bucket = bucket
#
# @property
# def measurements(self):
#     """Get selected database measurements"""
#     if not isinstance(self._measurements, list):
#         raise Exception('No measurements selected.')
#     return self._measurements
#
# @measurements.setter
# def measurements(self, measurements: str):
#     """Setter function for database measurements"""
#     self._measurements = measurements
#
# @property
# def fields(self):
#     """Get selected database fields"""
#     if not isinstance(self._fields, list):
#         raise Exception('No fields selected.')
#     return self._fields
#
# @fields.setter
# def fields(self, fields: str):
#     """Setter function for database fields"""
#     self._fields = fields

# def _assemble_fluxql_querystring(self,
#                                  start: str,
#                                  stop: str,
#                                  measurements: list,
#                                  vars: list) -> str:
#     """Assemble query string for flux query language
#
#     Note that the `stop` date is exclusive (not returned).
#     """
#     _bucketstring = self._fluxql_bucketstring(bucket=self.bucket)
#     _rangestring = self._fluxql_rangestring(start=start, stop=stop)
#     _filterstring_m = self._fluxql_filterstring(queryfor='_measurement', querylist=measurements)
#     _filterstring_v = self._fluxql_filterstring(queryfor='_field', querylist=vars)
#     _keepstring = f'|> keep(columns: ["_time", "_field", "_value", "units"])'
#     querystring = f"{_bucketstring} {_rangestring} {_filterstring_m} {_filterstring_v} " \
#                   f"{_keepstring}"
#     # _pivotstring = f'|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
#     # querystring = f"{_bucketstring} {_rangestring} {_filterstring_m} {_filterstring_v} " \
#     #               f"{_keepstring} {_pivotstring}"
#     return querystring

# def _check_if_same_units_freq(self, results, field):
#     found_units = results['units'].unique().tolist()
#     found_freq = results['freq'].unique().tolist()
#     if len(found_units) == 1 and len(found_freq) == 1:
#         return found_units[0], found_freq[0]
#     else:
#         raise Exception(f"More than one type of units and/or frequencies found for {field}:"
#                         f" units: {found_units}\n"
#                         f" freqencies: {found_freq}")

# def get_var_data(self,
#                  start: str,
#                  stop: str) -> DataFrame:
#     """Get data from database between 'start' and 'stop' dates
#
#     The 'stop' date is not included.
#     """
#
#     # InfluxDB needs ISO 8601 date format for query
#     start_iso = self._convert_datestr_to_iso8601(datestr=start)
#     stop_iso = self._convert_datestr_to_iso8601(datestr=stop)
#
#     querystring = self._assemble_fluxql_querystring(start=start_iso,
#                                                     stop=stop_iso,
#                                                     measurements=self.measurements,
#                                                     vars=self.fields)
#     results = self.query_client.query_data_frame(query=querystring)
#     results.drop(columns=['result', 'table'], inplace=True)
#     results['_time'] = results['_time'].dt.tz_localize(None)  # Remove timezone info, irrelevant
#     results.rename(columns={"_time": "TIMESTAMP_END"}, inplace=True)
#     # results.set_index("_time", inplace=True)
#     df = pd.pivot(results, index='TIMESTAMP_END', columns='_field', values='_value')
#     return results
