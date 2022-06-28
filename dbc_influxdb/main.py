# https://www.geeksforgeeks.org/getter-and-setter-in-python/
from pathlib import Path

import dateutil.parser as parser
import yaml
from influxdb_client import WriteOptions
from pandas import DataFrame

import dbc_influxdb.fluxql as fluxql
from dbc_influxdb.common import tags
from dbc_influxdb.db import get_client, get_query_api
from dbc_influxdb.filetypereader import FileTypeReader, get_conf_filetypes, read_configfile
from dbc_influxdb.varscanner import VarScanner


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
                         to_measurement: str):

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
                            data_frame_tag_columns=tags)

            print("Upload finished.")

    def upload_filetype(self,
                        file_df: DataFrame,
                        data_version: str,
                        fileinfo: dict,
                        to_bucket: str,
                        filetypeconf: dict,
                        parse_var_pos_indices: bool = True,
                        logger=None) -> DataFrame:
        """Upload data from file

        Primary method to automatically upload data from files to
        the database with the 'dataflow' package.

        """

        varscanner = VarScanner(file_df=file_df,
                                data_version=data_version,
                                fileinfo=fileinfo,
                                filetypeconf=filetypeconf,
                                conf_unitmapper=self.conf_unitmapper,
                                to_bucket=to_bucket,
                                conf_db=self.conf_db,
                                parse_var_pos_indices=parse_var_pos_indices,
                                logger=logger)
        varscanner.run()
        return varscanner.get_results()

    # def query(self, func, *args, **kwargs):
    #     client = get_client(self.conf_db)
    #     query_api = get_query_api(client)
    #     func(query_api=query_api, *args, **kwargs)
    #     client.close()

    def download(self,
                 bucket: str,
                 measurement: list,
                 field: list,
                 start: str,
                 stop: str,
                 data_version: str = None) -> tuple[DataFrame, dict]:
        """Get data from database between 'start' and 'stop' dates

        The 'stop' date is not included.
        """

        # TODO data version required!

        # InfluxDB needs ISO 8601 date format for query
        start_iso = self._convert_datestr_to_iso8601(datestr=start)
        stop_iso = self._convert_datestr_to_iso8601(datestr=stop)

        # Assemble query
        bucketstring = fluxql.bucketstring(bucket=bucket)
        rangestring = fluxql.rangestring(start=start_iso, stop=stop_iso)
        measurementstring = fluxql.filterstring(queryfor='_measurement', querylist=measurement, type='or')
        fieldstring = fluxql.filterstring(queryfor='_field', querylist=field, type='or')
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
            table['_time'] = table['_time'].dt.tz_localize(None)  # Remove timezone info, irrelevant
            table.rename(columns={"_time": "TIMESTAMP_END"}, inplace=True)
            table.set_index("TIMESTAMP_END", inplace=True)
            table.sort_index(inplace=True)

            # Detect of which variable the frame contains data
            field_in_table = [f for f in field if f in table.columns]

            # Store frame in dict with the field (variable name) as key
            # This way the table (data) of each variable can be accessed by
            # field name, i.e., variable name.
            data_detailed[field_in_table[0]] = table

            # Collect variables without tags in a separate (simplified) dataframe.
            # This dataframe only contains the timestamp and the data of each var.
            if ix == 0:
                data_simple = table[[field_in_table[0]]].copy()
            else:
                data_simple[field_in_table[0]] = table[[field_in_table[0]]].copy()

        # Info
        print(f"Downloaded data for {len(data_detailed)} variables:")
        for key, val in data_detailed.items():
            num_records = len(data_detailed[key])
            first_date = data_detailed[key].index[0]
            last_date = data_detailed[key].index[-1]
            print(f"    {key}   ({num_records} records)     "
                  f"first date: {first_date}    last date: {last_date}")

        return data_simple, data_detailed

    def readfile(self, filepath: str, filetype: str, nrows=None, logger=None):
        # Read data of current file
        logtxt = f"[{self.script_id}] Reading file {filepath} ..."
        logger.info(logtxt) if logger else print(logtxt)
        filetypeconf = self.conf_filetypes[filetype]
        file_df, fileinfo = FileTypeReader(filepath=filepath,
                                           filetype=filetype,
                                           filetypeconf=filetypeconf,
                                           nrows=nrows).get_data()
        return file_df, filetypeconf, fileinfo

    def _read_configs(self):

        # # Search in this file's folder
        # _dir_main = Path(__file__).parent.resolve()

        # Config locations
        _dir_filegroups = self.dirconf / 'filegroups'
        _file_unitmapper = self.dirconf / 'units.yaml'
        _file_dirs = self.dirconf / 'dirs.yaml'
        _file_dbconf = Path(f"{self.dirconf}_secret") / 'dbconf.yaml'

        # Read configs
        conf_filetypes = get_conf_filetypes(dir=_dir_filegroups)
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

    def _convert_datestr_to_iso8601(self, datestr: str) -> str:
        """Convert date string to ISO 8601 format

        Needed for InfluxDB query.
        Example:
            - '2022-05-27 00:00:00' is converted to '2022-05-27T00:00:00Z'
        """
        _datetime = parser.parse(datestr)
        _isostr = _datetime.isoformat()
        isostr_influx = f"{_isostr}Z"  # Needs to be in format '2022-05-27T00:00:00Z' for InfluxDB
        return isostr_influx

    def show_configs_unitmapper(self) -> dict:
        return self.conf_unitmapper

    def show_configs_dirs(self) -> dict:
        return self.conf_dirs

    def show_configs_filetypes(self) -> dict:
        return self.conf_filetypes

    def show_config_for_filetype(self, filetype: str) -> dict:
        return self.conf_filetypes[filetype]

    def show_fields_in_measurement(self, bucket: str, measurement: str) -> list:
        """Show fields (variable names) in measurement"""
        query = fluxql.fields_in_measurement(bucket=bucket, measurement=measurement)
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

    def _read_configfile(self, config_file) -> dict:
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

def test():
    # # Testing MeteoScreeningFromDatabase
    # # Example file from dbget output
    # import pandas as pd
    # testfile = r'L:\Dropbox\luhk_work\20 - CODING\26 - NOTEBOOKS\meteoscreening\test_qc.csv'
    # var_df = pd.read_csv(testfile)
    # var_df.set_index('TIMESTAMP_END', inplace=True)
    # var_df.index = pd.to_datetime(var_df.index)
    # # testdata.plot()

    # # UPLOAD SPECIFIC FILE
    #
    # to_bucket = 'ch-oe2_processing'
    # filepath = r'L:\Dropbox\luhk_work\40 - DATA\FLUXNET-WW2020_RELEASE-2022-1\FLX_CH-Oe2_FLUXNET2015_FULLSET_2004-2020_beta-3\FLX_CH-Oe2_FLUXNET2015_FULLSET_HH_2004-2020_beta-3.csv'
    #
    # # to_bucket = 'test'
    # data_version = 'FLUXNET-WW2020_RELEASE-2022-1'
    # dirconf = r'L:\Dropbox\luhk_work\20 - CODING\22 - POET\configs'
    # filetype = 'PROC-FLUXNET-FULLSET-HH-CSV-30MIN'
    # dbc = dbcInflux(dirconf=dirconf)
    # df, filetypeconf, fileinfo = dbc_influxdb.readfile(filepath=filepath,
    #                                           filetype=filetype,
    #                                           nrows=None)
    # varscanner_df = dbc_influxdb.upload(to_bucket=to_bucket,
    #                            file_df=df,
    #                            filetypeconf=filetypeconf,
    #                            fileinfo=fileinfo,
    #                            data_version=data_version,
    #                            parse_var_pos_indices=False)
    # print(varscanner_df)

    dirconf = r'F:\Dropbox\luhk_work\20 - CODING\22 - POET\configs'
    dbc = dbcInflux(dirconf=dirconf)
    dbc.show_buckets()
    # dbc.show_measurements_in_bucket(bucket='ch-aws_raw')
    # dbc.show_fields_in_bucket(bucket='ch-aws_raw')
    # dbc.show_fields_in_measurement(bucket='ch-lae_raw', measurement='SW')
    # # dbc.show_configs_filetypes()

    data_simple, data_detailed = dbc.download(bucket='ch-aws_raw',
                                              measurement=['TA'],
                                              field=['TA_M1B1_1.50_1'],
                                              start='2022-05-01 00:01:00',
                                              stop='2022-06-01 00:01:00')
    print(data_detailed)

    # # Download
    # tables_dict, \
    # fields_df = \
    #     dbc.download(bucket='ch-lae_processing',
    #                  measurement=['TA', 'SW', 'VPD'],
    #                  field=['TA_F', 'SW_IN_F', 'VPD_F'],
    #                  start='2020-06-10 12:00:00',
    #                  stop='2020-06-20 13:00:00',
    #                  data_version='FLUXNET-WW2020_RELEASE-2022-1')
    # # print(fields_df)

    # dbc.upload_singlevar(var_df=var_df, to_bucket='test', to_measurement='TA')


if __name__ == '__main__':
    test()
