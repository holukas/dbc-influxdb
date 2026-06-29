from pathlib import Path

import dateutil.parser as parser
import pandas as pd
from influxdb_client import WriteOptions
from pandas import DataFrame

import dbc_influxdb.fluxql as fluxql
from dbc_influxdb.common import tags, convert_ts_to_timezone
from dbc_influxdb.config import get_conf_filetypes, read_configfile
from dbc_influxdb.db import get_client, get_query_api, get_delete_api
from dbc_influxdb.logger import log, setup_logging


class dbcInflux:

    def __init__(self,
                 dirconf: str):

        setup_logging()

        self.dirconf = Path(dirconf)

        self.conf_filetypes, \
            self.conf_unitmapper, \
            self.conf_dirs, \
            self.conf_db = self._read_configs()

        self.test_connection()

    @staticmethod
    def _format_utc_offset(timezone_offset_to_utc_hours: int | float) -> str:
        """Format an hour offset as an ISO 8601 UTC offset string.

        Supports negative and fractional offsets; fractional hours are rounded
        to the nearest minute. e.g. ``1`` -> ``'+01:00'``, ``-5`` -> ``'-05:00'``,
        ``10`` -> ``'+10:00'``, ``5.5`` -> ``'+05:30'``.
        """
        total_minutes = round(timezone_offset_to_utc_hours * 60)
        sign = '+' if total_minutes >= 0 else '-'
        hours, minutes = divmod(abs(total_minutes), 60)
        return f"{sign}{hours:02d}:{minutes:02d}"

    def _add_timestamp_utc(self, timestamp_index, timezone_offset_to_utc_hours) -> pd.DatetimeIndex:
        # Needs to be in format '2022-05-27 00:00:00+01:00' for InfluxDB
        utc_str = self._format_utc_offset(timezone_offset_to_utc_hours)
        timestamp_index_utc = timestamp_index.tz_localize(utc_str)
        return timestamp_index_utc

    def upload_singlevar(self,
                         var_df: DataFrame,
                         to_bucket: str,
                         to_measurement: str,
                         timezone_offset_to_utc_hours: int | float,
                         delete_from_db_before_upload: bool = True):
        """Upload single variable to database.

        The database needs to know the timezone because all data in the db are
        stored in UTC/GMT.

        Args:
            var_df: contains measured variable data and tags (data_detailed)
            to_bucket: name of database bucket
            to_measurement: name of measurement, e.g. 'TA'
            timezone_offset_to_utc_hours: e.g. 1, see docstring in `._add_timestamp_utc' for more details
            delete_from_db_before_upload: data between the start and end dates of *var_df* are
                deleted before uploading. All data with the same variable name are deleted.
                Implemented to avoid duplicate uploads of the same data in cases where data
                remained the same, but one of the tags has changed.

        Note:
            *var_df* must contain one data column (the variable) plus a column
            for every tag listed in :data:`dbc_influxdb.common.tags` (e.g.
            ``site``, ``units``, ``gain``, ``offset``, ...).

        Returns:
            Nothing, only uploads to database.

        """
        # Work on a copy so the caller's DataFrame is not mutated (the index is
        # localized to UTC below).
        var_df = var_df.copy()

        data_cols = var_df.columns.to_list()

        # Check if data contain all tag columns
        cols_not_in_data = [l for l in tags if l not in data_cols]
        if len(cols_not_in_data) > 0:
            raise ValueError(f"Data do not contain required tag columns: {cols_not_in_data}")

        # Detect field name (variable name)
        # The field name is the name of the column that is not part of the tags
        field = [l for l in data_cols if l not in tags]
        if len(field) > 1:
            raise ValueError(f"Only one field (variable name) allowed, found {field}.")

        if delete_from_db_before_upload:
            start = str(var_df.index[0])
            stop = str(var_df.index[-1])
            data_version = list(set(var_df['data_version'].tolist()))
            if len(data_version) > 1:
                raise ValueError('Multiple data versions not supported')
            data_version = data_version[0]
            self.delete(bucket=to_bucket, measurements=[to_measurement],
                        start=start, stop=stop,
                        timezone_offset_to_utc_hours=timezone_offset_to_utc_hours,
                        data_version=data_version, fields=field)

        # Add timezone info to timestamp
        var_df.index = self._add_timestamp_utc(timestamp_index=var_df.index,
                                               timezone_offset_to_utc_hours=timezone_offset_to_utc_hours)

        # Database clients
        log.info("Connecting to database ...")

        # The WriteApi in batching mode (default mode) is suppose to run as a singleton.
        # To flush all your data you should wrap the execution using with
        # client.write_api(...) as write_api: statement or call write_api.close()
        # at the end of your script.
        # https://influxdb-client.readthedocs.io/en/stable/usage.html#write
        with get_client(conf_db=self.conf_db) as client, \
                client.write_api(write_options=WriteOptions(batch_size=5000,
                                                            flush_interval=10_000,
                                                            jitter_interval=2_000,
                                                            retry_interval=5_000,
                                                            max_retries=5,
                                                            max_retry_delay=30_000,
                                                            exponential_base=2)) as write_api:

            # Write to db
            log.info(f"--> UPLOAD TO DATABASE BUCKET {to_bucket}:  {field}")

            write_api.write(to_bucket,
                            record=var_df,
                            data_frame_measurement_name=to_measurement,
                            data_frame_tag_columns=tags,
                            write_precision='s')

            log.info("Upload finished.")

    def download(self,
                 bucket: str,
                 start: str,
                 stop: str,
                 timezone_offset_to_utc_hours: int | float,  # v0.3.0
                 data_version: list = None,
                 measurements: list = None,
                 fields: list = None,
                 verify_freq: str | bool = False) -> tuple[DataFrame, dict, dict]:
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
                e.g. ['meteoscreening']. If given as a string it is converted to a list
                with the string as the list element.
            verify_freq: checks if the downloaded data has the expected frequency, given
                as str in the format of pandas frequency strings, e.g., '30min' for 30-minute
                data. If the inferred frequency does not match, a warning is logged.

        """

        if isinstance(data_version, str):
            data_version = [data_version]

        fields_str = fields if fields else "ALL"
        measurements_str = measurements if measurements else "ALL"
        log.info(f"DOWNLOADING\n"
                 f"    from bucket {bucket}\n"
                 f"    variables {fields_str}\n"
                 f"    from measurements {measurements_str}\n"
                 f"    from data version {data_version}\n"
                 f"    between {start} and {stop}\n"
                 f"    with timezone offset to UTC of {timezone_offset_to_utc_hours}")

        # InfluxDB needs ISO 8601 date format (in requested timezone) for query
        start_iso = self._convert_datestr_to_iso8601(datestr=start,
                                                     timezone_offset_to_utc_hours=timezone_offset_to_utc_hours)
        stop_iso = self._convert_datestr_to_iso8601(datestr=stop,
                                                    timezone_offset_to_utc_hours=timezone_offset_to_utc_hours)

        # Assemble query
        bucketstring = fluxql.bucketstring(bucket=bucket)
        rangestring = fluxql.rangestring(start=start_iso, stop=stop_iso)

        # Measurements
        if measurements:
            measurementstring = fluxql.filterstring(queryfor='_measurement', querylist=measurements, logic='or')
        else:
            measurementstring = ''  # Empty means all measurements

        # Fields
        if fields:
            fieldstring = fluxql.filterstring(queryfor='_field', querylist=fields, logic='or')
        else:
            fieldstring = ''  # Empty means all fields

        pivotstring = fluxql.pivotstring()

        dataversionstring = ''
        if data_version:
            dataversionstring = fluxql.filterstring(queryfor='data_version', querylist=data_version, logic='or')
            querystring = f"{bucketstring} {rangestring} {measurementstring} " \
                          f"{dataversionstring} {fieldstring} {pivotstring}"
        else:
            querystring = f"{bucketstring} {rangestring} {measurementstring} " \
                          f"{fieldstring} {pivotstring}"

        # Run database query
        tables = self._query_df(querystring)  # DataFrame or list of DataFrames
        log.info("Download finished.")
        log.debug(f"Used querystring: {querystring}")
        log.debug("querystring was constructed from:\n"
                  f"    bucketstring: {bucketstring}\n"
                  f"    rangestring: {rangestring}\n"
                  f"    measurementstring: {measurementstring}\n"
                  f"    dataversionstring: {dataversionstring}\n"
                  f"    fieldstring: {fieldstring}\n"
                  f"    pivotstring: {pivotstring}")

        # In case only one single variable is downloaded, the query returns
        # a single dataframe. If multiple variables are downloaded, the query
        # returns a list of dataframes. To keep these two options consistent,
        # single dataframes are converted to a list, in which case the list
        # contains only one element: the dataframe of the single variable.
        tables = [tables] if not isinstance(tables, list) else tables

        # No data in the requested range: query_data_frame returns an empty
        # DataFrame (without the expected columns). Return empty results in that
        # case instead of raising when accessing '_measurement' below.
        tables = [t for t in tables if not t.empty and '_measurement' in t.columns]
        if not tables:
            log.info("No data found for the requested query.")
            return DataFrame(), {}, {}

        # Each table in tables contains data for one variable
        found_measurements = []
        data_detailed = {}  # Stores variables and their tags
        data_simple = DataFrame()  # Stores variables
        for ix, table in enumerate(tables):

            found_measurement = list(set(table['_measurement'].tolist()))
            if len(found_measurement) != 1:
                raise ValueError(f"Found {len(found_measurement)} measurements, but only one allowed")
            found_measurements.append(found_measurement[0])

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

            # Detect of which variable the frame contains data
            # Here it is useful that the variable name is also available as tag 'varname'.
            list_of_fields = list(set(table['varname'].tolist()))

            # Current table must contain one single variable name
            if len(list_of_fields) != 1:
                raise ValueError(f"Expected one field, got {list_of_fields}")

            field_in_table = list_of_fields[0]
            key = field_in_table

            # Keep all columns that are either the field or database tags
            keepcols = [col for col in table.columns if col in tags]
            keepcols.append(key)
            table = table[keepcols].copy()

            # Collect variables without tags in a separate (simplified) dataframe.
            # This dataframe only contains the timestamp and the data column of each var.
            # :: refactored in v0.7.0
            # Add new column if column does not exist in current df
            incomingdata = pd.DataFrame(table[key])
            data_simple = data_simple.combine_first(incomingdata)
            data_simple = data_simple[~data_simple.index.duplicated(keep='last')]

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
        log.info(f"Downloaded data for {len(data_detailed)} variables:")
        for key, val in data_detailed.items():
            num_records = len(data_detailed[key])
            first_date = data_detailed[key].index[0]
            last_date = data_detailed[key].index[-1]
            log.info(f"<-- {key}  "
                     f"({num_records} records)  "
                     f"first date: {first_date}  "
                     f"last date: {last_date}")

        if not measurements:
            found_measurements = list(set(found_measurements))
            measurements = found_measurements

        assigned_measurements = self._detect_measurement_for_field(bucket=bucket,
                                                                   measurementslist=measurements,
                                                                   varnameslist=list(data_detailed.keys()))

        if verify_freq and not data_simple.empty:
            self._verify_freq(data_index=data_simple.index, expected_freq=verify_freq)

        return data_simple, data_detailed, assigned_measurements

    @staticmethod
    def _verify_freq(data_index: pd.DatetimeIndex, expected_freq: str) -> None:
        """Warn if the inferred frequency of the data does not match *expected_freq*."""
        inferred_freq = pd.infer_freq(data_index)
        if inferred_freq is None:
            log.warning("Could not infer a frequency from the downloaded data "
                        f"(expected '{expected_freq}').")
        elif inferred_freq != expected_freq:
            log.warning(f"Inferred data frequency '{inferred_freq}' does not match "
                        f"expected frequency '{expected_freq}'.")
        else:
            log.info(f"Verified data frequency: '{inferred_freq}'.")

    def delete(self,
               bucket: str,
               measurements: list | bool,
               start: str,
               stop: str,
               timezone_offset_to_utc_hours: int | float,  # v0.3.0
               data_version: str,
               fields: list | bool) -> None:
        """
        Delete data from bucket

        Args:
            bucket: name of bucket in database
            measurements: list or True
                If list, list of measurements in database, e.g. ['TA', 'SW']
                If True, all *fields* in all *measurements* will be deleted
            fields: list or True
                If list, list of fields (variable names) to delete
                If True, all data in *fields* in *measurements* will be deleted.
            start: start datetime, e.g. '2022-07-04 00:30:00'
            stop: stop datetime, e.g. '2022-07-05 12:00:00'
            timezone_offset_to_utc_hours: the timezone of *start* and *stop* datetimes.
                Necessary because the database always stores data with UTC timestamps.
                For example, if data were originally recorded using CET (winter time),
                which corresponds to UTC+01:00, and all data between 1 Jun 2024 00:30 CET and
                2 Jun 2024 12:00 CET should be deleted, then *timezone_offset_to_utc_hours=1*.
            data_version: version ID of the data that should be deleted,
                e.g. 'meteoscreening_diive', 'raw', 'myID', ...

        Examples:

            Delete all variables across all measurements:
                measurements=True, fields=True

            Delete all variables of a specific measurement:
                measurements=['TA'], fields=True


            Delete specific variables in specific measurements:
                measurements=['TA', 'SW'], fields=['TA_T1_1_1', 'SW_T1_1_1']

            Delete specific variables in across all measurements:
                measurements=True, fields=['TA_T1_1_1', 'SW_T1_1_1']
                This basically searches the variables across all measurements
                and the deletes them.

        Returns:
            -

        docs:
        - https://influxdb-client.readthedocs.io/en/stable/usage.html#delete-data
        - https://docs.influxdata.com/influxdb/v2/reference/syntax/delete-predicate/

        """

        # Validate destructive inputs up front. Anything falsy-but-not-True
        # (None, False, empty list) is rejected so we never silently delete
        # nothing while logging a successful deletion, and never iterate over a
        # non-iterable bool.
        if not measurements:
            raise ValueError("`measurements` must be a non-empty list of measurement "
                             "names or True (all measurements).")
        if not fields:
            raise ValueError("`fields` must be a non-empty list of field names or "
                             "True (all fields).")

        # InfluxDB needs ISO 8601 date format (in requested timezone) for query
        start_iso = self._convert_datestr_to_iso8601(datestr=start,
                                                     timezone_offset_to_utc_hours=timezone_offset_to_utc_hours)
        stop_iso = self._convert_datestr_to_iso8601(datestr=stop,
                                                    timezone_offset_to_utc_hours=timezone_offset_to_utc_hours)

        # Check if measurements is boolean and True
        measurements_all = False
        if measurements and isinstance(measurements, bool):
            measurements = self.show_measurements_in_bucket(bucket=bucket, verbose=False)
            measurements_all = True

        # Run database query
        with get_client(self.conf_db) as client:
            delete_api = get_delete_api(client)

            # Delete
            kwargs = dict(start=start_iso, stop=stop_iso, bucket=bucket)
            for measurement in measurements:

                # Delete all variables (fields) in measurement
                if fields and isinstance(fields, bool):
                    predicate_str = (f'_measurement="{measurement}" '
                                     f'AND data_version="{data_version}"')
                    delete_api.delete(predicate=predicate_str, **kwargs)

                # Delete given variables (fields) in measurement
                elif isinstance(fields, list):
                    for field in fields:
                        predicate_str = (f'_measurement="{measurement}" '
                                         f'AND varname="{field}" '
                                         f'AND data_version="{data_version}"')
                        delete_api.delete(predicate=predicate_str, **kwargs)

        if measurements_all:
            measurements_str = "ALL"
        elif isinstance(measurements, list):
            measurements_str = measurements
        else:
            measurements_str = None

        if fields and isinstance(fields, bool):
            fields_str = "ALL"
        elif isinstance(fields, list):
            fields_str = fields
        else:
            fields_str = None

        log.info(f"Deleted variables {fields_str} between {start_iso} and {stop_iso} "
                 f"from measurements {measurements_str} in bucket {bucket}.")

        return None

    def show_configs_unitmapper(self) -> dict:
        return self.conf_unitmapper

    def show_configs_dirs(self) -> dict:
        return self.conf_dirs

    def show_configs_filetypes(self) -> dict:
        return self.conf_filetypes

    def show_config_for_filetype(self, filetype: str) -> dict:
        return self.conf_filetypes[filetype]

    def show_fields_in_measurement(self, bucket: str, measurement: str, days: int = 9999,
                                   data_version: str = None, verbose: bool = True) -> list:
        """Show fields (variable names) in measurement, optionally narrowed to one
        *data_version*."""
        if data_version:
            query = fluxql.fields_for_version(bucket=bucket, measurement=measurement,
                                              data_version=data_version, days=days)
            fieldslist = self._tag_value_list(self._query_df(query))
        else:
            query = fluxql.fields_in_measurement(bucket=bucket, measurement=measurement, days=days)
            fieldslist = self._query_df(query)['_value'].tolist()
        if verbose:
            log.info(f"{'=' * 40}\nFields in measurement {measurement} of bucket {bucket}:")
            for ix, f in enumerate(fieldslist, 1):
                log.info(f"#{ix}  {bucket}  {measurement}  {f}")
            log.info(f"Found {len(fieldslist)} fields in measurement {measurement} "
                     f"of bucket {bucket}.\n{'=' * 40}")
        return fieldslist

    def show_fields_in_bucket(self, bucket: str, measurement: str = None, verbose: bool = True) -> list:
        """Show fields (variable names) in bucket (optional: for specific measurement)"""
        if measurement is not None:
            return self.show_fields_in_measurement(bucket=bucket, measurement=measurement, verbose=verbose)
        query = fluxql.fields_in_bucket(bucket=bucket)
        results = self._query_df(query)
        fieldslist = results['_value'].tolist()
        if verbose:
            log.info(f"{'=' * 40}\nFields in bucket {bucket}:")
            for ix, f in enumerate(fieldslist, 1):
                log.info(f"#{ix}  {bucket}  {f}")
            log.info(f"Found {len(fieldslist)} variables (fields) in bucket {bucket}.\n{'=' * 40}")
        return fieldslist

    def show_measurements_in_bucket(self, bucket: str, data_version: str = None,
                                    verbose: bool = True) -> list:
        """Show measurements in bucket, optionally narrowed to one *data_version*."""
        if data_version:
            query = fluxql.measurements_for_version(bucket=bucket, data_version=data_version)
            measurements = self._tag_value_list(self._query_df(query))
        else:
            query = fluxql.measurements_in_bucket(bucket=bucket)
            measurements = self._query_df(query)['_value'].tolist()
        if verbose:
            log.info(f"{'=' * 40}\nMeasurements in bucket {bucket}:")
            for ix, m in enumerate(measurements, 1):
                log.info(f"#{ix}  {bucket}  {m}")
            log.info(f"Found {len(measurements)} measurements in bucket {bucket}.\n{'=' * 40}")
        return measurements

    def show_buckets(self) -> list:
        """Show all buckets in the database"""
        query = fluxql.buckets()
        results = self._query_df(query)
        results.drop(columns=['result', 'table'], inplace=True)
        bucketlist = results['name'].tolist()
        bucketlist = [x for x in bucketlist if not x.startswith('_')]
        for ix, b in enumerate(bucketlist, 1):
            log.info(f"#{ix}  {b}")
        log.info(f"Found {len(bucketlist)} buckets in database.")
        return bucketlist

    @staticmethod
    def _tag_value_list(results) -> list:
        """Extract the distinct values from a ``schema.tagValues`` result frame.

        Returns an empty list when the query found nothing (an empty frame has
        no ``_value`` column).
        """
        if hasattr(results, "columns") and "_value" in results.columns:
            return results["_value"].tolist()
        return []

    def show_data_versions_in_bucket(self, bucket: str, days: int = 9999,
                                     verbose: bool = True) -> list:
        """Show distinct data versions (``data_version`` tag) in *bucket*."""
        query = fluxql.data_versions_in_bucket(bucket=bucket, days=days)
        versions = self._tag_value_list(self._query_df(query))
        if verbose:
            log.info(f"{'=' * 40}\nData versions in bucket {bucket}:")
            for ix, v in enumerate(versions, 1):
                log.info(f"#{ix}  {bucket}  {v}")
            log.info(f"Found {len(versions)} data versions in bucket {bucket}.\n{'=' * 40}")
        return versions

    def show_units_in_field(self, bucket: str, measurement: str, field: str,
                            data_version: str = None, days: int = 9999,
                            verbose: bool = True) -> list:
        """Show distinct units (``units`` tag) for *field* of *measurement*,
        optionally narrowed to one *data_version*."""
        query = fluxql.units_in_field(bucket=bucket, measurement=measurement,
                                      field=field, data_version=data_version, days=days)
        units = self._tag_value_list(self._query_df(query))
        if verbose:
            log.info(f"Units for {bucket} / {measurement} / {field}: {units}")
        return units

    def _read_configs(self):
        """Read all YAML configuration files from *dirconf* and its secret sibling.

        See :mod:`dbc_influxdb.config` for the expected directory layout. The
        database connection config is read from the sibling directory
        ``<dirconf>_secret/dbconf.yaml`` so secrets can be kept out of the
        (often version-controlled) main config directory.
        """
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
        log.info("Reading configuration files was successful.")
        return conf_filetypes, conf_unitmapper, conf_dirs, conf_db

    def _query_df(self, query: str):
        """Run a Flux query and return the result as a DataFrame (or list of them).

        Opens a client for the duration of the query and closes it afterwards,
        even if the query raises.
        """
        with get_client(self.conf_db) as client:
            query_api = get_query_api(client)
            return query_api.query_data_frame(query=query)

    def test_connection(self):
        """Verify the database is reachable by pinging it.

        Raises ``ConnectionError`` if the ping fails (bad url/org/token in
        ``dbconf.yaml`` or an unreachable server).
        """
        with get_client(self.conf_db) as client:
            if not client.ping():
                raise ConnectionError(
                    "Could not connect to the InfluxDB database (ping failed). "
                    "Check the connection settings in dbconf.yaml (url, org, token).")
        log.info("Connection to database works.")

    #: Backwards-compatible alias for the previously private method name.
    _test_connection_to_db = test_connection

    @staticmethod
    def _convert_datestr_to_iso8601(datestr: str, timezone_offset_to_utc_hours: int | float) -> str:
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
        isostr_influx = f"{_isostr}{dbcInflux._format_utc_offset(timezone_offset_to_utc_hours)}"
        return isostr_influx

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
            fieldslist = self.show_fields_in_measurement(bucket=bucket, measurement=m, verbose=False)
            for var in varnameslist:
                if var in fieldslist:
                    assigned_measurements[var] = m
        return assigned_measurements
