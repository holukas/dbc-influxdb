import _csv
import fnmatch
import os
from logging import Logger
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 30)


class FileTypeReader:
    """Read file and its variables according to a specified filetype"""

    def __init__(self,
                 filepath: str,
                 filetype: str,
                 filetypeconf: dict,
                 nrows=None,
                 logger: Logger = None):
        """

        :param filepath:
        :param filetype:
        :param filetypeconf:
        :param nrows:
        :param logger:

        """
        self.filepath = filepath
        self.filetype = filetype
        self.logger = logger

        self.data_df = pd.DataFrame()
        self.df_list = pd.DataFrame()
        self.missed_ids = None

        # Settings for .read_csv

        # Translate for use in .read_csv, e.g. false (in yaml files) to None
        self.nrows = nrows
        self.compression = 'gzip' if filetypeconf['filetype_gzip'] else None
        self.skiprows = None if not filetypeconf['data_skiprows'] else filetypeconf['data_skiprows']
        self.header = None if not filetypeconf['data_headerrows'] else filetypeconf['data_headerrows']

        # If no header (no vars and no units), get column names from filetype configuration instead
        self.names = [key for key in filetypeconf['data_vars'].keys()] if not self.header else None

        # pandas index_col accepts None, but also 0 which is interpreted by Python as False.
        # This causes problems when dealing with different files that use sometimes
        # None, sometimes 0. To be specific that index_col is not used, the value
        # -9999 is set in the yaml file.
        self.index_col = None if filetypeconf['data_index_col'] == -9999 else filetypeconf['data_index_col']
        self.date_format = filetypeconf['data_date_parser']
        # self.date_parser = self._get_date_parser(parser=filetypeconf['data_date_parser'])  # deprecated in pandas
        self.na_values = filetypeconf['data_na_values']
        self.delimiter = filetypeconf['data_delimiter']
        self.keep_date_col = filetypeconf['data_keep_date_col']

        # Format parse_dates arg for .read_csv()
        # Necessary if date is parsed from named columns
        if filetypeconf['data_parse_dates']:
            self.parse_dates = self._convert_timestamp_idx_col(var=filetypeconf['data_parse_dates'])
            parsed_index_col = ('TIMESTAMP', '-')
            # self.parse_dates = filetypeconf['data_parse_dates']
            self.parse_dates = {parsed_index_col: self.parse_dates}
        else:
            self.parse_dates = False

        self.build_timestamp = filetypeconf['data_build_timestamp']
        self.data_encoding = filetypeconf['data_encoding']

        # Special file format
        self.special_format = None
        special_formats = ['-ICOSSEQ-', '-ALTERNATING-']
        for sf in special_formats:
            if sf in self.filetype:
                self.special_format = sf
                break
            else:
                continue
            # self.special_format = sf if sf in self.filetype else False

        self.file_info = dict(
            filepath=self.filepath,
            filetype=self.filetype,
            special_format=self.special_format
        )

        self.goodrows_col = None if not filetypeconf['data_keep_good_rows'] \
            else filetypeconf['data_keep_good_rows'][0]  # Col used to identify rows w/ good data
        self.goodrows_ids = None if not filetypeconf['data_keep_good_rows'] \
            else filetypeconf['data_keep_good_rows'][1:]  # ID(s) used to identify rows w/ good data

        self.badrows_col = None if not filetypeconf['data_remove_bad_rows'] \
            else filetypeconf['data_remove_bad_rows'][0]  # Col used to identify rows w/ bad data
        self.badrows_ids = None if not filetypeconf['data_remove_bad_rows'] \
            else filetypeconf['data_remove_bad_rows'][1:]  # ID(s) used to identify rows w/ bad data

        # # Detect if file has special format that needs formatting
        # self.is_special_format = True if any(f in self.config_filetype for f in self.special_formats) else False

        # Config is also needed later
        self.filetypeconf = filetypeconf

        self.run()

    def get_data(self):
        return self.df_list, self.file_info, self.missed_ids

    def run(self):
        self.data_df = self._readfile()

        # In case the timestamp was built from multiple columns with 'parse_dates',
        # e.g. in EddyPro full output files from the 'date' and 'time' columns,
        # the parsed column has to be set as the timestamp index
        if isinstance(self.parse_dates, dict):
            try:
                self.data_df.set_index(('TIMESTAMP', '-'), inplace=True)
            except KeyError:
                pass

        # Convert data to float or string
        # Columns can be dtype string after this step, which can either be
        # desired (ICOSSEQ locations), or unwanted (in case of bad data rows)
        self._convert_to_float_or_string()

        # Convert special data structures
        # After this step, the dataframe(s) is/are stored in a list.
        # Reason is that the special format -ALTERNATING- returns
        # two different dataframes from one file, which are returned
        # in a list. Other special formats are therefore also returned
        # in a list, to be consistent.
        if self.special_format:
            self.df_list, self.missed_ids = self._special_data_formats()
        else:
            self.missed_ids = 'none, is not special format'

        # ** After this step, self.df_list instead of self.data_df is used for further processing **

        # Convert dataframe to list of dataframes if not already done
        if not isinstance(self.df_list, list):
            self.df_list = [self.data_df.copy()]
        self.data_df = None  # Additional safeguard to really use df_list from now on

        # Filter data
        if self.goodrows_ids:
            self._keep_good_data_rows()

        if self.badrows_ids:
            self._remove_bad_data_rows()

        # todo check if this works Numeric data only
        self._to_numeric()

        # Sanitize data
        # Replace inf and -inf with NAN
        # Sometimes inf or -inf can appear, they are interpreted
        # as some sort of number (i.e., the column dtype does not
        # become 'object' and they are not a string) but cannot be
        # handled.
        for ix, df in enumerate(self.df_list):
            self.df_list[ix].replace([np.inf, -np.inf], np.nan, inplace=True)

        # Timestamp
        if self.build_timestamp:
            self._build_timestamp()
        self._sort_timestamp()
        self._remove_index_duplicates()

        # Units
        self._add_units_row()
        self._rename_unnamed_units()

        # Columns
        self._remove_unnamed_cols()

    def _convert_to_float_or_string(self):
        """Convert data to float or string

        Convert each column to best possible data format, either
        float64 or string. NaNs are not converted to string, they
        are still recognized as missing after this step.
        """
        self.data_df = self.data_df.convert_dtypes(
            convert_boolean=False, convert_integer=False)

        _found_dtypes = self.data_df.dtypes
        _is_dtype_string = _found_dtypes == 'string'
        _num_dtype_string = _is_dtype_string.sum()
        _dtype_str_colnames = _found_dtypes[_is_dtype_string].index.to_list()
        _dtype_other_colnames = _found_dtypes[~_is_dtype_string].index.to_list()

        # self.logger.info(f"     Found dtypes: {_found_dtypes.to_dict()}")
        #
        # if _num_dtype_string > 0:
        #     self.logger.info(f"### (!)STRING WARNING ###:")
        #     self.logger.info(f"### {_num_dtype_string} column(s) were classified "
        #                      f"as dtype 'string': {_dtype_str_colnames}")
        #     self.logger.info(f"### If this is expected you can ignore this warning.")

    def _convert_timestamp_idx_col(self, var):
        """Convert to list of tuples if needed

        Since YAML is not good at processing list of tuples,
        they are given as list of lists,
            e.g. [ [ "date", "[yyyy-mm-dd]" ], [ "time", "[HH:MM]" ] ].
        In this case, convert to list of tuples,
            e.g.  [ ( "date", "[yyyy-mm-dd]" ), ( "time", "[HH:MM]" ) ].
        """
        new = var
        if isinstance(var[0], int):
            pass
        elif isinstance(var[0], list):
            for idx, c in enumerate(var):
                new[idx] = (c[0], c[1])
        return new

    def _keep_good_data_rows(self):
        """Keep good data rows only, needed for irregular formats"""
        for ix, goodrows_id in enumerate(self.goodrows_ids):
            filter_goodrows = self.df_list[ix].iloc[:, self.goodrows_col] == goodrows_id
            self.df_list[ix] = self.df_list[ix][filter_goodrows].copy()

    def _remove_bad_data_rows(self):
        """Remove bad data rows, needed for irregular formats"""
        for ix, badrows_id in enumerate(self.badrows_ids):
            filter_badrows = self.df_list[ix].iloc[:, self.badrows_col] != badrows_id
            self.df_list[ix] = self.df_list[ix][filter_badrows].copy()

    def _special_data_formats(self) -> list:
        """
        Convert data with special format to conventional format

        Conversion of the special format -ALTERNATING- results in
        two different dataframes that are returned in a list. To
        account for this special case, all other special format
        conversions also need to be returned as lists after this
         step for the sake of consistency.
        """
        df = None
        if self.special_format == '-ICOSSEQ-':
            df = self._special_format_icosseq()  # Returns single dataframe
        elif self.special_format == '-ALTERNATING-':
            df = self._special_format_alternating()  # Returns two dataframes in list
            missed_ids = self._check_special_format_alternating_missed_ids()
        df_list = [df] if not isinstance(df, list) else df
        return df_list, missed_ids

    def _check_special_format_alternating_missed_ids(self):
        """Check if there are IDs that were not defined in the filetype config

        In case of special format -ALTERNATING-, there can be multiple integer IDs
        at the start of each data record, this method checks if there are any new and
        undefined IDs
        """
        available_ids = self.data_df['ID'].dropna().unique().tolist()
        missed_ids = [x for x in self.goodrows_ids if x not in available_ids]
        missed_ids = 'all IDs defined' if len(missed_ids) == 0 else missed_ids
        return missed_ids

    def _special_format_alternating(self) -> list:
        """Special format -ALTERNATING-

        Store data with differrent IDs in different dataframes

        This special format stores data from two different sources in
        one file. Each row contains an ID that indicates from which
        data source the respective data row originated. Typically,
        the ID changes with each row. For example, first row has
        ID 225, second row ID 1, third row ID 225, fourth row ID 1, etc.

        However, first all file data with all different IDs is stored
        in one dataframe. The column names in this one dataframe are
        wrong at this point, they are fixed here.

        Therefore, this dataframe is split into two dataframes here.
        Each dataframe then contains data from one single data source
        and all rows have the same ID. Since the two dfs have a different
        number of vars stored in them, the column names are then also fixed.

        """
        # Get data for first ID
        filter_data_rows = self.data_df.iloc[:, self.goodrows_col] == self.goodrows_ids[0]
        data_df = self.data_df[filter_data_rows].copy()
        raw_varnames = list(self.filetypeconf['data_vars'].keys())  # Varnames for this ID
        n_raw_varnames = len(raw_varnames)  # Number of vars for this ID
        data_df = data_df.iloc[:, 0:n_raw_varnames].copy()  # Keep number of cols for this ID
        data_df.columns = raw_varnames  # Assign correct varnames

        # Not all -ALTERNATING- files have a second ID
        # In case there is only one ID, return dataframe as list
        if len(self.goodrows_ids) == 1:
            return [data_df]

        # Get data for second ID
        # Script arrives here only if there is more than one ID defined in goodrows_ids
        filter_data2_rows = self.data_df.iloc[:, self.goodrows_col] == self.goodrows_ids[1]
        data2_df = self.data_df[filter_data2_rows].copy()
        raw_varnames = list(self.filetypeconf['data_vars2'].keys())
        n_raw_varnames = len(raw_varnames)
        data2_df = data2_df.iloc[:, 0:n_raw_varnames].copy()
        data2_df.columns = raw_varnames
        return [data_df, data2_df]

    def _special_format_icosseq(self) -> pd.DataFrame:
        """Convert structure of sequential -ICOSSEQ- files

        This file format stores different heights for the same var in different
        rows, which is a difference to regular formats. Here, the data format
        is converted in a way so that each variable for each height is in a
        separate column.

        In case of -ICOSSEQ- files, the colname giving the height of the measurements
        is either LOCATION (newer files) or INLET (older files).

        This conversion makes sure that different heights are stored in different
        columns instead of different rows.

        """
        df = self.data_df.copy()

        # Detect subformat: profile or chambers
        subformat = None
        if '-PRF-' in self.filetype:
            subformat = 'PRF'
        if '-CMB-' in self.filetype:
            subformat = 'CMB'

        # Detect name of col where the different heights are stored
        locs_col = 'LOCATION' if any('LOCATION' in col for col in df.columns) else False
        if not locs_col:
            locs_col = 'INLET' if any('INLET' in col for col in df.columns) else False

        # Detect unique locations identifiers, e.g. T1_35
        locations = df[locs_col].dropna()
        locations = locations.unique()
        locations = list(locations)
        locations.sort()

        # Convert data structure
        # Loop over data subsets of unique locations and collect all in df
        locations_df = pd.DataFrame()
        for loc in locations:
            _loc_df = df.loc[df[locs_col] == loc, :]

            # If chambers, add vertical position index 0 (measured at zero height/depth)
            if subformat == 'CMB':
                loc = f"{loc}_0" if subformat == 'CMB' else loc

            renamedcols = []
            for col in df.columns:
                # _newname_suffix = '' if _newname_suffix in col else 'CMB' if subformat ==

                # Add subformat info to newname,
                #   e.g. 'PRF' for profile data
                addsuffix = '' if subformat in col else subformat

                # Assemble newname with variable name and location info,
                #   e.g. CO2_CMB_FF1_0_1
                #   with 'col' = 'CO2', 'addsuffix' = 'CMB', 'loc' = 'FF1_0'
                newname = f"{col}_{addsuffix}_{loc}_1"

                # Replace double underlines that occur when 'addsuffix' is empty
                newname = newname.replace("__", "_")

                # units = self.filetypeconf['data_vars'][col]['units']
                renamedcols.append(newname)

            # Make header with variable names
            _loc_df.columns = renamedcols
            locations_df = pd.concat([locations_df, _loc_df], axis=0)

        # Remove string cols w/ location info, e.g. LOCATION_X_X_X
        subsetcols = [col for col in locations_df if locs_col not in col]
        locations_df = locations_df[subsetcols]

        # Set the collected and converted data as main data
        df = locations_df
        return df

    def _remove_index_duplicates(self):
        for ix, df in enumerate(self.df_list):
            self.df_list[ix] = self.df_list[ix][~self.df_list[ix].index.duplicated(keep='last')]

    def _sort_timestamp(self):
        for ix, df in enumerate(self.df_list):
            self.df_list[ix].sort_index(inplace=True)

    def _remove_unnamed_cols(self):
        """Remove columns that do not have a column name"""
        for ix, df in enumerate(self.df_list):
            newcols = []
            for col in self.df_list[ix].columns:
                if any('Unnamed' in value for value in col):
                    pass
                else:
                    newcols.append(col)
            self.df_list[ix] = self.df_list[ix][newcols]
            self.df_list[ix].columns = pd.MultiIndex.from_tuples(newcols)

    def _rename_unnamed_units(self):
        """Units not given in files with two-row header yields "Unnamed ..." units"""
        for ix, df in enumerate(self.df_list):
            newcols = []
            for col in self.df_list[ix].columns:
                if any('Unnamed' in value for value in col):
                    col = (col[0], '-not-given-')
                newcols.append(col)
            self.df_list[ix].columns = pd.MultiIndex.from_tuples(newcols)

    def _to_numeric(self):
        """Make sure all data are numeric"""
        for ix, df in enumerate(self.df_list):
            try:
                self.df_list[ix] = self.df_list[ix].astype(float)  # Crashes if not possible
            except:
                self.df_list[ix] = self.df_list[ix].apply(pd.to_numeric, errors='coerce')  # Does not crash

    # def _get_date_parser(self, parser):
    # date_parser arg is now deprecated in pandas
    #     return lambda c: pd.to_datetime(c, format=parser, errors='coerce') if parser else False
    #     # Alternative: date_parser = lambda x: dt.datetime.strptime(x, self.fileformatconf['date_format'])

    def _add_units_row(self):
        """Units not given in files with single-row header"""

        for ix, df in enumerate(self.df_list):
            if not isinstance(self.df_list[ix].columns, pd.MultiIndex):
                newcols = []
                for col in self.df_list[ix].columns:
                    newcol = (col, '-not-given-')
                    newcols.append(newcol)
                self.df_list[ix].columns = pd.MultiIndex.from_tuples(newcols)

    def _readfile(self):
        """Read data file"""
        args = dict(filepath_or_buffer=self.filepath,
                    skiprows=self.skiprows,
                    header=self.header,
                    na_values=self.na_values,
                    encoding=self.data_encoding,
                    delimiter=self.delimiter,
                    # mangle_dupe_cols=self.mangle_dupe_cols,  # deprecated in pandas
                    keep_date_col=self.keep_date_col,
                    parse_dates=self.parse_dates,
                    date_format=self.date_format,
                    # date_parser=self.date_format,  # deprecated in pandas
                    index_col=self.index_col,
                    # engine='c',
                    engine='python',
                    # nrows=5,
                    nrows=self.nrows,
                    compression=self.compression,
                    on_bad_lines='warn',  # in pandas v1.3.0
                    usecols=None,
                    names=self.names,
                    skip_blank_lines=True
                    )

        # Try to read with args
        try:
            # todo read header separately like in diive
            df = pd.read_csv(**args)
        except ValueError:
            # Found to occur when the first row is empty and the
            # second row has errors (e.g., too many columns).
            # Observed in file logger2013010423.a59 (CH-DAV).
            # 'names' arg cannot be used if the second row
            # has more columns than defined in config, therefore
            # the row has to be skipped. The first row (empty) alone
            # is not the problem since this is handled by
            # 'skip_blank_lines=True'. However, if the second row
            # has errors then BOTH rows need to be skipped by setting
            # arg 'skiprows=[0, 1]'.
            # [0, 1] means that the empty row is skipped (0)
            # and then the erroneous row is skipped (1).
            args['skiprows'] = [0, 1]
            df = pd.read_csv(**args)
        except _csv.Error:
            # The _csv.Error occurs e.g. in case there are NUL bytes in
            # the data file. The python engine cannot handle these bytes,
            # but the c engine can.
            args['engine'] = 'c'
            df = pd.read_csv(**args)

        return df

    def _build_timestamp(self):
        """
        Build full datetime timestamp by combining several cols
        """

        df_list = self.df_list

        for ix, df in enumerate(df_list):

            # Build from columns by index, column names not available
            if self.build_timestamp == 'YEAR0+MONTH1+DAY2+HOUR3+MINUTE4':
                # Remove rows where date info is missing
                _not_possible = df['YEAR'].isnull()
                df = df[~_not_possible]
                _not_possible = df['MONTH'].isnull()
                df = df[~_not_possible]
                _not_possible = df['DAY'].isnull()
                df = df[~_not_possible]
                _not_possible = df['HOUR'].isnull()
                df = df[~_not_possible]
                _not_possible = df['MINUTE'].isnull()
                df = df[~_not_possible]

                # pandas recognizes columns with these names as time columns
                df['TIMESTAMP'] = pd.to_datetime(df[['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE']])

                # Remove original columns
                dropcols = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE']
                df.drop(dropcols, axis=1, inplace=True)

                # Remove rows where timestamp-building did not work
                locs_emptydate = df['TIMESTAMP'].isnull()
                df = df.loc[~locs_emptydate, :]

                # Set as index
                df.set_index('TIMESTAMP', inplace=True)

                self.df_list[ix] = df.copy()

            # Build from columns by name, column names available
            if self.build_timestamp == 'YEAR+DOY+TIME':
                # Remove rows where date info is missing
                _not_possible = df['YEAR'].isnull()
                df = df[~_not_possible]
                _not_possible = df['DOY'].isnull()
                df = df[~_not_possible]
                _not_possible = df['DOY'] == 0
                df = df[~_not_possible]
                _not_possible = df['TIME'].isnull()
                df = df[~_not_possible]

                df['_basedate'] = pd.to_datetime(df['YEAR'], format='%Y', errors='coerce')
                df['_doy_timedelta'] = pd.to_timedelta(df['DOY'], unit='D') - pd.Timedelta(days=1)
                df['_time_str'] = df['TIME'].astype(int).astype(str).str.zfill(4)
                df['_time'] = pd.to_datetime(df['_time_str'], format='%H%M', errors='coerce')
                df['_hours'] = df['_time'].dt.hour
                df['_hours'] = pd.to_timedelta(df['_hours'], unit='hours')
                df['_minutes'] = df['_time'].dt.minute
                df['_minutes'] = pd.to_timedelta(df['_minutes'], unit='minutes')

                df['TIMESTAMP'] = df['_basedate'] \
                                  + df['_doy_timedelta'] \
                                  + df['_hours'] \
                                  + df['_minutes']

                dropcols = ['_basedate', '_doy_timedelta', '_hours', '_minutes', '_time', '_time_str',
                            'YEAR', 'DOY', 'TIME']
                df.drop(dropcols, axis=1, inplace=True)
                locs_emptydate = df['TIMESTAMP'].isnull()
                df = df.loc[~locs_emptydate, :]
                df.set_index('TIMESTAMP', inplace=True)

                self.df_list[ix] = df.copy()


def get_conf_filetypes(dir: Path, ext: str = 'yaml') -> dict:
    """Search config files with file extension *ext* in folder *dir*"""
    dir = str(dir)  # Required as string for os.walk
    conf_filetypes = {}
    for root, dirs, files in os.walk(dir):
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
