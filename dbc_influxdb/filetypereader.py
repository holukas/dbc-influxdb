import fnmatch
import os
from logging import Logger
from pathlib import Path

import pandas as pd
import yaml

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 30)


class FileTypeReader():
    """Read file and its variables according to a specified filetype"""

    def __init__(self,
                 filepath: str,
                 filetype: str,
                 filetypeconf: dict,
                 nrows=None,
                 logger: Logger = None):
        self.filepath = filepath
        self.filetype = filetype
        self.logger = logger

        self.data_df = pd.DataFrame()

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
        self.date_parser = self._get_date_parser(parser=filetypeconf['data_date_parser'])
        self.na_values = filetypeconf['data_na_values']
        self.delimiter = filetypeconf['data_delimiter']
        self.mangle_dupe_cols = filetypeconf['data_mangle_dupe_cols']
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
        special_formats = ['-ICOSSEQ-']
        for sf in special_formats:
            self.special_format = sf if sf in self.filetype else False

        self.file_info = dict(
            filepath=self.filepath,
            filetype=self.filetype,
            special_format=self.special_format
        )

        self.goodrows_col = None if not filetypeconf['data_keep_good_rows'] \
            else filetypeconf['data_keep_good_rows'][0]  # Col used to identify rows w/ good data
        self.goodrows_id = None if not filetypeconf['data_keep_good_rows'] \
            else filetypeconf['data_keep_good_rows'][1]  # ID used to identify rows w/ good data

        self.badrows_col = None if not filetypeconf['data_remove_bad_rows'] \
            else filetypeconf['data_remove_bad_rows'][0]  # Col used to identify rows w/ bad data
        self.badrows_id = None if not filetypeconf['data_remove_bad_rows'] \
            else filetypeconf['data_remove_bad_rows'][1]  # ID used to identify rows w/ bad data

        # # Detect if file has special format that needs formatting
        # self.is_special_format = True if any(f in self.config_filetype for f in self.special_formats) else False

        self.run()

    def get_data(self):
        return self.data_df, self.file_info

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
        if self.special_format:
            self._special_data_formats()

        # Filter data
        if self.goodrows_id:
            self._keep_good_data_rows()
        if self.badrows_id:
            self._remove_bad_data_rows()

        # todo check if this works Numeric data only
        self._to_numeric()

        # Timestamp
        if self.build_timestamp:
            self.data_df = self._build_timestamp()
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
        filter_goodrows = self.data_df.iloc[:, self.goodrows_col] == self.goodrows_id
        self.data_df = self.data_df[filter_goodrows]

    def _remove_bad_data_rows(self):
        """Remove bad data rows, needed for irregular formats"""
        filter_badrows = self.data_df.iloc[:, self.badrows_col] != self.badrows_id
        self.data_df = self.data_df[filter_badrows]

    def _special_data_formats(self):

        if self.special_format == '-ICOSSEQ-':
            self._special_format_icosseq()

    def _special_format_icosseq(self):
        """Convert structure of sequential ICOSSEQ files

        This file format stores different heights for the same var in different
        rows, which is a difference to regular formats. Here, the data format
        is converted in a way so that each variable for each height is in a
        separate column.

        In case of ICOSSEQ files, the colname giving the height of the measurements
        is either LOCATION (newer files) or INLET (older files).

        This conversion makes sure that different heights are stored in different
        columns instead of different rows.

        """

        # Detect subformat: profile or chambers
        subformat = None
        if '-PRF-' in self.filetype:
            subformat = 'PRF'
        if '-CMB-' in self.filetype:
            subformat = 'CMB'

        # Detect name of col where the different heights are stored
        locs_col = 'LOCATION' if any('LOCATION' in col for col in self.data_df.columns) else False
        if not locs_col:
            locs_col = 'INLET' if any('INLET' in col for col in self.data_df.columns) else False

        # Detect unique locations identifiers, e.g. T1_35
        locations = self.data_df[locs_col].dropna()
        locations = locations.unique()
        locations = list(locations)
        locations.sort()

        # Convert data structure
        # Loop over data subsets of unique locations and collect all in df
        locations_df = pd.DataFrame()
        for loc in locations:
            _loc_df = self.data_df.loc[self.data_df[locs_col] == loc, :]

            # If chambers, add vertical position index 0 (measured at zero height/depth)
            if subformat == 'CMB':
                loc = f"{loc}_0" if subformat == 'CMB' else loc

            renamedcols = []
            for col in self.data_df.columns:
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
        self.data_df = locations_df

    def _remove_index_duplicates(self):
        self.data_df = self.data_df[~self.data_df.index.duplicated(keep='last')]

    def _sort_timestamp(self):
        self.data_df.sort_index(inplace=True)

    def _remove_unnamed_cols(self):
        """Remove columns that do not have a column name"""
        newcols = []
        for col in self.data_df.columns:
            if any('Unnamed' in value for value in col):
                pass
            else:
                newcols.append(col)
        self.data_df = self.data_df[newcols]
        self.data_df.columns = pd.MultiIndex.from_tuples(newcols)

    def _rename_unnamed_units(self):
        """Units not given in files with two-row header yields "Unnamed ..." units"""
        newcols = []
        for col in self.data_df.columns:
            if any('Unnamed' in value for value in col):
                col = (col[0], '-not-given-')
            newcols.append(col)
        self.data_df.columns = pd.MultiIndex.from_tuples(newcols)

    def _to_numeric(self):
        """Make sure all data are numeric"""
        try:
            self.data_df = self.data_df.astype(float)  # Crashed if not possible
        except:
            self.data_df = self.data_df.apply(pd.to_numeric, errors='coerce')  # Does not crash

    def _get_date_parser(self, parser):
        return lambda c: pd.to_datetime(c, format=parser, errors='coerce') if parser else False
        # Alternative: date_parser = lambda x: dt.datetime.strptime(x, self.fileformatconf['date_format'])

    def _add_units_row(self):
        """Units not given in files with single-row header"""
        if not isinstance(self.data_df.columns, pd.MultiIndex):
            # if header and len(header) == 1:
            newcols = []
            for col in self.data_df.columns:
                newcol = (col, '-not-given-')
                newcols.append(newcol)
            self.data_df.columns = pd.MultiIndex.from_tuples(newcols)

    def _readfile(self):
        """Read data file"""
        args = dict(filepath_or_buffer=self.filepath,
                    skiprows=self.skiprows,
                    header=self.header,
                    na_values=self.na_values,
                    encoding=self.data_encoding,
                    delimiter=self.delimiter,
                    mangle_dupe_cols=self.mangle_dupe_cols,
                    keep_date_col=self.keep_date_col,
                    parse_dates=self.parse_dates,
                    date_parser=self.date_parser,
                    index_col=self.index_col,
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
            # print(df.head(5))
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

        return df

    def _build_timestamp(self):
        """
        Build full datetime timestamp by combining several cols

        :param df:
        :param method:
        :return:
        pd.DataFrame
        """

        df = self.data_df.copy()

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

            return df

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
            return df


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
