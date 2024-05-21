import warnings

import pandas as pd
from influxdb_client import WriteOptions
from pandas import DataFrame

from dbc_influxdb.common import tags
from dbc_influxdb.db import get_client

warnings.simplefilter(action='ignore', category=FutureWarning)


class VarScanner:
    script_id = "[dbc.varscanner]"

    def __init__(
            self,
            file_df: DataFrame,
            data_vars: dict,
            data_raw_freq: str,
            freq: str,
            config_filetype: str,
            filetypeconf: dict,
            conf_unitmapper: dict,
            to_bucket: str,
            conf_db: dict,
            logger=None
    ):
        self.file_df = file_df
        self.data_vars = data_vars
        self.data_raw_freq = data_raw_freq
        self.freq = freq
        self.config_filetype = config_filetype  # Filetype string, name of filetype
        self.filetypeconf = filetypeconf  # Configuration dict for this filetype
        self.conf_unitmapper = conf_unitmapper
        self.to_bucket = to_bucket
        self.conf_db = conf_db
        self.log = logger if logger else None

        self.varscanner_df = self._init_varscanner_df()
        self.vars_empty_not_uploaded = []

    def run(self):
        # Database clients
        client = get_client(conf_db=self.conf_db)

        # write_api = get_write_api(client=client)

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
            # Loop through vars
            self._loopvars(write_api=write_api)



        # self.varscanner_df.sort_values(by='raw_varname', axis=0, inplace=True)
        # self.varscanner_df.index = arange(1, len(self.varscanner_df) + 1)  # Reset index, starting at 1
        self._end_log()

    def _end_log(self):
        """Show some results in log file"""
        pass
        # print(f"{self.class_id} Found unique variables across all files:")
        # for ix, file in self.varscanner_df.iterrows():
        #     print(f"     Var #{ix}: {dict(file)}")
        # print(f"     Found {self.varscanner_df.__len__()} unique variables across all files.")

    def get_results(self):
        return self.varscanner_df

    def _loopvars(self, write_api):
        """Loop over vars in file"""

        numvars = len(self.file_df.columns)
        counter = 0

        # Find variables
        for dfvar in self.file_df.columns.to_list():

            counter += 1

            # if dfvar[0] == 'PREC_T1_0x50_1_Tot':
            #     print("STOP")
            # print(type(self.file_df[dfvar]))

            # Check if data are available, skip var if not
            if self.file_df[dfvar].dropna().empty:
                self.vars_empty_not_uploaded.append(dfvar)
                self._log_no_data(var=dfvar)
                continue

            # Collect varinfo
            newvar, is_greenlit = self.create_varentry(rawvar=dfvar)

            newvar['first_date'] = self.file_df[dfvar].index[0]
            newvar['last_date'] = self.file_df[dfvar].index[-1]

            # Check greenlit
            if not is_greenlit:
                newvar['greenlit'] = '-not-greenlit-'  # Stored but not used as tag

            # Ingest var into database
            elif is_greenlit:
                newvar['greenlit'] = 'greenlit'  # Stored but not used as tag
                self._ingest(df=self.file_df, newvar=newvar,
                             counter=counter, numvars=numvars, write_api=write_api)

            # todo Add var to found vars in overview of found variables
            self.varscanner_df = pd.concat([self.varscanner_df, pd.DataFrame.from_dict([newvar])],
                                           axis=0, ignore_index=True)

        if self.log:
            self.log.info(f"{self.script_id}")
            self.log.info(f"{self.script_id} *** FINISHED DATA UPLOAD FOR FILETYPE {newvar['config_filetype']}.")
            self.log.info(f"{self.script_id} *** database bucket: {newvar['db_bucket']}.")
            self.log.info(f"{self.script_id} *** first date: {newvar['first_date']}")
            self.log.info(f"{self.script_id} *** last date: {newvar['last_date']}")
            # self.logger.info(logtxt) if self.logger else print(logtxt)

    def _log_no_data(self, var):
        logtxt = f"### (!)VARIABLE WARNING: NO DATA ###: Variable {var} is empty and will be skipped."
        self.log.info(logtxt) if self.log else print(logtxt)

    def _ingest(self, df: pd.DataFrame, newvar, counter: int, numvars: int,
                write_api):
        """Collect variable data and tags and upload to database

        New df that contains the variable (field) and tags (all other columns)

        """

        # Initiate dataframe that will collect data and tags for current var

        # Depending on the format of the file (regular or one of the
        # special formats), the columns that contains the data for the
        # current var has to be addressed differently:
        #   - Regular formats have original varnames ('raw_varname') and
        #     original units ('raw_units') in df.
        #   - Special formats have *renamed* varnames ('field') and
        #     original units ('raw_units') in df.
        varcol = 'raw_varname' if not self.filetypeconf['data_special_format'] == '-ICOSSEQ-' else 'field'
        varcol = (newvar[varcol], newvar['raw_units'])  # Column name to access var in df
        var_df = pd.DataFrame(index=df.index, data=df[varcol])

        # Apply gain (gain = 1 if no gain is specified in filetype settings)
        # if newvar['gain'] != 1:
        #     print(newvar['gain'])
        var_df[varcol] = var_df[varcol].multiply(newvar['gain'])

        # Ignore data after the datetime given in `ignore_after`
        if newvar['ignore_after']:
            firstdate = var_df.index[0]
            lastalloweddate = pd.to_datetime(newvar['ignore_after'], format='%Y-%m-%d %H:%M:%S')
            lastalloweddate = pd.Timestamp(newvar['ignore_after'], tz='UTC+01:00')
            var_df = var_df.loc[firstdate:lastalloweddate].copy()

        # Remove units row (units stored as tag)
        var_df.columns = var_df.columns.droplevel(1)

        # 'var_df' currently has only one column containing the variable data.
        # Get name of the column so we can rename it
        varcol = var_df.iloc[:, 0].name
        var_df.rename(columns={varcol: newvar['field']}, inplace=True)

        var_df.dropna(inplace=True)

        # Tags: add as columns
        var_df['varname'] = newvar['field']  # Store 'field' ('_field' in influxdb) also as tag
        var_df['units'] = newvar['units']
        var_df['raw_varname'] = newvar['raw_varname']
        var_df['raw_units'] = newvar['raw_units']
        var_df['hpos'] = newvar['hpos']
        var_df['vpos'] = newvar['vpos']
        var_df['repl'] = newvar['repl']
        var_df['data_raw_freq'] = newvar['data_raw_freq']
        var_df['freq'] = newvar['freq']
        var_df['filegroup'] = newvar['filegroup']
        var_df['config_filetype'] = newvar['config_filetype']
        var_df['data_version'] = newvar['data_version']
        var_df['gain'] = newvar['gain']

        # Write to db
        # Output also the source file to log
        logtxt = f"{self.script_id} " \
                 f"--> UPLOAD TO DATABASE BUCKET {newvar['db_bucket']}:  " \
                 f"{newvar['raw_varname']} as {newvar['field']}  " \
                 f"Var #{counter} of {numvars}"
        self.log.info(logtxt) if self.log else print(logtxt)

        write_api.write(newvar['db_bucket'],
                        record=var_df,
                        data_frame_measurement_name=newvar['measurement'],
                        data_frame_tag_columns=tags,
                        write_precision='s')

    def _init_varentry(self, rawvar) -> dict:
        """Collect variable info"""
        newvar = dict(
            config_filetype=self.config_filetype,
            filegroup=self.filetypeconf['filegroup'],
            data_version=self.filetypeconf['data_version'],
            db_bucket=self.to_bucket,
            data_raw_freq=self.data_raw_freq,
            freq=self.freq,
            raw_units=rawvar[1],
            raw_varname='',
            measurement='',  # Not a tag, stored as _measurement in db
            field='',  # Not a tag, stored as _field in db
            varname='',  # Same as field, but is stored additionally as tag so the varname can be accessed via tags
            units='',
            hpos='',
            vpos='',
            repl='',
            gain=''
        )
        return newvar

    def create_varentry(self, rawvar):
        """Loop through variables in file and collect info for each var

        Collects the following varinfo:
            - raw_varname, raw_units
            - config_filetype, filetypeconf
            - measurement, field, varname (= same as field), units
            - hpos, vpos, repl

        """

        assigned_units = None
        gain = None
        is_greenlit = False

        # Collect varinfo as tags in dict
        newvar = self._init_varentry(rawvar=rawvar)

        # Get var settings from configuration
        if rawvar[0] in self.data_vars.keys():
            # Variable name in file data is the same as given in settings
            newvar, assigned_units, gain, is_greenlit, ignore_after = \
                self._match_exact_name(newvar=newvar, filetypeconf=self.filetypeconf, rawvar=rawvar)

        elif self.filetypeconf['data_special_format'] == '-ICOSSEQ-':
            # If rawvar is *not* given with the exact name in data_vars
            #
            # This is the case with e.g. ICOSSEQ files that store measurements
            # at different heights in different rows (instead of different
            # columns). In such case, the file is converted so that each
            # different height is in its separate column. That means that
            # the rawvar names for each column are generated dynamically
            # from info in the file and that therefore the rawvar cannot
            # be given with the *exact* name in the config file.

            # Assigned units from config file and measurement
            for dv in self.data_vars:
                if rawvar[0].startswith(dv):
                    newvar['raw_varname'] = f"{dv}"
                    newvar['measurement'] = self.data_vars[dv]['measurement']
                    newvar['field'] = rawvar[0]  # Already correct name
                    assigned_units = self.data_vars[dv]['units']

                    # Gain from config file if provided, else set to 1
                    gain = self.data_vars[dv]['gain'] \
                        if 'gain' in self.data_vars[dv] else 1

                    # ignore_after date from config file, else set to None
                    if 'ignore_after' in self.data_vars[dv]:
                        ignore_after = self.data_vars[dv]['ignore_after']
                    else:
                        ignore_after = None

                    # Indicate that var was found in config file
                    is_greenlit = True
                    break
        else:
            pass

        if not is_greenlit:
            # If script arrives here, no valid entry for current var
            # was found in the config file
            _varinfo_not_greenlit = dict(raw_varname=rawvar[0],
                                         measurement='-not-greenlit-',
                                         field='-not-greenlit-',
                                         varname='-not-greenlit-',
                                         units='-not-greenlit-',
                                         hpos='-not-greenlit-',
                                         vpos='-not-greenlit-',
                                         repl='-not-greenlit-',
                                         gain='-not-greenlit-')
            for k in _varinfo_not_greenlit.keys():
                newvar[k] = _varinfo_not_greenlit[k]
            return newvar, is_greenlit

        # Naming convention: units
        newvar['units'] = self.get_units_naming_convention(
            raw_units=newvar['raw_units'],
            assigned_units=assigned_units,
            conf_unitmapper=self.conf_unitmapper)

        # Position indices from field (the name of the variable)
        # For e.g. eddy covariance variables the indices are not
        # given in the yaml filetype settings, leave empty
        newvar['hpos'] = '-not-given-'
        newvar['vpos'] = '-not-given-'
        newvar['repl'] = '-not-given-'
        if self.filetypeconf['data_vars_parse_pos_indices']:
            try:
                newvar['hpos'] = newvar['field'].split('_')[-3]
                newvar['vpos'] = newvar['field'].split('_')[-2]
                newvar['repl'] = newvar['field'].split('_')[-1]
            except:
                pass

        newvar['varname'] = newvar['field']
        newvar['gain'] = gain
        newvar['ignore_after'] = ignore_after

        return newvar, is_greenlit

    def _match_exact_name(self, newvar, filetypeconf, rawvar):
        """Match variable name from data with variable name from settings ('data_vars')"""
        # If rawvar is given as variable in data_vars
        newvar['raw_varname'] = rawvar[0]
        newvar['measurement'] = self.data_vars[rawvar[0]]['measurement']

        # Naming convention: variable name
        newvar['field'] = self.get_varname_naming_convention(raw_varname=newvar['raw_varname'])

        # Assigned units from config file
        assigned_units = self.data_vars[rawvar[0]]['units']

        # Gain from config file if provided, else set to 1
        gain = self.data_vars[rawvar[0]]['gain'] \
            if 'gain' in self.data_vars[rawvar[0]] else 1

        # ignore_after date from config file, else set to None
        if 'ignore_after' in self.data_vars[rawvar[0]]:
            ignore_after = self.data_vars[rawvar[0]]['ignore_after']
        else:
            ignore_after = None

        # Indicate that var was found in config file
        is_greenlit = True

        return newvar, assigned_units, gain, is_greenlit, ignore_after

    def get_varname_naming_convention(self, raw_varname) -> str:
        """Map standarized naming convention varname to raw varname, stored as *field* in db"""
        if raw_varname in self.data_vars:
            field = self.data_vars[raw_varname]['field'] \
                if self.data_vars[raw_varname]['field'] else raw_varname
        else:
            field = '-not-defined-'
        return field

    @staticmethod
    def get_units_naming_convention(conf_unitmapper, raw_units, assigned_units) -> str:
        """Map standarized naming convention units to raw units
        - Assigned units are prioritized over units found in the file
        - Variables that do not have units in file will use assigned units
        """
        if assigned_units:
            raw_units = assigned_units
        if raw_units in conf_unitmapper:
            # Only map if given
            units = conf_unitmapper[raw_units] if conf_unitmapper[raw_units] else raw_units
        else:
            units = '-not-defined-'
        return units

    def _check_entry(self, newvar: dict) -> bool:
        """Check if var entry is already in df"""
        newvar = pd.Series(newvar).sort_index()
        entry_in_df = False
        # print(self.varscanner_df.__len__())
        for entry in self.varscanner_df.iterrows():
            bothequal = self.arrays_equal(newvar.values,
                                          entry[1].sort_index().values)  # entry[0] is the index in the df
            if bothequal:
                entry_in_df = True
                break
            else:
                pass
        return entry_in_df

    @staticmethod
    def arrays_equal(a, b):
        if a.shape != b.shape:
            return False
        for ai, bi in zip(a.flat, b.flat):
            if ai != bi:
                return False
        return True

    @staticmethod
    def _init_varscanner_df() -> pd.DataFrame:
        """Collects info about each var"""
        return pd.DataFrame(columns=['raw_varname', 'raw_units',
                                     'measurement', 'field', 'units',
                                     'config_filetype'])  # Collects all found variable names
