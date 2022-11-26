# Changelog

## v0.6 | 26 Nov 2022

### New special format -ALTERNATING-

- Added handling of `-ALTERNATING-` filetypes

Some data files store data from two different data sources in the same file. In these
files, the first record in each data row contains an ID that indicates from which data
source the respective data row originated. Typically, the ID changes in each row,
alternating between two different IDs.

This format was encountered e.g. in filetype `DAV10-RAW-PROFILE-201009252127-ALTERNATING-A-10MIN`.
The string `-ALTERNATING-` in the filetype name indicates that this data format needs
special treatment.

For example, in the file `meteo2011082314.a19` the first 10 data rows look like this (the dots `...`
mean that there are more data that are not shown here to keep it short):
> ,6.6696,6.6693,6.6495,6.6595, ...  
> 225,2011,231,1800,14.32,18.29, ...  
> 1,2011,231,1800, 43.5,387.39, ...  
> 225,2011,231,1810,14.87,18.35, ...  
> 1,2011,231,1811, 23.3,387.91, ...  
> 225,2011,231,1820,14.94,17.61, ...  
> 1,2011,231,1820, 43.5,389.68, ...  
> 225,2011,231,1830,14.97,17.47, ...  
> 1,2011,231,1831, 23.3,389.82, ...  
> 225,2011,231,1840,14.78,17.18, ...  
> ...

Here, ID 225 are data rows for the conventional profile, and ID 1 are data from the CO2/H2O profile.
It is possible that there are other IDs in the file (e.g. see the first row in the example), but
those IDs are ignored.

To handle these files, `dbc` splits the file data based on IDs. From this, two different dataframes
are built: one contains ID 225 data, and the other contains ID 1 data.

This required quite a bit of refactoring because up to now `dbc` only needed to handle one dataframe
per data file, and the `-ALTERNATING-` filetypes produce two dataframes per data file. These two 
dataframes are returned in a list, such as `[dataframe1, dataframe2]`. 

This is quite different than before, when `dataframeX` was directly used as `dataframeX`. Now,
the `dataframeX` is used in a list, i.e., as `[dataframeX]`. By using a list certain steps when
preparing and uploading data can be kept consistent, no matter if it is one dataframe that
contains all data from a file (`[dataframeX]`), or if the file data was split into two 
dataframes (`[dataframe1, dataframe2]`): in both cases it is a list.

The general logic for handling `-ALTERNATING-` filetypes is, relating to the example above:

- Read complete data file (contains IDs 225, 1 and other; 37 columns)
    - This results in 37 columns, because ID 225 (37 columns) has more columns than ID 1 (29 columns)
      and `.read_csv()` from pandas assumes that this number is valid for all data rows. 
- Split data file into dataframe1 (contains only ID 225 data; 37 columns) and dataframe2 (ID 1; 37 columns)
- Shorten to correct number of columns, based on variable names given in filetype settings:
    - dataframe1 --> 37 columns (based on variables given in filetype setting *data_vars*)
    - dataframe2 --> 29 columns (*data_vars2*)
- Assign correct variable names based on filetype settings
    - dataframe1 --> 37 variable names (based on variables given in filetype setting *data_vars*)
    - dataframe2 --> 29 variable names (*data_vars2*)
- Store both dataframes in a list as `[dataframe1, dataframe2]`

### Other changes

- In `filetypereader.FileTypeReader`, the engine used for `.read_csv()` switches to `c`
  in case of `_csv.Error`. This was implemented so `.read_csv` can handle files containing
  NUL bytes. The `python` engine which is used by default raises `_csv.Error` and cannot
  continue to read the file if NUL bytes occur. This is the case e.g. for the file
  `CH-DAV_CR1000_T1_35_1_TBL1_20180203-2103.csv.gz` (filetype `DAV10-RAW-TBL1-201507021347-CSV-GZ-1MIN`).

## v0.5.0 | 26 Oct 2022

- Added new module `manual_run` that collects code that can be used to manually upload
  and download files.
- Duplicate index entries in table data downloaded from database are now removed, the last
  data point is kept (`keep=last`).

## v0.4.0 | 29 Jul 2022

### Better handling of timezone info during upload

Data are uploaded using either `.upload_singlevar` or `.upload_filetype`. These
methods take the arg `timezone_of_timestamp` that gives info about the timezone
of the timestamp of the data that is uploaded.

For example, if `timezone_of_timestamp="UTC+01:00"` then this means that the
uploaded data has an offset of one hour with regards to `UTC`, this one hour offset
corresponds to `CET` (winter time). The upload methods then use this info to upload
the data with `UTC` timestamp. All data in the database are in `UTC`.

When a data file is read using `filetypereader`, no timezone info is added. This
means that the data file is read "as is", the timestamp is simply stored without any
info about the timezone.

For consistency, the timezone info is only added when the data are uploaded to
the database.

### Other

- Changed `timeout` in `db.get_client` from 20 to 999 seconds. This is to avoid a
  timeout error when downloading long (raw) datasets.

## v0.3.0 | 17 Jul 2022

- ~~`filetypereader`: File data can now be read in UTC (=GMT) time with timezone info~~
  (this behavior was changed in `v0.4.0`)
- Data can now be downloaded in desired timezone, e.g. `GMT+1` for `CET` (uses pytz.timezones)

## v0.2.0 | 28 Jun 2022

- Renamed package to `dbc-influxdb` (previous name was `dbc`)

## v0.1.0 | 26 Jun 2022

- Initial version
