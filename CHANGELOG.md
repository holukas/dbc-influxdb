# Changelog


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
