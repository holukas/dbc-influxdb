# dbc-influxdb

**d**ata**b**ase **c**ommunication with InfluxDB

Python library to communicate with `InfluxDB 2.x`: show, download, upload and delete time series data.

## Requirements

- Python `>=3.12`
- [uv](https://docs.astral.sh/uv/) for dependency management
- Access to an `InfluxDB 2.x` instance

## Installation

This project uses [uv](https://docs.astral.sh/uv/). To set up a development environment:

```bash
uv sync
```

This creates a virtual environment in `.venv` using the Python version pinned in
`.python-version` (3.12) and installs all dependencies from `uv.lock`.

Run commands inside the environment with `uv run`, e.g.:

```bash
uv run pytest
uv run jupyter lab
```

To add the library as a dependency of another uv project:

```bash
uv add dbc-influxdb
```

## Configuration

`dbcInflux` is initialized with a path to a directory holding the YAML
configuration files (`dirconf`). The directory is expected to contain the
configs for filetypes, the unit mapper, directories and the database
connection. The database config provides the connection details
(URL, organization, token) used to talk to InfluxDB.

```python
from dbc_influxdb import dbcInflux

dbc = dbcInflux(dirconf=r"path/to/configs")
```

The connection is tested automatically on initialization.

## Usage

### Download data

```python
data, data_detailed, assigned_measurements = dbc.download(
    bucket="my_bucket",
    measurements=["TA", "SW"],
    fields=["TA_T1_2_1"],
    start="2022-07-04 00:30:00",
    stop="2022-07-05 12:00:00",
    timezone_offset_to_utc_hours=1,   # e.g. 1 for CET (winter time)
    data_version="meteoscreening",
)
```

All data are stored in UTC in the database. `timezone_offset_to_utc_hours`
controls the timezone of the `start`/`stop` arguments and of the returned
timestamps. The exact `stop` date is **not** included.

### Upload a single variable

```python
dbc.upload_singlevar(
    var_df=var_df,                       # data + required tag columns
    to_bucket="my_bucket",
    to_measurement="TA",
    timezone_offset_to_utc_hours=1,
    delete_from_db_before_upload=True,
)
```

### Delete data

```python
dbc.delete(
    bucket="my_bucket",
    measurements=["TA"],
    start="2022-07-04 00:30:00",
    stop="2022-07-05 12:00:00",
    timezone_offset_to_utc_hours=1,
    data_version="meteoscreening",
    fields=["TA_T1_2_1"],
)
```

### Inspect the database and configs

```python
dbc.show_buckets()
dbc.show_measurements_in_bucket(bucket="my_bucket")
dbc.show_fields_in_bucket(bucket="my_bucket", measurement="TA")
dbc.show_fields_in_measurement(bucket="my_bucket", measurement="TA")

dbc.show_configs_filetypes()
dbc.show_config_for_filetype(filetype="...")
dbc.show_configs_unitmapper()
dbc.show_configs_dirs()
```

## Terminal UI (TUI)

An interactive terminal UI (built with [Textual](https://textual.textualize.io/))
is available for browsing a bucket's measurements/fields and **downloading** or
**deleting** data without writing a script:

```bash
dbc-influxdb-tui --dirconf path/to/configs
# or
uv run python -m dbc_influxdb.tui --dirconf path/to/configs
```

The config directory can also be supplied via the `DBC_DIRCONF` environment
variable.

To just see what the UI looks like — no config files and no database connection
required — launch it in demo mode with built-in sample data:

```bash
dbc-influxdb-tui --demo
# or
uv run python -m dbc_influxdb.tui --demo
```

In demo mode the buckets/measurements/fields are fabricated and Download/Delete
are simulated. Pick a bucket to populate its measurements, optionally narrow down to
specific measurements/fields, set the date range and timezone offset, then
Download or Delete. Deletes always show the matched scope and require explicit
confirmation in a modal first.

## Notebooks

Example workflows for downloading and deleting data are available in the
[`notebooks/`](notebooks) directory.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the version history.
