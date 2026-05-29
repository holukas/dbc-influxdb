# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

`dbc-influxdb` — a Python library for communicating with `InfluxDB 2.x`: show,
download, upload and delete time series data. The public API is the `dbcInflux`
class.

## Environment

- Python `>=3.12` (pinned in `.python-version` to `3.12`).
- Dependency management via [uv](https://docs.astral.sh/uv/) — `pyproject.toml`
  (PEP 621) + `uv.lock`, `hatchling` build backend. Poetry is no longer used.

### Common commands

```bash
uv sync                 # create/update .venv from uv.lock
uv run pytest           # run the test suite
uv lock                 # re-resolve and update uv.lock
uv add <package>        # add a dependency
uv run jupyter lab      # work with the notebooks/
```

Always run Python/tooling through `uv run` so the locked environment is used.

## Layout

- `dbc_influxdb/main.py` — `dbcInflux` class; the public API (`download`,
  `upload_singlevar`, `delete`, and `show_*` inspection methods).
- `dbc_influxdb/db.py` — InfluxDB client/query/delete API helpers.
- `dbc_influxdb/fluxql.py` — Flux query construction.
- `dbc_influxdb/common.py` — shared helpers (tags, timezone conversion via `pytz`).
- `dbc_influxdb/varscanner.py`, `manual_run.py` — supporting modules.
- `tests/test_dbc.py` — tests. Note `test_db_connection` requires an external
  config directory and a live InfluxDB instance, so it fails in environments
  without them; this is expected and unrelated to code changes.
- `notebooks/` — example download/delete workflows.

## Conventions

- All data are stored in UTC in the database. Methods take a
  `timezone_offset_to_utc_hours` argument controlling the timezone of input
  dates and returned timestamps.
- Record notable changes in `CHANGELOG.md` (newest entry at the top, version +
  date heading) and keep the `version` in `pyproject.toml` in sync.

## Git

- **Never run `git commit` or `git push`.** Make file changes and, when asked,
  provide the commit message as text only — the user commits themselves.
- **Never add a `Co-Authored-By: Claude` trailer** (or any Claude attribution)
  to commit messages or PRs.
