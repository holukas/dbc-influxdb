# Configuration layout

`dbcInflux(dirconf=...)` expects a configuration directory plus a **sibling**
secret directory:

```
<dirconf>/
    dirs.yaml             # directory settings (read and exposed, otherwise passthrough)
    units.yaml            # mapping: raw unit string -> standardized unit string
    filegroups/
        *.yaml            # one YAML file per filetype; each file has a single top-level key
<dirconf>_secret/         # SIBLING of <dirconf> (note the "_secret" suffix), NOT inside it
    dbconf.yaml           # InfluxDB connection settings
```

For example, if `dirconf = ".../POET/configs"`, then the database connection
file is read from `.../POET/configs_secret/dbconf.yaml`.

Keeping `dbconf.yaml` in a separate `_secret` directory makes it easy to keep
the connection token out of a version-controlled config directory.

The files in this folder are **templates** — copy them to your own config
location and fill in real values.
