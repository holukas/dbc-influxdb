"""Optional terminal UI (TUI) for dbc-influxdb, built with Textual.

Provides an interactive way to browse a bucket's measurements/fields and to
**download** or **delete** time-series data, without writing a script.

Importing this module pulls in Textual but does not require a database
connection or config; the ``dbcInflux`` client is only created in
:func:`main`.

Run it::

    dbc-influxdb-tui --dirconf path/to/configs
    # or
    uv run --extra tui python -m dbc_influxdb.tui --dirconf path/to/configs

The selection cascade (bucket -> measurements -> fields) is driven by the
existing inspection methods (:meth:`dbcInflux.show_buckets`,
:meth:`dbcInflux.show_measurements_in_bucket`,
:meth:`dbcInflux.show_fields_in_measurement`). All database calls run in worker
threads so the UI never blocks.

Safety: delete is irreversible, so it always shows the matched scope and
requires explicit confirmation in a modal before anything is deleted.
"""
from __future__ import annotations

from pathlib import Path

from textual import work
from textual.app import App, ComposeResult
from textual.containers import Grid, Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Label,
    RichLog,
    Select,
    SelectionList,
    Static,
)


def _split_csv(raw: str) -> list[str]:
    """Split a comma-separated input into a clean list of non-empty items."""
    return [part.strip() for part in raw.split(",") if part.strip()]


def _parse_offset(raw: str) -> int | float:
    """Parse the timezone-offset input, preferring int then float; default 1."""
    raw = raw.strip()
    if not raw:
        return 1
    try:
        return int(raw)
    except ValueError:
        return float(raw)


class ConfirmDelete(ModalScreen[bool]):
    """Modal that shows the delete scope and asks for explicit confirmation."""

    DEFAULT_CSS = """
    ConfirmDelete {
        align: center middle;
    }
    ConfirmDelete > Grid {
        grid-size: 2;
        grid-gutter: 1 2;
        grid-rows: 1fr 3;
        padding: 1 2;
        width: 70;
        height: auto;
        border: thick $error 80%;
        background: $surface;
    }
    ConfirmDelete #question {
        column-span: 2;
        height: auto;
        content-align: left top;
    }
    ConfirmDelete Button {
        width: 100%;
    }
    """

    def __init__(self, summary: str) -> None:
        super().__init__()
        self._summary = summary

    def compose(self) -> ComposeResult:
        yield Grid(
            Static(
                f"This will permanently DELETE data and cannot be undone.\n\n{self._summary}",
                id="question",
            ),
            Button("Cancel", variant="primary", id="cancel"),
            Button("Delete", variant="error", id="confirm"),
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm")


class DbcTui(App):
    """Interactive download/delete front-end for a :class:`dbcInflux` instance."""

    CSS = """
    #panes {
        height: 1fr;
    }
    #selection {
        width: 45%;
        border: round $primary;
        padding: 0 1;
    }
    #params {
        width: 55%;
        border: round $primary;
        padding: 0 1;
    }
    .heading {
        text-style: bold;
        color: $secondary;
        margin: 1 0 0 0;
    }
    .hint {
        color: $text-muted;
    }
    SelectionList {
        height: 1fr;
        border: round $panel;
    }
    Input {
        margin: 0 0 1 0;
    }
    #actions {
        height: auto;
        margin: 1 0;
    }
    #actions Button {
        margin: 0 1 0 0;
    }
    #log {
        height: 12;
        border: round $panel;
    }
    """

    BINDINGS = [
        ("d", "download", "Download"),
        ("x", "delete", "Delete"),
        ("r", "refresh_buckets", "Reload buckets"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, dbc, *, demo: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.dbc = dbc
        self._demo = demo

    # ------------------------------------------------------------------ layout
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="panes"):
            with Vertical(id="selection"):
                yield Static("Bucket", classes="heading")
                yield Select([], prompt="Select a bucket…", id="bucket")
                yield Static("Measurements (none = all)", classes="heading")
                yield SelectionList(id="measurements")
                yield Static("Fields (none = all)", classes="heading")
                yield SelectionList(id="fields")
            with VerticalScroll(id="params"):
                yield Static("Parameters", classes="heading")
                yield Label("Start (e.g. 2022-07-04 00:30:00)")
                yield Input(placeholder="YYYY-MM-DD HH:MM:SS", id="start")
                yield Label("Stop (exclusive)")
                yield Input(placeholder="YYYY-MM-DD HH:MM:SS", id="stop")
                yield Label("Data version(s), comma-separated")
                yield Input(placeholder="e.g. meteoscreening_diive", id="data_version")
                yield Label("Timezone offset to UTC (hours)")
                yield Input(value="1", id="offset")
                yield Label("Save downloaded CSV to (optional)")
                yield Input(placeholder="path/to/output.csv", id="csv_path")
                with Horizontal(id="actions"):
                    yield Button("Download", variant="success", id="btn-download")
                    yield Button("Delete", variant="error", id="btn-delete")
                yield Static("Output", classes="heading")
                yield RichLog(id="log", markup=True, highlight=True, wrap=True)
        yield Footer()

    def on_mount(self) -> None:
        self.title = "dbc-influxdb"
        self.sub_title = "DEMO — sample data, no database" if self._demo else "download / delete"
        if self._demo:
            self._log(
                "[yellow]DEMO mode: built-in sample data, no database connection. "
                "Download/Delete are simulated.[/]"
            )
        self._log("[dim]Loading buckets…[/]")
        self._load_buckets()

    # --------------------------------------------------------------- utilities
    def _log(self, message: str) -> None:
        self.query_one("#log", RichLog).write(message)

    @property
    def _bucket(self) -> str | None:
        # A real selection is the bucket name (a str); "no selection" is a
        # Select sentinel (Select.BLANK / Select.NULL depending on version).
        value = self.query_one("#bucket", Select).value
        return value if isinstance(value, str) else None

    # ------------------------------------------------------------- data loading
    @work(thread=True, exclusive=True, group="buckets")
    def _load_buckets(self) -> None:
        try:
            buckets = self.dbc.show_buckets()
        except Exception as exc:  # noqa: BLE001 - surface any DB error to the UI
            self.call_from_thread(self._log, f"[red]Could not load buckets: {exc}[/]")
            return
        self.call_from_thread(self._set_buckets, buckets)

    def _set_buckets(self, buckets: list[str]) -> None:
        self.query_one("#bucket", Select).set_options((b, b) for b in buckets)
        self._log(f"[green]Loaded {len(buckets)} buckets.[/]")

    @work(thread=True, exclusive=True, group="measurements")
    def _load_measurements(self, bucket: str) -> None:
        try:
            measurements = self.dbc.show_measurements_in_bucket(bucket=bucket, verbose=False)
        except Exception as exc:  # noqa: BLE001
            self.call_from_thread(self._log, f"[red]Could not load measurements: {exc}[/]")
            return
        self.call_from_thread(self._set_measurements, measurements)

    def _set_measurements(self, measurements: list[str]) -> None:
        ml = self.query_one("#measurements", SelectionList)
        ml.clear_options()
        ml.add_options((m, m) for m in measurements)
        self.query_one("#fields", SelectionList).clear_options()
        self._log(f"[green]Loaded {len(measurements)} measurements.[/]")

    @work(thread=True, exclusive=True, group="fields")
    def _load_fields(self, bucket: str, measurements: list[str]) -> None:
        fields: set[str] = set()
        try:
            for measurement in measurements:
                fields.update(
                    self.dbc.show_fields_in_measurement(
                        bucket=bucket, measurement=measurement, verbose=False
                    )
                )
        except Exception as exc:  # noqa: BLE001
            self.call_from_thread(self._log, f"[red]Could not load fields: {exc}[/]")
            return
        self.call_from_thread(self._set_fields, sorted(fields))

    def _set_fields(self, fields: list[str]) -> None:
        fl = self.query_one("#fields", SelectionList)
        fl.clear_options()
        fl.add_options((f, f) for f in fields)
        self._log(f"[green]Loaded {len(fields)} fields.[/]")

    # ------------------------------------------------------------------ events
    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id != "bucket":
            return
        self.query_one("#measurements", SelectionList).clear_options()
        self.query_one("#fields", SelectionList).clear_options()
        if isinstance(event.value, str):
            self._load_measurements(event.value)

    def on_selection_list_selected_changed(
        self, event: SelectionList.SelectedChanged
    ) -> None:
        if event.selection_list.id != "measurements":
            return
        bucket = self._bucket
        selected = list(event.selection_list.selected)
        if bucket and selected:
            self._load_fields(bucket, selected)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-download":
            self.action_download()
        elif event.button.id == "btn-delete":
            self.action_delete()

    # ----------------------------------------------------------------- actions
    def _common_params(self) -> dict | None:
        bucket = self._bucket
        if not bucket:
            self._log("[red]Select a bucket first.[/]")
            return None
        start = self.query_one("#start", Input).value.strip()
        stop = self.query_one("#stop", Input).value.strip()
        if not start or not stop:
            self._log("[red]Start and stop dates are required.[/]")
            return None
        return {
            "bucket": bucket,
            "start": start,
            "stop": stop,
            "offset": _parse_offset(self.query_one("#offset", Input).value),
            "measurements": list(self.query_one("#measurements", SelectionList).selected),
            "fields": list(self.query_one("#fields", SelectionList).selected),
            "data_version": _split_csv(self.query_one("#data_version", Input).value),
        }

    def action_refresh_buckets(self) -> None:
        self._log("[dim]Reloading buckets…[/]")
        self._load_buckets()

    def action_download(self) -> None:
        params = self._common_params()
        if params is None:
            return
        csv_path = self.query_one("#csv_path", Input).value.strip() or None
        self._log(
            f"[cyan]Downloading from [b]{params['bucket']}[/b] "
            f"between {params['start']} and {params['stop']}…[/]"
        )
        self._do_download(params, csv_path)

    @work(thread=True, exclusive=True, group="action")
    def _do_download(self, params: dict, csv_path: str | None) -> None:
        try:
            data_simple, data_detailed, _ = self.dbc.download(
                bucket=params["bucket"],
                start=params["start"],
                stop=params["stop"],
                timezone_offset_to_utc_hours=params["offset"],
                measurements=params["measurements"] or None,
                fields=params["fields"] or None,
                data_version=params["data_version"] or None,
            )
        except Exception as exc:  # noqa: BLE001
            self.call_from_thread(self._log, f"[red]Download failed: {exc}[/]")
            return

        msg = (
            f"[green]Downloaded {len(data_detailed)} variable(s), "
            f"{len(data_simple)} row(s).[/]"
        )
        if csv_path:
            try:
                data_simple.to_csv(csv_path)
                msg += f"\n[green]Saved CSV -> {csv_path}[/]"
            except Exception as exc:  # noqa: BLE001
                msg += f"\n[red]Could not save CSV: {exc}[/]"
        self.call_from_thread(self._log, msg)

    def action_delete(self) -> None:
        params = self._common_params()
        if params is None:
            return
        data_version = params["data_version"]
        if len(data_version) != 1:
            self._log("[red]Delete requires exactly one data version.[/]")
            return

        measurements = params["measurements"] or True
        fields = params["fields"] or True
        m_str = "ALL" if measurements is True else ", ".join(measurements)
        f_str = "ALL" if fields is True else ", ".join(fields)
        summary = (
            f"Bucket:        {params['bucket']}\n"
            f"Measurements:  {m_str}\n"
            f"Fields:        {f_str}\n"
            f"Data version:  {data_version[0]}\n"
            f"Range:         {params['start']}  ->  {params['stop']}\n"
            f"TZ offset:     {params['offset']}"
        )

        def _on_confirm(confirmed: bool | None) -> None:
            if confirmed:
                self._log("[yellow]Deleting…[/]")
                self._do_delete(params, measurements, fields, data_version[0])
            else:
                self._log("[dim]Delete cancelled.[/]")

        self.push_screen(ConfirmDelete(summary), _on_confirm)

    @work(thread=True, exclusive=True, group="action")
    def _do_delete(self, params: dict, measurements, fields, data_version: str) -> None:
        try:
            self.dbc.delete(
                bucket=params["bucket"],
                measurements=measurements,
                fields=fields,
                start=params["start"],
                stop=params["stop"],
                timezone_offset_to_utc_hours=params["offset"],
                data_version=data_version,
            )
        except Exception as exc:  # noqa: BLE001
            self.call_from_thread(self._log, f"[red]Delete failed: {exc}[/]")
            return
        self.call_from_thread(self._log, "[green]Delete finished.[/]")


class _DemoDbc:
    """In-memory stand-in for :class:`dbcInflux` used by ``--demo``.

    Implements the handful of methods the TUI calls, backed by hard-coded sample
    data, so the interface can be explored without any config files or database
    connection. Download/Delete are simulated (small delay, fabricated result).
    """

    _DATA = {
        "ch-dav_raw": {
            "TA": ["TA_T1_1_1", "TA_T1_2_1"],
            "SW": ["SW_IN_T1_1_1", "SW_OUT_T1_1_1"],
        },
        "ch-dav_processed": {
            "TA": ["TA_T1_1_1"],
            "SWC": ["SWC_T1_1_1", "SWC_T1_2_1", "SWC_T2_1_1"],
        },
        "ch-cha_processed": {
            "TA": ["TA_T1_1_1"],
            "SW": ["SW_IN_T1_1_1"],
        },
    }

    def show_buckets(self) -> list:
        import time

        time.sleep(0.3)  # mimic a little network latency so the UI feels real
        return list(self._DATA)

    def show_measurements_in_bucket(self, bucket: str, verbose: bool = True) -> list:
        import time

        time.sleep(0.2)
        return list(self._DATA.get(bucket, {}))

    def show_fields_in_measurement(self, bucket: str, measurement: str,
                                   verbose: bool = True) -> list:
        import time

        time.sleep(0.1)
        return list(self._DATA.get(bucket, {}).get(measurement, []))

    def download(self, *, bucket, start, stop, timezone_offset_to_utc_hours,
                 measurements=None, fields=None, data_version=None):
        import time

        import pandas as pd

        time.sleep(0.6)
        cols = list(fields) if fields else ["TA_T1_1_1", "SW_IN_T1_1_1"]
        index = pd.date_range(start=start, periods=48, freq="30min", name="TIMESTAMP_END")
        data_simple = pd.DataFrame({c: range(len(index)) for c in cols}, index=index)
        data_detailed = {c: data_simple[[c]].copy() for c in cols}
        first_measurement = measurements[0] if measurements else "TA"
        assigned = {c: first_measurement for c in cols}
        return data_simple, data_detailed, assigned

    def delete(self, **kwargs) -> None:
        import time

        time.sleep(0.6)
        return None


def main() -> None:
    """Console-script entry point (``dbc-influxdb-tui``)."""
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Interactive TUI for dbc-influxdb.")
    parser.add_argument(
        "--dirconf",
        default=os.environ.get("DBC_DIRCONF"),
        help="Path to the dbc-influxdb config directory "
        "(or set the DBC_DIRCONF environment variable).",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Launch with built-in sample data; no config or database connection "
        "required (download/delete are simulated). Use it just to see the UI.",
    )
    args = parser.parse_args()

    if args.demo:
        DbcTui(_DemoDbc(), demo=True).run()
        return

    if not args.dirconf:
        parser.error("--dirconf is required (or set DBC_DIRCONF), unless --demo is given.")
    if not Path(args.dirconf).exists():
        parser.error(f"Config directory not found: {args.dirconf}")

    # Imported here so that importing this module does not require a DB/config.
    from dbc_influxdb.main import dbcInflux

    dbc = dbcInflux(dirconf=args.dirconf)
    DbcTui(dbc).run()


if __name__ == "__main__":
    main()
