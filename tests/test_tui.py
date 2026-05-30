"""Smoke tests for the optional Textual TUI.

These run fully headless (Textual's ``run_test`` harness) against a stub that
mimics the ``dbcInflux`` inspection/download/delete methods, so no database is
required. Skipped automatically if the optional ``textual`` dependency is not
installed.
"""
import asyncio

import pandas as pd
import pytest

pytest.importorskip("textual")

from dbc_influxdb.tui import DbcTui, _parse_offset, _split_csv  # noqa: E402


class StubDbc:
    """Minimal stand-in for dbcInflux used by the TUI."""

    def show_buckets(self):
        return ["b1", "b2", "b3"]

    def show_measurements_in_bucket(self, bucket, verbose=True):
        return ["TA", "SW"]

    def show_fields_in_measurement(self, bucket, measurement, verbose=True):
        return [f"{measurement}_T1_1_1"]

    def download(self, **kwargs):
        return pd.DataFrame(), {}, {}

    def delete(self, **kwargs):
        return None


def test_split_csv():
    assert _split_csv("a, b ,,c ") == ["a", "b", "c"]
    assert _split_csv("") == []


def test_parse_offset():
    assert _parse_offset("") == 1
    assert _parse_offset("2") == 2
    assert _parse_offset("5.5") == 5.5


def _richlog_text(app) -> str:
    from textual.widgets import RichLog

    rich_log = app.query_one("#log", RichLog)
    return "\n".join(strip.text for strip in rich_log.lines)


async def _smoke() -> None:
    app = DbcTui(StubDbc())
    async with app.run_test() as pilot:
        # Wait for the on_mount buckets worker to populate the UI.
        for _ in range(50):
            await pilot.pause()
            if "Loaded 3 buckets" in _richlog_text(app):
                break
        assert "Loaded 3 buckets" in _richlog_text(app)

        # Download with no bucket selected must surface a validation message
        # (synchronous path, no DB call).
        app.action_download()
        await pilot.pause()
        assert "Select a bucket first" in _richlog_text(app)


def test_app_smoke():
    asyncio.run(_smoke())


def test_demo_backend():
    from dbc_influxdb.tui import _DemoDbc

    demo = _DemoDbc()
    assert demo.show_buckets()
    assert demo.show_measurements_in_bucket("ch-dav_raw")
    data_simple, data_detailed, assigned = demo.download(
        bucket="ch-dav_raw",
        start="2022-07-04 00:30:00",
        stop="2022-07-05 00:30:00",
        timezone_offset_to_utc_hours=1,
        fields=["TA_T1_1_1"],
    )
    assert len(data_simple) == 48
    assert list(data_detailed) == ["TA_T1_1_1"]
    assert assigned == {"TA_T1_1_1": "TA"}
