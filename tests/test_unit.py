"""Unit tests for pure logic that does not require a database connection."""
import datetime as dt

import pandas as pd
import pytest

from dbc_influxdb import fluxql
from dbc_influxdb.common import convert_ts_to_timezone, tags
from dbc_influxdb.main import dbcInflux


class TestUtcOffsetFormatting:
    @pytest.mark.parametrize("hours,expected", [
        (0, "+00:00"),
        (1, "+01:00"),
        (10, "+10:00"),
        (-1, "-01:00"),
        (-5, "-05:00"),
    ])
    def test_format_utc_offset(self, hours, expected):
        assert dbcInflux._format_utc_offset(hours) == expected

    def test_convert_datestr_positive_offset(self):
        result = dbcInflux._convert_datestr_to_iso8601("2022-05-27 00:00:00", 1)
        assert result == "2022-05-27T00:00:00+01:00"

    def test_convert_datestr_negative_offset(self):
        result = dbcInflux._convert_datestr_to_iso8601("2022-05-27 00:00:00", -5)
        assert result == "2022-05-27T00:00:00-05:00"


class TestConvertTsToTimezone:
    def test_positive_offset(self):
        s = pd.Series(pd.to_datetime(["2022-01-01 00:00:00"]).tz_localize("UTC"))
        result = convert_ts_to_timezone(timezone_offset_to_utc_hours=1, timestamp_index=s)
        assert result.iloc[0].utcoffset() == dt.timedelta(hours=1)

    def test_negative_offset(self):
        s = pd.Series(pd.to_datetime(["2022-01-01 00:00:00"]).tz_localize("UTC"))
        result = convert_ts_to_timezone(timezone_offset_to_utc_hours=-5, timestamp_index=s)
        assert result.iloc[0].utcoffset() == dt.timedelta(hours=-5)


class TestFluxQL:
    def test_bucketstring(self):
        assert fluxql.bucketstring("mybucket") == 'from(bucket: "mybucket")'

    def test_rangestring(self):
        assert fluxql.rangestring("A", "B") == "|> range(start: A, stop: B)"

    def test_filterstring_single(self):
        assert (fluxql.filterstring("_field", ["x"], "or")
                == '|> filter(fn: (r) => r["_field"] == "x")')

    def test_filterstring_multiple_or(self):
        expected = '|> filter(fn: (r) => r["_field"] == "x" or r["_field"] == "y")'
        assert fluxql.filterstring("_field", ["x", "y"], "or") == expected

    def test_pivotstring(self):
        assert fluxql.pivotstring() == (
            '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        )


class TestVerifyFreq:
    def test_match_logs_no_warning(self, caplog):
        idx = pd.date_range("2022-01-01", periods=5, freq="30min")
        dbcInflux._verify_freq(data_index=idx, expected_freq="30min")
        assert not [r for r in caplog.records if r.levelname == "WARNING"]

    def test_mismatch_warns(self, caplog):
        idx = pd.date_range("2022-01-01", periods=5, freq="30min")
        with caplog.at_level("WARNING"):
            dbcInflux._verify_freq(data_index=idx, expected_freq="10min")
        assert any(r.levelname == "WARNING" for r in caplog.records)


def test_tags_is_list_of_str():
    assert isinstance(tags, list)
    assert all(isinstance(t, str) for t in tags)
    assert "varname" in tags
