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
        (5.5, "+05:30"),
        (-3.5, "-03:30"),
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

    def test_fractional_offset(self):
        s = pd.Series(pd.to_datetime(["2022-01-01 00:00:00"]).tz_localize("UTC"))
        result = convert_ts_to_timezone(timezone_offset_to_utc_hours=5.5, timestamp_index=s)
        assert result.iloc[0].utcoffset() == dt.timedelta(hours=5, minutes=30)

    def test_shifts_clock_value(self):
        # offset +1 must move a 12:00 UTC timestamp to 13:00 local
        s = pd.Series(pd.to_datetime(["2022-01-01 12:00:00"]).tz_localize("UTC"))
        result = convert_ts_to_timezone(timezone_offset_to_utc_hours=1, timestamp_index=s)
        assert result.iloc[0].hour == 13


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


def _bare_dbc():
    """Create a dbcInflux instance without running __init__ (no DB connection)."""
    return dbcInflux.__new__(dbcInflux)


class TestDownloadEmptyResult:
    def test_empty_result_returns_empty(self, monkeypatch):
        dbc = _bare_dbc()
        # query returns an empty DataFrame (no data in range)
        monkeypatch.setattr(dbc, "_query_df", lambda query: pd.DataFrame())
        data_simple, data_detailed, assigned = dbc.download(
            bucket="b",
            start="2022-01-01 00:00:00",
            stop="2022-01-02 00:00:00",
            timezone_offset_to_utc_hours=1,
        )
        assert data_simple.empty
        assert data_detailed == {}
        assert assigned == {}


class TestDeleteValidation:
    @pytest.mark.parametrize("measurements,fields", [
        (None, True),
        (False, True),
        ([], True),
        (True, None),
        (True, False),
        (True, []),
    ])
    def test_invalid_inputs_raise(self, measurements, fields):
        dbc = _bare_dbc()
        with pytest.raises(ValueError):
            dbc.delete(
                bucket="b",
                measurements=measurements,
                start="2022-01-01 00:00:00",
                stop="2022-01-02 00:00:00",
                timezone_offset_to_utc_hours=1,
                data_version="raw",
                fields=fields,
            )


class TestUploadValidation:
    def test_missing_tag_columns_raises(self):
        dbc = _bare_dbc()
        df = pd.DataFrame({"TA": [1.0]}, index=pd.to_datetime(["2022-01-01"]))
        with pytest.raises(ValueError):
            dbc.upload_singlevar(df, to_bucket="b", to_measurement="TA",
                                 timezone_offset_to_utc_hours=1)

    def test_multiple_fields_raises(self):
        dbc = _bare_dbc()
        data = {t: ["x"] for t in tags}
        data["TA"] = [1.0]
        data["SW"] = [2.0]
        df = pd.DataFrame(data, index=pd.to_datetime(["2022-01-01"]))
        with pytest.raises(ValueError):
            dbc.upload_singlevar(df, to_bucket="b", to_measurement="TA",
                                 timezone_offset_to_utc_hours=1)
