"""
Offline unit tests for entsoe.series_parsers.

These tests run without network access or API credentials. Fixtures in
tests/fixtures/ are minimal synthetic XML documents constructed to match
the tag structures the parsers expect — not real ENTSO-E responses.
"""
import bs4
import pandas as pd
import pytest

from entsoe.series_parsers import (
    _extract_timeseries,
    _parse_datetimeindex,
    _parse_timeseries_generic,
    _resolution_to_timedelta,
)


# --- _resolution_to_timedelta --------------------------------------------
# Issue #516 was an unknown resolution string hitting the hardcoded dict
# and raising at runtime. Pinning every known key means schema additions
# surface here first.

@pytest.mark.parametrize(
    "resolution,expected",
    [
        ("PT60M", "60min"),
        ("P1Y", "12MS"),
        ("PT15M", "15min"),
        ("PT30M", "30min"),
        ("P1D", "1D"),
        ("P7D", "7D"),
        ("P1M", "1MS"),
        ("PT1M", "1min"),
    ],
)
def test_resolution_to_timedelta_known(resolution, expected):
    assert _resolution_to_timedelta(resolution) == expected


def test_resolution_to_timedelta_unknown_raises():
    with pytest.raises(NotImplementedError, match="PT4S"):
        _resolution_to_timedelta("PT4S")


# --- _extract_timeseries -------------------------------------------------

def test_extract_timeseries_empty_string():
    assert list(_extract_timeseries("")) == []


def test_extract_timeseries_none():
    assert list(_extract_timeseries(None)) == []


def test_extract_timeseries_no_matches():
    assert list(_extract_timeseries("<?xml?><doc><other/></doc>")) == []


def test_extract_timeseries_yields_each_tag(load_fixture):
    xml = load_fixture("prices.xml")
    result = list(_extract_timeseries(xml))
    assert len(result) == 2
    assert all(isinstance(ts, bs4.element.Tag) for ts in result)
    assert all(ts.name == "timeseries" for ts in result)


# --- _parse_datetimeindex ------------------------------------------------

def test_parse_datetimeindex_basic(load_fixture):
    xml = load_fixture("loads.xml")
    soup = next(_extract_timeseries(xml))

    idx = _parse_datetimeindex(soup)

    assert isinstance(idx, pd.DatetimeIndex)
    assert len(idx) == 3  # inclusive='left': 00:00, 01:00, 02:00
    assert idx[0] == pd.Timestamp("2026-01-01T00:00Z")
    assert idx[-1] == pd.Timestamp("2026-01-01T02:00Z")
    assert str(idx.tz) == "UTC"


def test_parse_datetimeindex_tz_conversion(load_fixture):
    xml = load_fixture("loads.xml")
    soup = next(_extract_timeseries(xml))

    idx = _parse_datetimeindex(soup, tz="Europe/Brussels")

    # Final step of tz path converts back to UTC regardless of input tz.
    assert str(idx.tz) == "UTC"
    assert len(idx) == 3


# --- _parse_timeseries_generic -------------------------------------------

def test_parse_timeseries_generic_returns_freq_dict(load_fixture):
    xml = load_fixture("loads.xml")
    soup = next(_extract_timeseries(xml))

    result = _parse_timeseries_generic(soup)

    assert set(result.keys()) >= {"15min", "30min", "60min"}
    assert result["15min"] is None
    assert result["30min"] is None

    s60 = result["60min"]
    assert isinstance(s60, pd.Series)
    assert len(s60) == 3
    assert s60.dtype == float
    assert list(s60.values) == [5000.0, 5100.0, 5200.0]


def test_parse_timeseries_generic_merge_series(load_fixture):
    xml = load_fixture("loads.xml")
    soup = next(_extract_timeseries(xml))

    result = _parse_timeseries_generic(soup, merge_series=True)

    assert isinstance(result, pd.Series)
    assert len(result) == 3
    assert result.iloc[0] == 5000.0


def test_parse_timeseries_generic_a03_forward_fills_gaps(load_fixture):
    """
    Curvetype A03 permits omitting a <point> when the value is unchanged
    from the previous position. The parser must reindex onto the full
    range and forward-fill.
    """
    xml = load_fixture("a03_gaps.xml")
    soup = next(_extract_timeseries(xml))

    result = _parse_timeseries_generic(soup, merge_series=True)

    # Fixture gives positions 1 and 3 over a 4-hour window.
    # Expected: [100, 100, 200, 200] after forward-fill.
    assert len(result) == 4
    assert list(result.values) == [100.0, 100.0, 200.0, 200.0]
