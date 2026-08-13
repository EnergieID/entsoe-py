"""Falsification lab for candidate C-01 (run 2026-08-12a).

Riskiest assumption under test: `year_limited` silently drops the chunk-start
instant of every non-first yearly block when the underlying query returns data
on a half-open interval [start, end) — i.e. the ENTSO-E API's exclusive `end`,
as reported in EnergieID/entsoe-py#531 and #536.

The fake query functions below stand in for the API layer only; the decorator
under test is the real production code imported from `entsoe.decorators`.
"""
import pandas as pd
import pytest

from entsoe.decorators import year_limited


def _hourly(start: pd.Timestamp, end: pd.Timestamp, inclusive: str) -> pd.Series:
    idx = pd.date_range(start, end, freq="h", inclusive=inclusive)
    return pd.Series(1.0, index=idx)


@year_limited
def query_end_exclusive(*, start=None, end=None):
    """Mimics the reported API behaviour: returns [start, end)."""
    return _hourly(start, end, inclusive="left")


@year_limited
def query_end_inclusive(*, start=None, end=None):
    """Mimics an end-inclusive backend: returns [start, end].

    Guards the original intent of the mask: no duplicated boundary records.
    """
    return _hourly(start, end, inclusive="both")


TZ = "Europe/Brussels"
START = pd.Timestamp("2020-07-05 00:00", tz=TZ)
END = pd.Timestamp("2026-07-05 00:00", tz=TZ)
EXPECTED = pd.date_range(START, END, freq="h", inclusive="left")


def test_no_hour_dropped_at_year_boundaries():
    """Fails on master: one hour vanishes at every yearly chunk boundary."""
    result = query_end_exclusive(start=START, end=END)
    missing = EXPECTED.difference(result.index)
    assert missing.empty, (
        f"{len(missing)} boundary instants silently dropped: {list(missing)}"
    )


def test_no_duplicates_with_end_inclusive_backend():
    """The historical reason for the mask: boundary rows must not repeat."""
    result = query_end_inclusive(start=START, end=END)
    dup = result.index[result.index.duplicated()]
    assert dup.empty, f"duplicated boundary instants: {list(dup.unique())}"


def test_full_series_shape_end_exclusive():
    result = query_end_exclusive(start=START, end=END)
    assert len(result) == len(EXPECTED)
    assert result.index.equals(EXPECTED)
