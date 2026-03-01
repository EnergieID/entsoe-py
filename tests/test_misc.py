"""Tests for entsoe.misc time block utilities.

Covers:
- Unit tests for year_blocks, month_blocks, day_blocks (Task 7.1)
- Property test for time block contiguity and coverage (Task 7.2, Property 14)
- Property test for time block timezone preservation (Task 7.3, Property 15)
"""

import pandas as pd
import pytz
from hypothesis import given, settings
from hypothesis import strategies as st

from entsoe.misc import day_blocks, month_blocks, year_blocks


# ---------------------------------------------------------------------------
# Task 7.1 — Unit tests for year_blocks, month_blocks, day_blocks
# Requirements: 12.1, 12.2, 12.3, 12.4
# ---------------------------------------------------------------------------


class TestYearBlocks:
    """Unit tests for year_blocks."""

    def test_single_unit_span_returns_one_block(self):
        """A span within a single year returns exactly one block."""
        start = pd.Timestamp("2023-03-15")
        end = pd.Timestamp("2023-09-20")
        blocks = list(year_blocks(start, end))
        assert len(blocks) == 1
        assert blocks[0] == (start, end)

    def test_multi_unit_span_returns_contiguous_blocks(self):
        """A span crossing year boundaries returns contiguous blocks."""
        start = pd.Timestamp("2021-06-01")
        end = pd.Timestamp("2023-06-01")
        blocks = list(year_blocks(start, end))
        assert len(blocks) >= 2
        # First block starts at start, last block ends at end
        assert blocks[0][0] == start
        assert blocks[-1][1] == end
        # Contiguity: each block's end == next block's start
        for i in range(len(blocks) - 1):
            assert blocks[i][1] == blocks[i + 1][0]

    def test_start_equals_end_returns_empty(self):
        """When start == end, the result is an empty sequence."""
        ts = pd.Timestamp("2023-01-01")
        blocks = list(year_blocks(ts, ts))
        assert blocks == []


class TestMonthBlocks:
    """Unit tests for month_blocks."""

    def test_single_unit_span_returns_one_block(self):
        """A span within a single month returns exactly one block."""
        start = pd.Timestamp("2023-03-05")
        end = pd.Timestamp("2023-03-25")
        blocks = list(month_blocks(start, end))
        assert len(blocks) == 1
        assert blocks[0] == (start, end)

    def test_multi_unit_span_returns_contiguous_blocks(self):
        """A span crossing month boundaries returns contiguous blocks."""
        start = pd.Timestamp("2023-01-15")
        end = pd.Timestamp("2023-04-15")
        blocks = list(month_blocks(start, end))
        assert len(blocks) >= 2
        assert blocks[0][0] == start
        assert blocks[-1][1] == end
        for i in range(len(blocks) - 1):
            assert blocks[i][1] == blocks[i + 1][0]

    def test_start_equals_end_returns_empty(self):
        """When start == end, the result is an empty sequence."""
        ts = pd.Timestamp("2023-06-01")
        blocks = list(month_blocks(ts, ts))
        assert blocks == []


class TestDayBlocks:
    """Unit tests for day_blocks."""

    def test_single_unit_span_returns_one_block(self):
        """A span within a single day returns exactly one block."""
        start = pd.Timestamp("2023-05-10 06:00")
        end = pd.Timestamp("2023-05-10 18:00")
        blocks = list(day_blocks(start, end))
        assert len(blocks) == 1
        assert blocks[0] == (start, end)

    def test_multi_unit_span_returns_contiguous_blocks(self):
        """A span crossing day boundaries returns contiguous blocks."""
        start = pd.Timestamp("2023-01-01")
        end = pd.Timestamp("2023-01-05")
        blocks = list(day_blocks(start, end))
        assert len(blocks) >= 2
        assert blocks[0][0] == start
        assert blocks[-1][1] == end
        for i in range(len(blocks) - 1):
            assert blocks[i][1] == blocks[i + 1][0]

    def test_start_equals_end_returns_empty(self):
        """When start == end, the result is an empty sequence."""
        ts = pd.Timestamp("2023-07-04")
        blocks = list(day_blocks(ts, ts))
        assert blocks == []


# ---------------------------------------------------------------------------
# Task 7.2 — Property 14: Time block contiguity and coverage
# ---------------------------------------------------------------------------

# Strategies: generate reasonable (start, end) pairs with start < end.
# Keep ranges bounded to avoid slow tests.
# rrule operates at second precision, so we must avoid sub-second timestamps
# to ensure the first generated rrule point matches start exactly.

_year_start = st.datetimes(
    min_value=pd.Timestamp("2020-01-01").to_pydatetime(),
    max_value=pd.Timestamp("2023-01-01").to_pydatetime(),
).map(lambda dt: dt.replace(microsecond=0))
_year_end_delta = st.timedeltas(
    min_value=pd.Timedelta(hours=1),
    max_value=pd.Timedelta(days=3 * 365),
)

_month_start = st.datetimes(
    min_value=pd.Timestamp("2022-01-01").to_pydatetime(),
    max_value=pd.Timestamp("2024-06-01").to_pydatetime(),
).map(lambda dt: dt.replace(microsecond=0))
_month_end_delta = st.timedeltas(
    min_value=pd.Timedelta(hours=1),
    max_value=pd.Timedelta(days=180),
)

_day_start = st.datetimes(
    min_value=pd.Timestamp("2023-01-01").to_pydatetime(),
    max_value=pd.Timestamp("2024-01-01").to_pydatetime(),
).map(lambda dt: dt.replace(microsecond=0))
_day_end_delta = st.timedeltas(
    min_value=pd.Timedelta(hours=1),
    max_value=pd.Timedelta(days=30),
)


def _assert_contiguity_and_coverage(blocks, start, end):
    """Shared assertions for contiguity and coverage properties."""
    assert len(blocks) >= 1, "Expected at least one block for start < end"
    # (a) first block starts at start
    assert blocks[0][0] == start
    # (b) last block ends at end
    assert blocks[-1][1] == end
    # (c) contiguity: each block's end == next block's start
    for i in range(len(blocks) - 1):
        assert blocks[i][1] == blocks[i + 1][0], (
            f"Gap between block {i} end={blocks[i][1]} and block {i+1} start={blocks[i+1][0]}"
        )
    # (d) no overlaps: each block start < block end
    for i, (s, e) in enumerate(blocks):
        assert s < e, f"Block {i} has start >= end: {s} >= {e}"


@given(start_dt=_year_start, delta=_year_end_delta)
@settings(max_examples=100)
def test_property_year_blocks_contiguity_and_coverage(start_dt, delta):
    start = pd.Timestamp(start_dt)
    end = pd.Timestamp(start_dt + delta)
    blocks = list(year_blocks(start, end))
    _assert_contiguity_and_coverage(blocks, start, end)


@given(start_dt=_month_start, delta=_month_end_delta)
@settings(max_examples=100)
def test_property_month_blocks_contiguity_and_coverage(start_dt, delta):
    start = pd.Timestamp(start_dt)
    end = pd.Timestamp(start_dt + delta)
    blocks = list(month_blocks(start, end))
    _assert_contiguity_and_coverage(blocks, start, end)


@given(start_dt=_day_start, delta=_day_end_delta)
@settings(max_examples=100)
def test_property_day_blocks_contiguity_and_coverage(start_dt, delta):
    start = pd.Timestamp(start_dt)
    end = pd.Timestamp(start_dt + delta)
    blocks = list(day_blocks(start, end))
    _assert_contiguity_and_coverage(blocks, start, end)


# ---------------------------------------------------------------------------
# Task 7.3 — Property 15: Time block timezone preservation
# ---------------------------------------------------------------------------

_tz_strategy = st.sampled_from([pytz.UTC, pytz.timezone("Europe/Berlin")])

_tz_start = st.datetimes(
    min_value=pd.Timestamp("2022-01-01").to_pydatetime(),
    max_value=pd.Timestamp("2024-01-01").to_pydatetime(),
).map(lambda dt: dt.replace(microsecond=0))
_tz_delta = st.timedeltas(
    min_value=pd.Timedelta(hours=1),
    max_value=pd.Timedelta(days=90),
)


@given(start_dt=_tz_start, delta=_tz_delta, tz=_tz_strategy)
@settings(max_examples=100)
def test_property_year_blocks_timezone_preservation(start_dt, delta, tz):
    start = pd.Timestamp(start_dt, tz=tz)
    end = pd.Timestamp(start_dt + delta, tz=tz)
    blocks = list(year_blocks(start, end))
    for s, e in blocks:
        assert s.tzinfo is not None, f"Block start {s} lost timezone"
        assert e.tzinfo is not None, f"Block end {e} lost timezone"
        assert str(s.tz) == str(start.tz)
        assert str(e.tz) == str(start.tz)


@given(start_dt=_tz_start, delta=_tz_delta, tz=_tz_strategy)
@settings(max_examples=100)
def test_property_month_blocks_timezone_preservation(start_dt, delta, tz):
    start = pd.Timestamp(start_dt, tz=tz)
    end = pd.Timestamp(start_dt + delta, tz=tz)
    blocks = list(month_blocks(start, end))
    for s, e in blocks:
        assert s.tzinfo is not None, f"Block start {s} lost timezone"
        assert e.tzinfo is not None, f"Block end {e} lost timezone"
        assert str(s.tz) == str(start.tz)
        assert str(e.tz) == str(start.tz)


@given(start_dt=_tz_start, delta=_tz_delta, tz=_tz_strategy)
@settings(max_examples=100)
def test_property_day_blocks_timezone_preservation(start_dt, delta, tz):
    start = pd.Timestamp(start_dt, tz=tz)
    end = pd.Timestamp(start_dt + delta, tz=tz)
    blocks = list(day_blocks(start, end))
    for s, e in blocks:
        assert s.tzinfo is not None, f"Block start {s} lost timezone"
        assert e.tzinfo is not None, f"Block end {e} lost timezone"
        assert str(s.tz) == str(start.tz)
        assert str(e.tz) == str(start.tz)
