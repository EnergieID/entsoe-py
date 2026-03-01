"""
Tests for entsoe.decorators module.

Covers: retry, paginated, year_limited, day_limited, documents_limited decorators.
Uses real exception types and realistic failure sequences — no mock-the-return patterns.
"""

import pytest
import pandas as pd
from socket import gaierror
from http.client import RemoteDisconnected
import requests
from hypothesis import given, settings
from hypothesis import strategies as st

from entsoe.decorators import (
    retry,
    paginated,
    year_limited,
    day_limited,
    documents_limited,
)
from entsoe.exceptions import NoMatchingDataError, PaginationError
from entsoe.misc import year_blocks, day_blocks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeClient:
    """Minimal stand-in for the 'self' argument expected by retry decorator."""

    def __init__(self, retry_count, retry_delay=0):
        self.retry_count = retry_count
        self.retry_delay = retry_delay


# ===========================================================================
# 9.1  Unit tests for retry decorator
# ===========================================================================


class TestRetryDecorator:
    """Requirements 13.1–13.6"""

    def test_success_on_first_call(self):
        """13.1 — success on first call returns result without retrying."""
        call_count = 0

        @retry
        def fn(self_arg):
            nonlocal call_count
            call_count += 1
            return "ok"

        result = fn(FakeClient(retry_count=3))
        assert result == "ok"
        assert call_count == 1

    def test_connection_error_retries(self):
        """13.2 — requests.ConnectionError retries up to retry_count."""
        call_count = 0

        @retry
        def fn(self_arg):
            nonlocal call_count
            call_count += 1
            raise requests.ConnectionError("conn err")

        client = FakeClient(retry_count=4)
        with pytest.raises(requests.ConnectionError):
            fn(client)
        assert call_count == 4

    def test_gaierror_retries(self):
        """13.3 — gaierror retries using same logic."""
        call_count = 0

        @retry
        def fn(self_arg):
            nonlocal call_count
            call_count += 1
            raise gaierror("dns fail")

        client = FakeClient(retry_count=3)
        with pytest.raises(gaierror):
            fn(client)
        assert call_count == 3

    def test_remote_disconnected_retries(self):
        """13.4 — RemoteDisconnected retries using same logic."""
        call_count = 0

        @retry
        def fn(self_arg):
            nonlocal call_count
            call_count += 1
            raise RemoteDisconnected("disconnected")

        client = FakeClient(retry_count=2)
        with pytest.raises(RemoteDisconnected):
            fn(client)
        assert call_count == 2

    def test_all_retries_exhausted_raises_last_exception(self):
        """13.5 — all retries exhausted raises the last exception."""
        errors = []

        @retry
        def fn(self_arg):
            e = requests.ConnectionError(f"attempt {len(errors)}")
            errors.append(e)
            raise e

        client = FakeClient(retry_count=3)
        with pytest.raises(requests.ConnectionError, match="attempt 2"):
            fn(client)

    def test_non_connection_exception_propagates_immediately(self):
        """13.6 — non-connection exception propagates without retry."""
        call_count = 0

        @retry
        def fn(self_arg):
            nonlocal call_count
            call_count += 1
            raise ValueError("bad value")

        client = FakeClient(retry_count=5)
        with pytest.raises(ValueError, match="bad value"):
            fn(client)
        assert call_count == 1


# ===========================================================================
# 9.2  Property test — retry on connection errors
# ===========================================================================

class TestRetryConnectionErrorProperty:

    @given(
        exc_type=st.sampled_from(
            [requests.ConnectionError, gaierror, RemoteDisconnected]
        ),
        retry_count=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=100)
    def test_retries_exactly_n_times(self, exc_type, retry_count):
        call_count = 0

        @retry
        def fn(self_arg):
            nonlocal call_count
            call_count += 1
            raise exc_type("fail")

        client = FakeClient(retry_count=retry_count)
        with pytest.raises(exc_type):
            fn(client)
        assert call_count == retry_count


# ===========================================================================
# 9.3  Property test — no retry on non-connection exceptions
# ===========================================================================

class TestRetryNonConnectionProperty:

    @given(
        exc_type=st.sampled_from(
            [ValueError, TypeError, RuntimeError, KeyError, ZeroDivisionError]
        ),
    )
    @settings(max_examples=100)
    def test_calls_exactly_once(self, exc_type):
        call_count = 0

        @retry
        def fn(self_arg):
            nonlocal call_count
            call_count += 1
            raise exc_type("nope")

        client = FakeClient(retry_count=5)
        with pytest.raises(exc_type):
            fn(client)
        assert call_count == 1


# ===========================================================================
# 9.4  Unit tests for paginated decorator
# ===========================================================================


class TestPaginatedDecorator:
    """Requirements 14.1–14.4"""

    def test_success_without_pagination_error(self):
        """14.1 — success without PaginationError returns result directly."""
        @paginated
        def fn(*args, start, end, **kwargs):
            return pd.Series([1.0, 2.0], index=pd.date_range(start, periods=2, freq="h"))

        result = fn(start=pd.Timestamp("2023-01-01"), end=pd.Timestamp("2023-01-02"))
        assert isinstance(result, pd.Series)
        assert len(result) == 2

    def test_pagination_error_splits_at_midpoint(self):
        """14.2 — PaginationError splits time range at midpoint and recurses."""
        calls = []

        @paginated
        def fn(*args, start, end, **kwargs):
            calls.append((start, end))
            if len(calls) == 1:
                raise PaginationError("too much data")
            return pd.Series([1.0], index=[start])

        s = pd.Timestamp("2023-01-01")
        e = pd.Timestamp("2023-01-03")
        result = fn(start=s, end=e)

        # First call fails, then two recursive calls
        assert len(calls) == 3
        # The pivot should be the midpoint
        pivot = s + (e - s) / 2
        assert calls[1] == (s, pivot)
        assert calls[2] == (pivot, e)

    def test_recursive_calls_concatenate_with_pd_concat(self):
        """14.3 — recursive calls concatenate results with pd.concat."""
        call_count = 0

        @paginated
        def fn(*args, start, end, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise PaginationError("split")
            return pd.Series(
                [float(call_count)],
                index=pd.DatetimeIndex([start]),
            )

        s = pd.Timestamp("2023-01-01")
        e = pd.Timestamp("2023-01-03")
        result = fn(start=s, end=e)
        assert len(result) == 2
        assert 2.0 in result.values
        assert 3.0 in result.values

    def test_nested_pagination_errors_continue_splitting(self):
        """14.4 — nested PaginationErrors continue splitting."""
        calls = []

        @paginated
        def fn(*args, start, end, **kwargs):
            calls.append((start, end))
            # Fail on first two calls, succeed on the rest
            if len(calls) <= 2:
                raise PaginationError("split again")
            return pd.Series([1.0], index=[start])

        s = pd.Timestamp("2023-01-01")
        e = pd.Timestamp("2023-01-05")
        result = fn(start=s, end=e)
        # First call fails → 2 sub-calls; first sub-call fails → 2 more sub-calls
        # Total: 1 (fail) + 1 (fail) + 2 (ok) + 1 (ok from second half of first split) = 5
        assert len(calls) >= 5
        assert isinstance(result, pd.Series)


# ===========================================================================
# 9.5  Unit tests for year_limited decorator
# ===========================================================================


class TestYearLimitedDecorator:
    """Requirements 15.1–15.7"""

    def test_missing_start_raises(self):
        """15.1 — missing start kwarg raises Exception."""
        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            return pd.Series()

        with pytest.raises(Exception, match="start and end"):
            fn("self", end=pd.Timestamp("2023-01-01", tz="UTC"))

    def test_missing_end_raises(self):
        """15.1 — missing end kwarg raises Exception."""
        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            return pd.Series()

        with pytest.raises(Exception, match="start and end"):
            fn("self", start=pd.Timestamp("2023-01-01", tz="UTC"))

    def test_non_timestamp_start_raises(self):
        """15.2 — non-Timestamp start raises Exception."""
        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            return pd.Series()

        with pytest.raises(Exception, match="timezoned pandas"):
            fn("self", start="2023-01-01", end=pd.Timestamp("2023-12-31", tz="UTC"))

    def test_non_timestamp_end_raises(self):
        """15.2 — non-Timestamp end raises Exception."""
        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            return pd.Series()

        with pytest.raises(Exception, match="timezoned pandas"):
            fn("self", start=pd.Timestamp("2023-01-01", tz="UTC"), end="2023-12-31")

    def test_naive_timestamps_raise(self):
        """15.3 — naive timestamps raise Exception."""
        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            return pd.Series()

        with pytest.raises(Exception, match="timezoned pandas"):
            fn(
                "self",
                start=pd.Timestamp("2023-01-01"),
                end=pd.Timestamp("2023-12-31"),
            )

    def test_multi_year_span_calls_per_year_block(self):
        """15.4 — multi-year span calls wrapped function per year block."""
        received_blocks = []

        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            received_blocks.append((start, end))
            idx = pd.date_range(start, end, freq="D", inclusive="left")
            return pd.Series(range(len(idx)), index=idx)

        s = pd.Timestamp("2021-06-01", tz="UTC")
        e = pd.Timestamp("2023-06-01", tz="UTC")
        result = fn("self", start=s, end=e)

        expected_blocks = list(year_blocks(s, e))
        assert len(received_blocks) == len(expected_blocks)
        for (rs, re_), (es, ee) in zip(received_blocks, expected_blocks):
            assert rs == es
            assert re_ == ee
        assert isinstance(result, pd.Series)

    def test_no_matching_data_blocks_skipped(self):
        """15.5 — NoMatchingDataError blocks are skipped."""
        call_count = 0

        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise NoMatchingDataError
            idx = pd.date_range(start, end, freq="D", inclusive="left")
            return pd.Series(range(len(idx)), index=idx)

        s = pd.Timestamp("2021-06-01", tz="UTC")
        e = pd.Timestamp("2023-06-01", tz="UTC")
        result = fn("self", start=s, end=e)
        assert isinstance(result, pd.Series)
        assert call_count > 1

    def test_all_blocks_no_matching_data_raises(self):
        """15.6 — all blocks NoMatchingDataError raises NoMatchingDataError."""
        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            raise NoMatchingDataError

        s = pd.Timestamp("2021-06-01", tz="UTC")
        e = pd.Timestamp("2023-06-01", tz="UTC")
        with pytest.raises(NoMatchingDataError):
            fn("self", start=s, end=e)

    def test_non_unavailability_truncates_datetimeindex_frames(self):
        """15.7 — non-unavailability query truncates DatetimeIndex frames."""
        @year_limited
        def some_query(*args, start=None, end=None, **kwargs):
            # Return data that extends beyond the block boundaries
            wide_start = start - pd.Timedelta(days=5)
            wide_end = end + pd.Timedelta(days=5)
            idx = pd.date_range(wide_start, wide_end, freq="D")
            return pd.Series(range(len(idx)), index=idx)

        s = pd.Timestamp("2022-06-01", tz="UTC")
        e = pd.Timestamp("2024-06-01", tz="UTC")
        result = some_query("self", start=s, end=e)

        # The result should not contain timestamps beyond the original end
        assert result.index.max() <= e
        # The first block is closed on the left, so start should be included
        assert result.index.min() >= s - pd.Timedelta(days=5)


# ===========================================================================
# 9.6  Property test — year-limited calls per year block
# ===========================================================================

class TestYearLimitedProperty:

    @given(
        start_year=st.integers(min_value=2015, max_value=2020),
        span_years=st.integers(min_value=2, max_value=4),
        start_month=st.integers(min_value=1, max_value=12),
        start_day=st.integers(min_value=1, max_value=28),
    )
    @settings(max_examples=100)
    def test_calls_once_per_year_block(self, start_year, span_years, start_month, start_day):
        s = pd.Timestamp(year=start_year, month=start_month, day=start_day, tz="UTC")
        e = s + pd.DateOffset(years=span_years)

        received_blocks = []

        @year_limited
        def fn(*args, start=None, end=None, **kwargs):
            received_blocks.append((start, end))
            idx = pd.date_range(start, end, freq="D", inclusive="left")
            if len(idx) == 0:
                idx = pd.DatetimeIndex([start])
            return pd.Series(range(len(idx)), index=idx)

        fn("self", start=s, end=e)

        expected_blocks = list(year_blocks(s, e))
        assert len(received_blocks) == len(expected_blocks)


# ===========================================================================
# 9.7  Unit tests for day_limited decorator
# ===========================================================================


class TestDayLimitedDecorator:
    """Requirements 16.1–16.3"""

    def test_multi_day_span_calls_per_day_block(self):
        """16.1 — multi-day span calls wrapped function per day block."""
        received_blocks = []

        @day_limited
        def fn(*args, start, end, **kwargs):
            received_blocks.append((start, end))
            idx = pd.date_range(start, end, freq="h", inclusive="left")
            return pd.DataFrame({"v": range(len(idx))}, index=idx)

        s = pd.Timestamp("2023-01-01")
        e = pd.Timestamp("2023-01-04")
        result = fn("self", start=s, end=e)

        expected_blocks = list(day_blocks(s, e))
        assert len(received_blocks) == len(expected_blocks)
        for (rs, re_), (es, ee) in zip(received_blocks, expected_blocks):
            assert rs == es
            assert re_ == ee
        assert isinstance(result, pd.DataFrame)

    def test_no_matching_data_blocks_skipped(self):
        """16.2 — NoMatchingDataError blocks are skipped."""
        call_count = 0

        @day_limited
        def fn(*args, start, end, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise NoMatchingDataError
            idx = pd.date_range(start, end, freq="h", inclusive="left")
            return pd.DataFrame({"v": range(len(idx))}, index=idx)

        s = pd.Timestamp("2023-01-01")
        e = pd.Timestamp("2023-01-04")
        result = fn("self", start=s, end=e)
        assert isinstance(result, pd.DataFrame)
        assert call_count > 1

    def test_all_blocks_no_matching_data_raises(self):
        """16.3 — all blocks NoMatchingDataError raises NoMatchingDataError."""
        @day_limited
        def fn(*args, start, end, **kwargs):
            raise NoMatchingDataError

        s = pd.Timestamp("2023-01-01")
        e = pd.Timestamp("2023-01-04")
        with pytest.raises(NoMatchingDataError):
            fn("self", start=s, end=e)


# ===========================================================================
# 9.8  Property test — day-limited calls per day block
# ===========================================================================

class TestDayLimitedProperty:

    @given(
        start_offset_days=st.integers(min_value=0, max_value=100),
        span_days=st.integers(min_value=2, max_value=10),
    )
    @settings(max_examples=100)
    def test_calls_once_per_day_block(self, start_offset_days, span_days):
        s = pd.Timestamp("2023-01-01") + pd.Timedelta(days=start_offset_days)
        e = s + pd.Timedelta(days=span_days)

        received_blocks = []

        @day_limited
        def fn(*args, start, end, **kwargs):
            received_blocks.append((start, end))
            idx = pd.date_range(start, end, freq="h", inclusive="left")
            if len(idx) == 0:
                idx = pd.DatetimeIndex([start])
            return pd.DataFrame({"v": range(len(idx))}, index=idx)

        fn("self", start=s, end=e)

        expected_blocks = list(day_blocks(s, e))
        assert len(received_blocks) == len(expected_blocks)


# ===========================================================================
# 9.9  Unit tests for documents_limited decorator
# ===========================================================================


class TestDocumentsLimitedDecorator:
    """Requirements 17.1–17.4"""

    def test_iterates_offsets_in_steps_of_n(self):
        """17.1 — iterates offsets from 0 to 4800 in steps of n."""
        received_offsets = []
        n = 200

        @documents_limited(n)
        def fn(*args, offset=0, **kwargs):
            received_offsets.append(offset)
            idx = pd.DatetimeIndex([pd.Timestamp("2023-01-01") + pd.Timedelta(hours=offset)])
            return pd.DataFrame({"v": [float(offset)]}, index=idx)

        fn()

        expected_offsets = list(range(0, 4800 + n, n))
        assert received_offsets == expected_offsets

    def test_no_matching_data_at_offset_stops_iteration(self):
        """17.2 — NoMatchingDataError at offset stops iteration."""
        received_offsets = []
        n = 100

        @documents_limited(n)
        def fn(*args, offset=0, **kwargs):
            received_offsets.append(offset)
            if offset >= 300:
                raise NoMatchingDataError
            idx = pd.DatetimeIndex([pd.Timestamp("2023-01-01") + pd.Timedelta(hours=offset)])
            return pd.DataFrame({"v": [float(offset)]}, index=idx)

        result = fn()
        # Should have called offsets 0, 100, 200, 300 (stops at 300)
        assert received_offsets == [0, 100, 200, 300]
        assert isinstance(result, pd.DataFrame)

    def test_all_offsets_no_matching_data_raises(self):
        """17.3 — all offsets NoMatchingDataError raises NoMatchingDataError."""
        n = 200

        @documents_limited(n)
        def fn(*args, offset=0, **kwargs):
            raise NoMatchingDataError

        with pytest.raises(NoMatchingDataError):
            fn()

    def test_concatenation_handles_duplicate_indices_with_ffill(self):
        """17.4 — concatenation handles duplicate indices with forward-fill."""
        n = 100
        call_count = 0

        @documents_limited(n)
        def some_query(*args, offset=0, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 2:
                raise NoMatchingDataError
            # Both frames share the same index but different values / NaNs
            idx = pd.DatetimeIndex([
                pd.Timestamp("2023-01-01"),
                pd.Timestamp("2023-01-02"),
            ])
            if call_count == 1:
                return pd.DataFrame({"a": [1.0, float("nan")]}, index=idx)
            else:
                return pd.DataFrame({"a": [float("nan"), 2.0]}, index=idx)

        result = some_query()
        # Duplicate indices are grouped; ffill + last valid value
        assert isinstance(result, pd.DataFrame)
        # After dedup with ffill().iloc[[-1]], each duplicate group keeps last valid
        assert result.loc[pd.Timestamp("2023-01-01"), "a"] == 1.0
        assert result.loc[pd.Timestamp("2023-01-02"), "a"] == 2.0
