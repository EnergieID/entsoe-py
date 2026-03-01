import pytest
from entsoe.series_parsers import _resolution_to_timedelta


class TestResolutionConversion:
    """Unit tests for _resolution_to_timedelta.
    """

    @pytest.mark.parametrize(
        "code, expected",
        [
            ("PT60M", "60min"),
            ("PT15M", "15min"),
            ("PT30M", "30min"),
            ("P1Y", "12MS"),
            ("P1D", "1D"),
            ("P7D", "7D"),
            ("P1M", "1MS"),
            ("PT1M", "1min"),
        ],
        ids=["PT60M", "PT15M", "PT30M", "P1Y", "P1D", "P7D", "P1M", "PT1M"],
    )
    def test_known_resolution_codes(self, code, expected):
        """Each known ENTSO-E resolution code maps to the correct pandas frequency string."""
        assert _resolution_to_timedelta(code) == expected

    def test_unknown_resolution_raises_not_implemented(self):
        """An unknown resolution code raises NotImplementedError with the code in the message."""
        unknown = "PT45M"
        with pytest.raises(NotImplementedError, match=unknown):
            _resolution_to_timedelta(unknown)

import pandas as pd
from hypothesis import given, settings, strategies as st

# Strategy for generating known ENTSO-E resolution codes (mirrors conftest.resolution_codes)
_resolution_codes = st.sampled_from(['PT60M', 'PT15M', 'PT30M', 'P1Y', 'P1D', 'P7D', 'P1M', 'PT1M'])

# The documented mapping from ENTSO-E resolution codes to pandas frequency strings
EXPECTED_MAPPING = {
    'PT60M': '60min',
    'PT15M': '15min',
    'PT30M': '30min',
    'P1Y': '12MS',
    'P1D': '1D',
    'P7D': '7D',
    'P1M': '1MS',
    'PT1M': '1min',
}


class TestResolutionCodeRoundTrip:
    """Property test for resolution code round-trip validity.
    """

    @given(code=_resolution_codes)
    @settings(max_examples=100)
    def test_resolution_round_trip_produces_valid_offset(self, code):
        """For all known ENTSO-E resolution codes, _resolution_to_timedelta shall return
        a string that, when passed to pd.tseries.frequencies.to_offset, produces a valid
        pandas frequency object, and the returned string shall match the documented mapping."""
        freq_string = _resolution_to_timedelta(code)

        # The returned string must produce a valid pandas offset (not None)
        offset = pd.tseries.frequencies.to_offset(freq_string)
        assert offset is not None, (
            f"to_offset returned None for freq_string={freq_string!r} (code={code!r})"
        )

        # The returned string must match the documented mapping
        assert freq_string == EXPECTED_MAPPING[code], (
            f"Expected {EXPECTED_MAPPING[code]!r} for code={code!r}, got {freq_string!r}"
        )


KNOWN_RESOLUTION_CODES = {'PT60M', 'PT15M', 'PT30M', 'P1Y', 'P1D', 'P7D', 'P1M', 'PT1M'}


class TestUnknownResolutionCodes:
    """Property test for unknown resolution codes.
    """

    @given(code=st.text().filter(lambda s: s not in KNOWN_RESOLUTION_CODES))
    @settings(max_examples=100)
    def test_unknown_resolution_raises_not_implemented_with_message(self, code):
        """For all strings not in the set of known resolution codes,
        _resolution_to_timedelta shall raise NotImplementedError with a message
        containing the unrecognized input string."""
        with pytest.raises(NotImplementedError) as exc_info:
            _resolution_to_timedelta(code)
        assert code in str(exc_info.value), (
            f"Expected the unrecognized code {code!r} to appear in the error message, "
            f"but got: {str(exc_info.value)!r}"
        )


import bs4
from entsoe.series_parsers import _parse_datetimeindex


def _make_period_soup(start: str, end: str, resolution: str) -> bs4.element.Tag:
    """Build a minimal BeautifulSoup tag with start, end, and resolution elements."""
    xml = (
        f'<period>'
        f'<timeinterval>'
        f'<start>{start}</start>'
        f'<end>{end}</end>'
        f'</timeinterval>'
        f'<resolution>{resolution}</resolution>'
        f'</period>'
    )
    return bs4.BeautifulSoup(xml, 'xml').find('period')


class TestDatetimeIndexConstruction:
    """Unit tests for _parse_datetimeindex.
    """

    def test_basic_hourly_index(self):
        """A 24-hour period with PT60M resolution produces 24 hourly timestamps
        from start (inclusive) to end (exclusive)."""
        soup = _make_period_soup(
            start='2023-01-01T00:00Z',
            end='2023-01-02T00:00Z',
            resolution='PT60M',
        )
        index = _parse_datetimeindex(soup)

        assert len(index) == 24
        assert index[0] == pd.Timestamp('2023-01-01T00:00Z')
        assert index[-1] == pd.Timestamp('2023-01-01T23:00Z')
        # All elements are strictly less than end
        assert all(ts < pd.Timestamp('2023-01-02T00:00Z') for ts in index)

    def test_timezone_conversion_to_utc(self):
        """When a tz parameter is provided, the index is converted to that
        timezone and then to UTC."""
        soup = _make_period_soup(
            start='2023-06-01T00:00Z',
            end='2023-06-02T00:00Z',
            resolution='PT60M',
        )
        index = _parse_datetimeindex(soup, tz='Europe/Berlin')

        # The result should be in UTC
        assert str(index.tz) == 'UTC'
        assert len(index) == 24

    def test_dst_weekly_resolution_removes_extra_element(self):
        """When a DST transition occurs within a weekly period, the extra
        index element caused by the 25-hour day is removed.
        October 2023 DST transition: clocks go back on Oct 29 in Europe/Berlin.
        The function detects the DST jump and removes the last index element."""
        # A 5-week period spanning the October DST transition
        soup = _make_period_soup(
            start='2023-10-02T00:00Z',
            end='2023-11-06T00:00Z',
            resolution='P7D',
        )
        index = _parse_datetimeindex(soup, tz='Europe/Berlin')

        # date_range produces 5 elements for 5 weeks, but the DST fix
        # removes the last one, leaving 4
        assert len(index) == 4
        assert str(index.tz) == 'UTC'
        assert index[0] == pd.Timestamp('2023-10-02T00:00Z')
        assert index[-1] == pd.Timestamp('2023-10-23T00:00Z')

    def test_dst_daily_resolution_no_tz_removes_extra_element(self):
        """When a DST transition occurs and resolution is daily without timezone,
        the extra index element is removed when end.hour == start.hour + 1.
        This simulates the case where the period has one extra hour due to DST."""
        # Simulate a period where end hour = start hour + 1 (DST artifact)
        # Start at midnight, end at 01:00 three days later — the +1 hour
        # signals a DST-caused extra element
        soup = _make_period_soup(
            start='2023-10-28T00:00Z',
            end='2023-10-31T01:00Z',
            resolution='P1D',
        )
        index = _parse_datetimeindex(soup)

        # 3 days from Oct 28 to Oct 31, but end hour (01) == start hour (00) + 1
        # triggers the DST correction, removing the extra element
        assert len(index) == 3
        assert index[0] == pd.Timestamp('2023-10-28T00:00Z')
        assert index[-1] == pd.Timestamp('2023-10-30T00:00Z')


# ---------------------------------------------------------------------------
# Property 3: Datetime index bounds and frequency
# ---------------------------------------------------------------------------

# Resolution codes that work well with short time ranges (avoid P1D, P7D, P1M, P1Y)
_short_range_resolutions = st.sampled_from(['PT15M', 'PT30M', 'PT60M'])

# Mapping from resolution code to pandas offset string (subset used here)
_RESOLUTION_TO_FREQ = {
    'PT15M': '15min',
    'PT30M': '30min',
    'PT60M': '60min',
}

# Mapping from resolution code to timedelta for arithmetic
_RESOLUTION_TO_DELTA = {
    'PT15M': pd.Timedelta(minutes=15),
    'PT30M': pd.Timedelta(minutes=30),
    'PT60M': pd.Timedelta(minutes=60),
}


@st.composite
def _datetime_index_inputs(draw):
    """Generate (start, end, resolution) tuples suitable for _parse_datetimeindex.

    - start is floored to the hour
    - end = start + N * resolution_delta where N >= 1
    - Only uses PT15M / PT30M / PT60M to avoid needing very large time ranges
    """
    resolution = draw(_short_range_resolutions)
    delta = _RESOLUTION_TO_DELTA[resolution]

    # Generate a start timestamp floored to the hour (2000–2030 range)
    raw_dt = draw(
        st.datetimes(
            min_value=pd.Timestamp('2000-01-01').to_pydatetime(),
            max_value=pd.Timestamp('2030-01-01').to_pydatetime(),
        )
    )
    start = pd.Timestamp(raw_dt).floor('h')

    # N periods: at least 1, at most 96 (covers up to 4 days at 15-min resolution)
    n = draw(st.integers(min_value=1, max_value=96))
    end = start + n * delta

    return start, end, resolution


class TestDatetimeIndexBoundsAndFrequency:
    """Property test for datetime index bounds and frequency.
    """

    @given(inputs=_datetime_index_inputs())
    @settings(max_examples=100)
    def test_index_bounds_and_frequency(self, inputs):
        """For all valid combinations of start timestamp, end timestamp, and
        resolution code, _parse_datetimeindex shall produce a DatetimeIndex
        where the first element equals start, the last element is strictly
        less than end, and the frequency matches the resolution."""
        start, end, resolution = inputs
        expected_freq = _RESOLUTION_TO_FREQ[resolution]

        # Build a minimal BeautifulSoup period tag using the existing helper.
        # The 'Z' suffix makes the timestamps UTC-aware inside the parser.
        start_str = start.strftime('%Y-%m-%dT%H:%MZ')
        end_str = end.strftime('%Y-%m-%dT%H:%MZ')
        soup = _make_period_soup(start=start_str, end=end_str, resolution=resolution)

        index = _parse_datetimeindex(soup)

        # Make start/end UTC-aware for comparison (the parser returns UTC timestamps)
        start_utc = start.tz_localize('UTC')
        end_utc = end.tz_localize('UTC')

        # The index must not be empty
        assert len(index) > 0, (
            f"Expected non-empty index for start={start}, end={end}, resolution={resolution}"
        )

        # First element equals start
        assert index[0] == start_utc, (
            f"First element {index[0]} != start {start_utc}"
        )

        # Last element is strictly less than end
        assert index[-1] < end_utc, (
            f"Last element {index[-1]} is not strictly less than end {end_utc}"
        )

        # Frequency matches the resolution
        assert pd.tseries.frequencies.to_offset(expected_freq) == pd.tseries.frequencies.to_offset(index.freq), (
            f"Expected freq {expected_freq}, got {index.freq} "
            f"(resolution={resolution})"
        )


# ---------------------------------------------------------------------------
# Task 2.6: Unit tests for generic time series parsing
# ---------------------------------------------------------------------------

from entsoe.series_parsers import _parse_timeseries_generic, _extract_timeseries
from tests.conftest import build_timeseries_xml


def _get_timeseries_soup(periods, curve_type='A01'):
    """Build XML via build_timeseries_xml and return the first <timeseries> bs4 tag."""
    xml_text = build_timeseries_xml(periods, curve_type=curve_type)
    return next(_extract_timeseries(xml_text))


class TestGenericTimeSeriesParsing:
    """Unit tests for _parse_timeseries_generic.
    """

    def test_position_to_timestamp_mapping(self):
        """Each position p_i maps to timestamp start + (p_i - 1) * delta."""
        periods = [{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-01T04:00Z',
            'resolution': 'PT60M',
            'points': [(1, 100), (2, 200), (3, 300), (4, 400)],
        }]
        soup = _get_timeseries_soup(periods)
        result = _parse_timeseries_generic(soup)

        series = result['60min']
        start = pd.Timestamp('2023-01-01T00:00Z')
        delta = pd.Timedelta(hours=1)

        assert series[start + 0 * delta] == 100.0
        assert series[start + 1 * delta] == 200.0
        assert series[start + 2 * delta] == 300.0
        assert series[start + 3 * delta] == 400.0
        assert len(series) == 4

    def test_a03_curve_type_forward_fills_missing_positions(self):
        """A03 curve type reindexes to a continuous range and forward-fills gaps."""
        # Provide positions 1 and 3, skip position 2 — position 2 should be forward-filled
        periods = [{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-01T04:00Z',
            'resolution': 'PT60M',
            'points': [(1, 10), (3, 30)],
        }]
        soup = _get_timeseries_soup(periods, curve_type='A03')
        result = _parse_timeseries_generic(soup)

        series = result['60min']
        start = pd.Timestamp('2023-01-01T00:00Z')
        delta = pd.Timedelta(hours=1)

        # Should have 4 entries (continuous from start to end - delta)
        assert len(series) == 4
        assert series[start + 0 * delta] == 10.0   # position 1
        assert series[start + 1 * delta] == 10.0   # position 2 forward-filled from position 1
        assert series[start + 2 * delta] == 30.0   # position 3
        assert series[start + 3 * delta] == 30.0   # position 4 forward-filled from position 3

    def test_a01_curve_type_preserves_only_explicit_positions(self):
        """A01 curve type preserves only the explicitly provided positions."""
        # Provide positions 1 and 3 only — position 2 should NOT appear
        periods = [{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-01T04:00Z',
            'resolution': 'PT60M',
            'points': [(1, 10), (3, 30)],
        }]
        soup = _get_timeseries_soup(periods, curve_type='A01')
        result = _parse_timeseries_generic(soup)

        series = result['60min']
        start = pd.Timestamp('2023-01-01T00:00Z')
        delta = pd.Timedelta(hours=1)

        assert len(series) == 2
        assert series[start + 0 * delta] == 10.0   # position 1
        assert series[start + 2 * delta] == 30.0   # position 3
        # Position 2 timestamp should not be in the index
        assert (start + 1 * delta) not in series.index

    def test_multiple_periods_different_resolutions_return_dict_keyed_by_freq(self):
        """Multiple periods with different resolutions are grouped by frequency string."""
        # Build a single <timeseries> with two <period> elements at different resolutions.
        # _parse_timeseries_generic operates on one timeseries soup tag, so both periods
        # must be inside the same <timeseries>.
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<timeseries>\n'
            '  <curvetype>A01</curvetype>\n'
            '  <period>\n'
            '    <timeinterval>\n'
            '      <start>2023-01-01T00:00Z</start>\n'
            '      <end>2023-01-01T02:00Z</end>\n'
            '    </timeinterval>\n'
            '    <resolution>PT60M</resolution>\n'
            '    <point><position>1</position><quantity>100</quantity></point>\n'
            '    <point><position>2</position><quantity>200</quantity></point>\n'
            '  </period>\n'
            '  <period>\n'
            '    <timeinterval>\n'
            '      <start>2023-01-01T00:00Z</start>\n'
            '      <end>2023-01-01T01:00Z</end>\n'
            '    </timeinterval>\n'
            '    <resolution>PT15M</resolution>\n'
            '    <point><position>1</position><quantity>10</quantity></point>\n'
            '    <point><position>2</position><quantity>20</quantity></point>\n'
            '    <point><position>3</position><quantity>30</quantity></point>\n'
            '    <point><position>4</position><quantity>40</quantity></point>\n'
            '  </period>\n'
            '</timeseries>'
        )
        soup = bs4.BeautifulSoup(xml, 'xml').find('timeseries')
        result = _parse_timeseries_generic(soup)

        # Result is a dict; both frequency keys should have data
        assert isinstance(result, dict)
        assert result['60min'] is not None
        assert result['15min'] is not None
        assert len(result['60min']) == 2
        assert len(result['15min']) == 4

    def test_merge_series_concatenates_resolution_groups(self):
        """When merge_series=True, all resolution groups are concatenated into a single Series."""
        # Build a single <timeseries> with two <period> elements at different resolutions.
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<timeseries>\n'
            '  <curvetype>A01</curvetype>\n'
            '  <period>\n'
            '    <timeinterval>\n'
            '      <start>2023-01-01T00:00Z</start>\n'
            '      <end>2023-01-01T02:00Z</end>\n'
            '    </timeinterval>\n'
            '    <resolution>PT60M</resolution>\n'
            '    <point><position>1</position><quantity>100</quantity></point>\n'
            '    <point><position>2</position><quantity>200</quantity></point>\n'
            '  </period>\n'
            '  <period>\n'
            '    <timeinterval>\n'
            '      <start>2023-01-02T00:00Z</start>\n'
            '      <end>2023-01-02T01:00Z</end>\n'
            '    </timeinterval>\n'
            '    <resolution>PT15M</resolution>\n'
            '    <point><position>1</position><quantity>10</quantity></point>\n'
            '    <point><position>2</position><quantity>20</quantity></point>\n'
            '    <point><position>3</position><quantity>30</quantity></point>\n'
            '    <point><position>4</position><quantity>40</quantity></point>\n'
            '  </period>\n'
            '</timeseries>'
        )
        soup = bs4.BeautifulSoup(xml, 'xml').find('timeseries')
        result = _parse_timeseries_generic(soup, merge_series=True)

        # merge_series=True returns a single pd.Series, not a dict
        assert isinstance(result, pd.Series)
        # Total points: 2 (60min) + 4 (15min) = 6
        assert len(result) == 6


# ---------------------------------------------------------------------------
# Property 4: Position-to-timestamp mapping
# ---------------------------------------------------------------------------

_POS_RESOLUTIONS = st.sampled_from(['PT15M', 'PT30M', 'PT60M'])

_POS_DELTA_MAP = {
    'PT15M': pd.Timedelta(minutes=15),
    'PT30M': pd.Timedelta(minutes=30),
    'PT60M': pd.Timedelta(minutes=60),
}

_POS_FREQ_MAP = {
    'PT15M': '15min',
    'PT30M': '30min',
    'PT60M': '60min',
}


@st.composite
def _position_mapping_inputs(draw):
    """Generate (start, resolution, n_points, values) for position-to-timestamp tests.

    - start is floored to the hour
    - resolution is one of PT15M / PT30M / PT60M
    - n_points is 1..24
    - values is a list of n_points random floats
    """
    resolution = draw(_POS_RESOLUTIONS)
    delta = _POS_DELTA_MAP[resolution]

    raw_dt = draw(
        st.datetimes(
            min_value=pd.Timestamp('2000-01-01').to_pydatetime(),
            max_value=pd.Timestamp('2030-01-01').to_pydatetime(),
        )
    )
    start = pd.Timestamp(raw_dt).floor('h')

    n_points = draw(st.integers(min_value=1, max_value=24))
    values = draw(
        st.lists(
            st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
            min_size=n_points,
            max_size=n_points,
        )
    )
    return start, resolution, n_points, values


class TestPositionToTimestampMapping:
    """Property test for position-to-timestamp mapping.
    """

    @given(inputs=_position_mapping_inputs())
    @settings(max_examples=100)
    def test_position_maps_to_correct_timestamp(self, inputs):
        """For all valid XML periods with N points at positions p1..pN, start
        timestamp S, and resolution delta D, _parse_timeseries_generic shall
        map each position p_i to timestamp S + (p_i - 1) * D.

        """
        start, resolution, n_points, values = inputs
        delta = _POS_DELTA_MAP[resolution]
        freq_str = _POS_FREQ_MAP[resolution]
        end = start + n_points * delta

        # Build points with sequential positions 1..N
        points = [(i + 1, values[i]) for i in range(n_points)]

        period = {
            'start': start.strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
            'resolution': resolution,
            'points': points,
        }

        soup = _get_timeseries_soup([period])
        result = _parse_timeseries_generic(soup)

        series = result[freq_str]
        assert series is not None, f"No series found for freq {freq_str}"
        assert len(series) == n_points, (
            f"Expected {n_points} points, got {len(series)}"
        )

        start_utc = start.tz_localize('UTC')
        for i in range(n_points):
            expected_ts = start_utc + i * delta
            assert expected_ts in series.index, (
                f"Position {i+1}: expected timestamp {expected_ts} not in index"
            )
            assert series[expected_ts] == pytest.approx(values[i]), (
                f"Position {i+1}: expected value {values[i]}, got {series[expected_ts]}"
            )


# ---------------------------------------------------------------------------
# Property 5: A03 curve type forward-fill completeness
# ---------------------------------------------------------------------------


@st.composite
def _a03_forward_fill_inputs(draw):
    """Generate inputs for A03 forward-fill completeness tests.

    Returns (start, resolution, n_total, subset_positions, values) where:
    - start is floored to the hour
    - resolution is one of PT15M / PT30M / PT60M
    - n_total is 4..24 (total positions in the period)
    - subset_positions is a sorted list of positions from 1..N (always includes 1)
    - values maps each subset position to a random float
    """
    resolution = draw(_POS_RESOLUTIONS)

    raw_dt = draw(
        st.datetimes(
            min_value=pd.Timestamp('2000-01-01').to_pydatetime(),
            max_value=pd.Timestamp('2030-01-01').to_pydatetime(),
        )
    )
    start = pd.Timestamp(raw_dt).floor('h')

    n_total = draw(st.integers(min_value=4, max_value=24))

    # Draw a subset of positions from 2..N, then always include position 1
    remaining = draw(
        st.lists(
            st.integers(min_value=2, max_value=n_total),
            min_size=0,
            max_size=n_total - 1,
            unique=True,
        )
    )
    subset_positions = sorted([1] + remaining)

    values = draw(
        st.lists(
            st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
            min_size=len(subset_positions),
            max_size=len(subset_positions),
        )
    )

    return start, resolution, n_total, subset_positions, values


class TestA03ForwardFillCompleteness:
    """Property test for A03 curve type forward-fill completeness.
    """

    @given(inputs=_a03_forward_fill_inputs())
    @settings(max_examples=100)
    def test_a03_produces_continuous_forward_filled_series(self, inputs):
        """For all XML periods with curve type A03 and any subset of positions
        from 1..N, _parse_timeseries_generic shall produce a Series with a
        continuous DatetimeIndex (no gaps) where missing positions are
        forward-filled from the last provided value.

        """
        start, resolution, n_total, subset_positions, values = inputs
        delta = _POS_DELTA_MAP[resolution]
        freq_str = _POS_FREQ_MAP[resolution]
        end = start + n_total * delta

        # Build points only for the subset positions
        points = [(pos, values[i]) for i, pos in enumerate(subset_positions)]

        period = {
            'start': start.strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
            'resolution': resolution,
            'points': points,
        }

        soup = _get_timeseries_soup([period], curve_type='A03')
        result = _parse_timeseries_generic(soup)

        series = result[freq_str]
        assert series is not None, f"No series found for freq {freq_str}"

        # 1. The result must have exactly N entries (continuous, no gaps)
        assert len(series) == n_total, (
            f"Expected {n_total} entries (continuous), got {len(series)}. "
            f"Subset positions: {subset_positions}"
        )

        # 2. The index must be continuous with no gaps
        start_utc = start.tz_localize('UTC')
        expected_index = pd.date_range(start_utc, periods=n_total, freq=freq_str)
        pd.testing.assert_index_equal(series.index, expected_index)

        # 3. Verify forward-fill: each position should have the value of the
        #    last provided position at or before it
        pos_to_value = dict(zip(subset_positions, values))
        last_value = None
        for pos in range(1, n_total + 1):
            ts = start_utc + (pos - 1) * delta
            if pos in pos_to_value:
                last_value = pos_to_value[pos]
            assert series[ts] == pytest.approx(last_value), (
                f"Position {pos} (ts={ts}): expected {last_value} "
                f"(forward-filled), got {series[ts]}"
            )


# ---------------------------------------------------------------------------
# Property 6: A01 curve type preserves only explicit positions
# ---------------------------------------------------------------------------


@st.composite
def _a01_explicit_positions_inputs(draw):
    """Generate inputs for A01 explicit-positions tests.

    Returns (start, resolution, n_total, subset_positions, values) where:
    - start is floored to the hour
    - resolution is one of PT15M / PT30M / PT60M
    - n_total is 4..24 (total positions in the period)
    - subset_positions is a sorted list of at least 1 unique position from 1..N
    - values maps each subset position to a random float
    """
    resolution = draw(_POS_RESOLUTIONS)

    raw_dt = draw(
        st.datetimes(
            min_value=pd.Timestamp('2000-01-01').to_pydatetime(),
            max_value=pd.Timestamp('2030-01-01').to_pydatetime(),
        )
    )
    start = pd.Timestamp(raw_dt).floor('h')

    n_total = draw(st.integers(min_value=4, max_value=24))

    # Draw a random non-empty subset of positions from 1..N
    subset_positions = draw(
        st.lists(
            st.integers(min_value=1, max_value=n_total),
            min_size=1,
            max_size=n_total,
            unique=True,
        ).map(sorted)
    )

    values = draw(
        st.lists(
            st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
            min_size=len(subset_positions),
            max_size=len(subset_positions),
        )
    )

    return start, resolution, n_total, subset_positions, values


class TestA01ExplicitPositions:
    """Property test for A01 curve type preserving only explicit positions.
    """

    @given(inputs=_a01_explicit_positions_inputs())
    @settings(max_examples=100)
    def test_a01_preserves_only_explicit_positions(self, inputs):
        """For all XML periods with curve type A01 and a set of explicit
        positions, _parse_timeseries_generic shall produce a Series containing
        exactly those positions and no others.

        """
        start, resolution, n_total, subset_positions, values = inputs
        delta = _POS_DELTA_MAP[resolution]
        freq_str = _POS_FREQ_MAP[resolution]
        end = start + n_total * delta

        # Build points only for the subset positions
        points = [(pos, values[i]) for i, pos in enumerate(subset_positions)]

        period = {
            'start': start.strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
            'resolution': resolution,
            'points': points,
        }

        soup = _get_timeseries_soup([period], curve_type='A01')
        result = _parse_timeseries_generic(soup)

        series = result[freq_str]
        assert series is not None, f"No series found for freq {freq_str}"

        # 1. The result must contain exactly the provided positions and no others
        assert len(series) == len(subset_positions), (
            f"Expected {len(subset_positions)} entries (explicit only), "
            f"got {len(series)}. Subset: {subset_positions}"
        )

        # 2. Each position maps to the correct timestamp and value
        start_utc = start.tz_localize('UTC')
        expected_timestamps = set()
        for i, pos in enumerate(subset_positions):
            expected_ts = start_utc + (pos - 1) * delta
            expected_timestamps.add(expected_ts)
            assert expected_ts in series.index, (
                f"Position {pos}: expected timestamp {expected_ts} not in index"
            )
            assert series[expected_ts] == pytest.approx(values[i]), (
                f"Position {pos}: expected value {values[i]}, got {series[expected_ts]}"
            )

        # 3. No extra timestamps beyond the explicit positions
        actual_timestamps = set(series.index)
        assert actual_timestamps == expected_timestamps, (
            f"Extra timestamps found: {actual_timestamps - expected_timestamps}"
        )
