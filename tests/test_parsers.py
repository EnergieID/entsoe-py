"""
Unit tests for domain parsers in entsoe/parsers.py.

These tests feed real XML through the full parsing pipeline — no mocking of
internal helpers. XML is constructed via the conftest.py builder helpers.
"""
import warnings

import pandas as pd
import pytest
from bs4 import XMLParsedAsHTMLWarning

from entsoe.parsers import parse_prices
from tests.conftest import build_price_xml, _wrap_document

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


# ---------------------------------------------------------------------------
# Price parser — Requirements 4.1, 4.2, 4.3, 4.4
# ---------------------------------------------------------------------------


class TestParsePricesSingleTimeseries:
    """Requirement 4.1: single timeseries → correct float Series under resolution key."""

    def test_single_hourly_timeseries(self):
        xml = build_price_xml([{
            "start": "2023-01-01T00:00Z",
            "end": "2023-01-01T03:00Z",
            "resolution": "PT60M",
            "points": [(1, 50.0), (2, 60.5), (3, 70.25)],
        }])
        result = parse_prices(xml)

        assert "60min" in result
        s = result["60min"]
        assert isinstance(s, pd.Series)
        assert s.dtype == float
        assert len(s) == 3
        assert list(s.values) == [50.0, 60.5, 70.25]
        # Verify timestamps
        expected_idx = pd.date_range("2023-01-01T00:00Z", periods=3, freq="60min")
        pd.testing.assert_index_equal(s.index, expected_idx)

    def test_single_15min_timeseries(self):
        xml = build_price_xml([{
            "start": "2023-06-15T12:00Z",
            "end": "2023-06-15T13:00Z",
            "resolution": "PT15M",
            "points": [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)],
        }])
        result = parse_prices(xml)

        s = result["15min"]
        assert len(s) == 4
        assert s.iloc[0] == 10.0
        assert s.iloc[3] == 40.0


class TestParsePricesMultipleResolutions:
    """Requirement 4.2: multiple timeseries at different resolutions → separate Series."""

    def test_hourly_and_15min_timeseries(self):
        xml = build_price_xml([
            {
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0)],
            },
            {
                "start": "2023-01-01T02:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT15M",
                "points": [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)],
            },
        ])
        result = parse_prices(xml)

        assert len(result["60min"]) == 2
        assert result["60min"].iloc[0] == 100.0

        assert len(result["15min"]) == 4
        assert result["15min"].iloc[0] == 10.0


class TestParsePricesEmpty:
    """Requirement 4.3: empty XML → dict with empty Series for each resolution."""

    def test_empty_document(self):
        xml = _wrap_document("")
        result = parse_prices(xml)

        assert isinstance(result, dict)
        for key in ("15min", "30min", "60min"):
            assert key in result
            assert isinstance(result[key], pd.Series)
            assert len(result[key]) == 0


class TestParsePricesCommaThousandSeparators:
    """Requirement 4.4: commas stripped before float conversion."""

    def test_comma_in_price_value(self):
        # Build XML manually to inject comma-formatted values
        xml = _wrap_document(
            '  <timeseries>\n'
            '    <curvetype>A01</curvetype>\n'
            '      <period>\n'
            '        <timeinterval>\n'
            '          <start>2023-01-01T00:00Z</start>\n'
            '          <end>2023-01-01T02:00Z</end>\n'
            '        </timeinterval>\n'
            '        <resolution>PT60M</resolution>\n'
            '        <point>\n'
            '          <position>1</position>\n'
            '          <price.amount>1,234.56</price.amount>\n'
            '        </point>\n'
            '        <point>\n'
            '          <position>2</position>\n'
            '          <price.amount>2,345.67</price.amount>\n'
            '        </point>\n'
            '      </period>\n'
            '  </timeseries>\n'
        )
        result = parse_prices(xml)

        s = result["60min"]
        assert len(s) == 2
        assert s.iloc[0] == 1234.56
        assert s.iloc[1] == 2345.67


# ---------------------------------------------------------------------------
# Property-based tests — Hypothesis
# ---------------------------------------------------------------------------

from hypothesis import given, settings, strategies as st
from tests.conftest import build_price_xml


# Resolution code → dict key mapping for price-relevant resolutions
_RES_TO_KEY = {
    'PT15M': '15min',
    'PT30M': '30min',
    'PT60M': '60min',
}


@given(
    resolution=st.sampled_from(['PT15M', 'PT30M', 'PT60M']),
    prices=st.lists(
        st.floats(min_value=-500.0, max_value=5000.0, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=24,
    ),
)
@settings(max_examples=100)
def test_property_price_parser_float_series_with_correct_values(resolution, prices):
    """Property 7: Price parser produces float Series with correct values.

    For all valid price XML with known price values, parse_prices shall return
    a dictionary where each resolution key maps to a float Series, and the
    values match the input.

    """
    # Round prices to 2 decimal places to avoid floating-point formatting issues
    prices = [round(p, 2) for p in prices]
    n = len(prices)

    # Compute end timestamp based on resolution and number of points
    delta_map = {'PT15M': 15, 'PT30M': 30, 'PT60M': 60}
    total_minutes = n * delta_map[resolution]
    start = "2023-01-01T00:00Z"
    end_ts = pd.Timestamp(start) + pd.Timedelta(minutes=total_minutes)
    end = end_ts.strftime('%Y-%m-%dT%H:%MZ')

    points = [(i + 1, p) for i, p in enumerate(prices)]
    xml = build_price_xml([{
        "start": start,
        "end": end,
        "resolution": resolution,
        "points": points,
    }])

    result = parse_prices(xml)
    key = _RES_TO_KEY[resolution]

    # Result must contain the resolution key with a float Series
    assert key in result
    s = result[key]
    assert isinstance(s, pd.Series)
    assert s.dtype == float
    assert len(s) == n

    # All values must match the input prices
    for i, expected in enumerate(prices):
        assert s.iloc[i] == pytest.approx(expected, abs=1e-9)


@given(
    resolution=st.sampled_from(['PT15M', 'PT30M', 'PT60M']),
    raw_values=st.lists(
        st.floats(min_value=1000.0, max_value=99999.99, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=12,
    ),
)
@settings(max_examples=100)
def test_property_price_parser_comma_thousand_separators(resolution, raw_values):
    """Property 7 (comma variant): comma-separated values like '1,234.56' parse correctly.

    """
    raw_values = [round(v, 2) for v in raw_values]
    n = len(raw_values)

    delta_map = {'PT15M': 15, 'PT30M': 30, 'PT60M': 60}
    total_minutes = n * delta_map[resolution]
    start = "2023-01-01T00:00Z"
    end_ts = pd.Timestamp(start) + pd.Timedelta(minutes=total_minutes)
    end = end_ts.strftime('%Y-%m-%dT%H:%MZ')

    # Format values with comma thousand separators (e.g. 1234.56 → "1,234.56")
    formatted = [f"{v:,.2f}" for v in raw_values]

    # Build XML manually with comma-formatted price values
    points_xml = ""
    for i, fv in enumerate(formatted):
        points_xml += (
            f'        <point>\n'
            f'          <position>{i + 1}</position>\n'
            f'          <price.amount>{fv}</price.amount>\n'
            f'        </point>\n'
        )

    xml = _wrap_document(
        f'  <timeseries>\n'
        f'    <curvetype>A01</curvetype>\n'
        f'      <period>\n'
        f'        <timeinterval>\n'
        f'          <start>{start}</start>\n'
        f'          <end>{end}</end>\n'
        f'        </timeinterval>\n'
        f'        <resolution>{resolution}</resolution>\n'
        f'{points_xml}'
        f'      </period>\n'
        f'  </timeseries>\n'
    )

    result = parse_prices(xml)
    key = _RES_TO_KEY[resolution]

    s = result[key]
    assert isinstance(s, pd.Series)
    assert s.dtype == float
    assert len(s) == n

    for i, expected in enumerate(raw_values):
        assert s.iloc[i] == pytest.approx(expected, abs=1e-9)


# ---------------------------------------------------------------------------
# Load parser — Requirements 5.1, 5.2, 5.3, 5.4
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_loads
from tests.conftest import build_load_xml


class TestParseLoadsForecasted:
    """Requirement 5.1: process_type A01 → DataFrame with 'Forecasted Load' column."""

    def test_a01_returns_forecasted_load_column(self):
        xml = build_load_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [(1, 1000.0), (2, 1100.0), (3, 1200.0)],
            }],
            process_type='A01',
        )
        result = parse_loads(xml, process_type='A01')

        assert isinstance(result, pd.DataFrame)
        assert "Forecasted Load" in result.columns
        assert len(result) == 3
        assert list(result["Forecasted Load"].values) == [1000.0, 1100.0, 1200.0]


class TestParseLoadsActual:
    """Requirement 5.2: process_type A16 → DataFrame with 'Actual Load' column."""

    def test_a16_returns_actual_load_column(self):
        xml = build_load_xml(
            [{
                "start": "2023-06-15T12:00Z",
                "end": "2023-06-15T15:00Z",
                "resolution": "PT60M",
                "points": [(1, 500.0), (2, 550.0), (3, 600.0)],
            }],
            process_type='A16',
        )
        result = parse_loads(xml, process_type='A16')

        assert isinstance(result, pd.DataFrame)
        assert "Actual Load" in result.columns
        assert len(result) == 3
        assert list(result["Actual Load"].values) == [500.0, 550.0, 600.0]


class TestParseLoadsMinMaxForecast:
    """Requirement 5.3: other process_type → 'Min Forecasted Load' and 'Max Forecasted Load'."""

    def test_other_process_type_returns_min_max_columns(self):
        # Build XML with two timeseries: one A60 (min) and one A61 (max)
        xml = _wrap_document(
            '  <timeseries>\n'
            '    <businesstype>A60</businesstype>\n'
            '    <curvetype>A01</curvetype>\n'
            '      <period>\n'
            '        <timeinterval>\n'
            '          <start>2023-01-01T00:00Z</start>\n'
            '          <end>2023-01-01T03:00Z</end>\n'
            '        </timeinterval>\n'
            '        <resolution>PT60M</resolution>\n'
            '        <point>\n'
            '          <position>1</position>\n'
            '          <quantity>800.0</quantity>\n'
            '        </point>\n'
            '        <point>\n'
            '          <position>2</position>\n'
            '          <quantity>850.0</quantity>\n'
            '        </point>\n'
            '        <point>\n'
            '          <position>3</position>\n'
            '          <quantity>900.0</quantity>\n'
            '        </point>\n'
            '      </period>\n'
            '  </timeseries>\n'
            '  <timeseries>\n'
            '    <businesstype>A61</businesstype>\n'
            '    <curvetype>A01</curvetype>\n'
            '      <period>\n'
            '        <timeinterval>\n'
            '          <start>2023-01-01T00:00Z</start>\n'
            '          <end>2023-01-01T03:00Z</end>\n'
            '        </timeinterval>\n'
            '        <resolution>PT60M</resolution>\n'
            '        <point>\n'
            '          <position>1</position>\n'
            '          <quantity>1200.0</quantity>\n'
            '        </point>\n'
            '        <point>\n'
            '          <position>2</position>\n'
            '          <quantity>1250.0</quantity>\n'
            '        </point>\n'
            '        <point>\n'
            '          <position>3</position>\n'
            '          <quantity>1300.0</quantity>\n'
            '        </point>\n'
            '      </period>\n'
            '  </timeseries>\n'
        )
        result = parse_loads(xml, process_type='A02')

        assert isinstance(result, pd.DataFrame)
        assert "Min Forecasted Load" in result.columns
        assert "Max Forecasted Load" in result.columns
        assert list(result["Min Forecasted Load"].values) == [800.0, 850.0, 900.0]
        assert list(result["Max Forecasted Load"].values) == [1200.0, 1250.0, 1300.0]


class TestParseLoadsMultipleTimeseriesSorted:
    """Requirement 5.4: multiple timeseries concatenated and sorted by index."""

    def test_multiple_timeseries_concatenated_and_sorted(self):
        # Build XML with two timeseries covering non-contiguous time ranges
        # Second timeseries has earlier timestamps to verify sorting
        xml = _wrap_document(
            '  <timeseries>\n'
            '    <businesstype>A04</businesstype>\n'
            '    <curvetype>A01</curvetype>\n'
            '      <period>\n'
            '        <timeinterval>\n'
            '          <start>2023-01-01T06:00Z</start>\n'
            '          <end>2023-01-01T08:00Z</end>\n'
            '        </timeinterval>\n'
            '        <resolution>PT60M</resolution>\n'
            '        <point>\n'
            '          <position>1</position>\n'
            '          <quantity>600.0</quantity>\n'
            '        </point>\n'
            '        <point>\n'
            '          <position>2</position>\n'
            '          <quantity>700.0</quantity>\n'
            '        </point>\n'
            '      </period>\n'
            '  </timeseries>\n'
            '  <timeseries>\n'
            '    <businesstype>A04</businesstype>\n'
            '    <curvetype>A01</curvetype>\n'
            '      <period>\n'
            '        <timeinterval>\n'
            '          <start>2023-01-01T00:00Z</start>\n'
            '          <end>2023-01-01T02:00Z</end>\n'
            '        </timeinterval>\n'
            '        <resolution>PT60M</resolution>\n'
            '        <point>\n'
            '          <position>1</position>\n'
            '          <quantity>100.0</quantity>\n'
            '        </point>\n'
            '        <point>\n'
            '          <position>2</position>\n'
            '          <quantity>200.0</quantity>\n'
            '        </point>\n'
            '      </period>\n'
            '  </timeseries>\n'
        )
        result = parse_loads(xml, process_type='A01')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        # Index should be sorted: 00:00, 01:00, 06:00, 07:00
        assert result.index.is_monotonic_increasing
        # Values should follow sorted order
        assert list(result["Forecasted Load"].values) == [100.0, 200.0, 600.0, 700.0]


# ---------------------------------------------------------------------------
# Generation parser — Requirements 6.1, 6.2, 6.3, 6.4, 6.5
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_generation
from entsoe.mappings import PSRTYPE_MAPPINGS
from tests.conftest import build_generation_xml


class TestParseGenerationPerPlantFalse:
    """Requirement 6.1: per_plant=False aggregates by PSR type with PSRTYPE_MAPPINGS column names."""

    def test_aggregated_by_psr_type(self):
        xml = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [(1, 500.0), (2, 600.0), (3, 700.0)],
            }],
            psr_type='B14',
        )
        result = parse_generation(xml, per_plant=False)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        # Column name should use PSRTYPE_MAPPINGS value for B14
        expected_name = PSRTYPE_MAPPINGS['B14']  # 'Nuclear'
        assert expected_name in result.columns
        assert list(result[expected_name].values) == [500.0, 600.0, 700.0]


class TestParseGenerationPerPlantTrue:
    """Requirement 6.2: per_plant=True includes plant name in Series name tuple."""

    def test_plant_name_in_column_tuple(self):
        xml = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0)],
            }],
            psr_type='B16',
            per_plant=True,
            plant_name='SolarPark Alpha',
        )
        result = parse_generation(xml, per_plant=True)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # With per_plant=True, column should be a tuple containing the plant name
        col = result.columns[0]
        # The tuple should contain the plant name and the PSR type name
        assert 'SolarPark Alpha' in col
        assert PSRTYPE_MAPPINGS['B16'] in col  # 'Solar'


class TestParseGenerationIncludeEic:
    """Requirement 6.3: include_eic=True with per_plant=True includes EIC code in name tuple."""

    def test_eic_code_in_column_tuple(self):
        # Use two plants with different EIC codes so the EIC level is not
        # dropped by _calc_nett_and_drop_redundant_columns (it drops the last
        # level when it has only one unique value).
        xml1 = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 300.0), (2, 400.0)],
            }],
            psr_type='B04',
            per_plant=True,
            plant_name='GasPlant Alpha',
            include_eic=True,
            eic_code='11W0000000000001',
        )
        xml2 = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 500.0), (2, 600.0)],
            }],
            psr_type='B04',
            per_plant=True,
            plant_name='GasPlant Beta',
            include_eic=True,
            eic_code='11W0000000000002',
        )
        from bs4 import BeautifulSoup
        soup1 = BeautifulSoup(xml1, 'html.parser')
        soup2 = BeautifulSoup(xml2, 'html.parser')
        ts1 = soup1.find('timeseries')
        ts2 = soup2.find('timeseries')
        combined_xml = _wrap_document(str(ts1) + '\n' + str(ts2))

        result = parse_generation(combined_xml, per_plant=True, include_eic=True)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # Both columns should contain EIC codes, plant names, and PSR type
        col1, col2 = result.columns[0], result.columns[1]
        eic_codes = {col1[-1], col2[-1]}
        assert '11W0000000000001' in eic_codes
        assert '11W0000000000002' in eic_codes
        plant_names = {col1[0], col2[0]}
        assert 'GasPlant Alpha' in plant_names
        assert 'GasPlant Beta' in plant_names
        # PSR type name should be in each tuple
        assert PSRTYPE_MAPPINGS['B04'] in col1  # 'Fossil Gas'
        assert PSRTYPE_MAPPINGS['B04'] in col2


class TestParseGenerationOutBiddingZone:
    """Requirement 6.4: outBiddingZone_Domain labels metric as 'Actual Consumption'."""

    def test_consumption_label(self):
        xml = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 50.0), (2, 60.0)],
            }],
            psr_type='B10',
            has_out_bidding_zone=True,
        )
        result = parse_generation(xml, per_plant=False)

        assert isinstance(result, pd.DataFrame)
        # With a single timeseries, the metric level ('Actual Consumption')
        # is the only value in the last level and gets dropped, leaving just
        # the PSR type name as the column. But the underlying series was
        # named with 'Actual Consumption' (not 'Actual Aggregated').
        psr_name = PSRTYPE_MAPPINGS['B10']  # 'Hydro Pumped Storage'
        assert psr_name in result.columns
        assert list(result[psr_name].values) == [50.0, 60.0]

    def test_consumption_label_with_both_metrics(self):
        """Build XML with both in-bidding-zone (aggregated) and out-bidding-zone (consumption)
        timeseries for the same PSR type to verify the metric labels."""
        # Aggregated (no outBiddingZone)
        agg_xml = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 1000.0), (2, 1100.0)],
            }],
            psr_type='B10',
            has_out_bidding_zone=False,
        )
        # Consumption (with outBiddingZone)
        cons_xml = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 200.0), (2, 250.0)],
            }],
            psr_type='B10',
            has_out_bidding_zone=True,
        )
        # Combine both timeseries into one document by extracting inner XML
        from bs4 import BeautifulSoup
        soup_agg = BeautifulSoup(agg_xml, 'html.parser')
        soup_cons = BeautifulSoup(cons_xml, 'html.parser')
        ts_agg = soup_agg.find('timeseries')
        ts_cons = soup_cons.find('timeseries')
        combined_xml = _wrap_document(str(ts_agg) + '\n' + str(ts_cons))

        result = parse_generation(combined_xml, per_plant=False)

        assert isinstance(result, pd.DataFrame)
        # Should have MultiIndex columns with both 'Actual Aggregated' and 'Actual Consumption'
        psr_name = PSRTYPE_MAPPINGS['B10']  # 'Hydro Pumped Storage'
        col_strs = [str(c) for c in result.columns]
        has_consumption = any('Actual Consumption' in s for s in col_strs)
        has_aggregated = any('Actual Aggregated' in s for s in col_strs)
        assert has_consumption, f"Expected 'Actual Consumption' in columns, got {result.columns.tolist()}"
        assert has_aggregated, f"Expected 'Actual Aggregated' in columns, got {result.columns.tolist()}"


class TestParseGenerationNett:
    """Requirement 6.5: nett=True calculates net generation."""

    def test_nett_subtracts_consumption_from_aggregated(self):
        """Build XML with both aggregated and consumption for same PSR type,
        then verify nett=True subtracts consumption from aggregated."""
        agg_xml = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 1000.0), (2, 1100.0)],
            }],
            psr_type='B10',
            has_out_bidding_zone=False,
        )
        cons_xml = build_generation_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 200.0), (2, 300.0)],
            }],
            psr_type='B10',
            has_out_bidding_zone=True,
        )
        from bs4 import BeautifulSoup
        soup_agg = BeautifulSoup(agg_xml, 'html.parser')
        soup_cons = BeautifulSoup(cons_xml, 'html.parser')
        ts_agg = soup_agg.find('timeseries')
        ts_cons = soup_cons.find('timeseries')
        combined_xml = _wrap_document(str(ts_agg) + '\n' + str(ts_cons))

        result = parse_generation(combined_xml, per_plant=False, nett=True)

        assert isinstance(result, pd.DataFrame)
        psr_name = PSRTYPE_MAPPINGS['B10']  # 'Hydro Pumped Storage'
        assert psr_name in result.columns
        # Net = Aggregated - Consumption: 1000-200=800, 1100-300=800
        assert list(result[psr_name].values) == [800.0, 800.0]


# ---------------------------------------------------------------------------
# Crossborder flow parser — Requirements 7.1, 7.2, 7.3
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_crossborder_flows
from tests.conftest import build_crossborder_flow_xml, build_timeseries_xml


class TestParseCrossborderFlowsBasic:
    """Requirement 7.1: valid XML returns float Series with DatetimeIndex."""

    def test_single_timeseries_returns_float_series(self):
        xml = build_crossborder_flow_xml([{
            "start": "2023-01-01T00:00Z",
            "end": "2023-01-01T03:00Z",
            "resolution": "PT60M",
            "points": [(1, 150.0), (2, 200.5), (3, -50.0)],
        }])
        result = parse_crossborder_flows(xml)

        assert isinstance(result, pd.Series)
        assert result.dtype == float
        assert isinstance(result.index, pd.DatetimeIndex)
        assert len(result) == 3
        assert list(result.values) == [150.0, 200.5, -50.0]


class TestParseCrossborderFlowsMultiTimeseries:
    """Requirements 7.2, 7.3: multiple timeseries concatenated and sorted."""

    def test_multiple_timeseries_sorted(self):
        xml = build_crossborder_flow_xml([
            {
                "start": "2023-01-01T06:00Z",
                "end": "2023-01-01T08:00Z",
                "resolution": "PT60M",
                "points": [(1, 300.0), (2, 400.0)],
            },
            {
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0)],
            },
        ])
        result = parse_crossborder_flows(xml)

        assert isinstance(result, pd.Series)
        assert len(result) == 4
        assert result.index.is_monotonic_increasing
        # Values should follow sorted order: 00:00, 01:00, 06:00, 07:00
        assert list(result.values) == [100.0, 200.0, 300.0, 400.0]


# ---------------------------------------------------------------------------
# Property 8: Parsed multi-timeseries output is sorted
# ---------------------------------------------------------------------------


@given(
    n_series=st.integers(min_value=2, max_value=4),
    n_points=st.integers(min_value=1, max_value=6),
)
@settings(max_examples=100)
def test_property_multi_timeseries_sorted_output(n_series, n_points):
    """Property 8: Parsed multi-timeseries output is sorted.

    For all valid XML containing multiple timeseries, parsers that concatenate
    timeseries shall produce output with a monotonically increasing datetime index.

    """
    periods = []
    base = pd.Timestamp("2023-01-01T00:00Z")
    for i in range(n_series):
        start = base + pd.Timedelta(hours=i * n_points * 2)
        end = start + pd.Timedelta(hours=n_points)
        points = [(j + 1, float(j * 10 + i)) for j in range(n_points)]
        periods.append({
            "start": start.strftime('%Y-%m-%dT%H:%MZ'),
            "end": end.strftime('%Y-%m-%dT%H:%MZ'),
            "resolution": "PT60M",
            "points": points,
        })
    # Shuffle periods to test that sorting works regardless of input order
    import random
    random.shuffle(periods)

    xml = build_crossborder_flow_xml(periods)
    result = parse_crossborder_flows(xml)

    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.is_monotonic_increasing


# ---------------------------------------------------------------------------
# Property 9: Crossborder flow output structure
# ---------------------------------------------------------------------------


@given(
    n_points=st.integers(min_value=1, max_value=24),
    values=st.lists(
        st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=24,
    ),
)
@settings(max_examples=100)
def test_property_crossborder_flow_output_structure(n_points, values):
    """Property 9: Crossborder flow output structure.

    For all valid crossborder flow XML, parse_crossborder_flows shall return
    a pd.Series with float64 dtype and a DatetimeIndex.

    """
    values = values[:n_points]
    n = len(values)
    start = "2023-06-01T00:00Z"
    end_ts = pd.Timestamp(start) + pd.Timedelta(hours=n)
    end = end_ts.strftime('%Y-%m-%dT%H:%MZ')

    points = [(i + 1, round(v, 2)) for i, v in enumerate(values)]
    xml = build_crossborder_flow_xml([{
        "start": start,
        "end": end,
        "resolution": "PT60M",
        "points": points,
    }])

    result = parse_crossborder_flows(xml)

    assert isinstance(result, pd.Series)
    assert result.dtype == float
    assert isinstance(result.index, pd.DatetimeIndex)
    assert len(result) == n


# ---------------------------------------------------------------------------
# Net position parser — Requirements 10.1, 10.2, 10.3
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_netpositions


def _build_netposition_xml(periods: list, out_domain_mrid: str) -> str:
    """Build net position XML with out_domain.mrid element."""
    timeseries_parts = []
    for period in periods:
        points_xml = '\n'.join(
            f'        <point>\n'
            f'          <position>{pos}</position>\n'
            f'          <quantity>{val}</quantity>\n'
            f'        </point>'
            for pos, val in period['points']
        )
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <curvetype>A01</curvetype>\n'
            f'    <out_domain.mrid>{out_domain_mrid}</out_domain.mrid>\n'
            f'      <period>\n'
            f'        <timeinterval>\n'
            f'          <start>{period["start"]}</start>\n'
            f'          <end>{period["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{period["resolution"]}</resolution>\n'
            f'{points_xml}\n'
            f'      </period>\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


class TestParseNetpositionsRegion:
    """Requirement 10.1: out_domain containing 'REGION' multiplies by -1."""

    def test_region_domain_negates_values(self):
        xml = _build_netposition_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0), (3, 300.0)],
            }],
            out_domain_mrid="10Y_REGION_TEST__",
        )
        result = parse_netpositions(xml)

        assert isinstance(result, pd.Series)
        assert len(result) == 3
        # REGION → factor = -1, abs(value) * -1
        assert list(result.values) == [-100.0, -200.0, -300.0]


class TestParseNetpositionsNonRegion:
    """Requirement 10.2: out_domain not containing 'REGION' keeps positive."""

    def test_non_region_domain_keeps_positive(self):
        xml = _build_netposition_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0), (3, 300.0)],
            }],
            out_domain_mrid="10YDE-VE-------2",
        )
        result = parse_netpositions(xml)

        assert isinstance(result, pd.Series)
        assert len(result) == 3
        assert list(result.values) == [100.0, 200.0, 300.0]


class TestParseNetpositionsAbsBeforeSign:
    """Requirement 10.3: absolute value is applied before sign factor."""

    def test_negative_input_becomes_positive_for_non_region(self):
        xml = _build_netposition_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, -150.0), (2, -250.0)],
            }],
            out_domain_mrid="10YDE-VE-------2",
        )
        result = parse_netpositions(xml)

        # abs(-150) * 1 = 150, abs(-250) * 1 = 250
        assert list(result.values) == [150.0, 250.0]

    def test_negative_input_becomes_negative_for_region(self):
        xml = _build_netposition_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, -150.0), (2, -250.0)],
            }],
            out_domain_mrid="10Y_REGION_TEST__",
        )
        result = parse_netpositions(xml)

        # abs(-150) * -1 = -150, abs(-250) * -1 = -250
        assert list(result.values) == [-150.0, -250.0]


# ---------------------------------------------------------------------------
# Property 10: Net position sign convention
# ---------------------------------------------------------------------------


@given(
    quantities=st.lists(
        st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=12,
    ),
    is_region=st.booleans(),
)
@settings(max_examples=100)
def test_property_net_position_sign_convention(quantities, is_region):
    """Property 10: Net position sign convention.

    For all valid net position XML, when out_domain.mrid contains 'REGION',
    all output values shall be non-positive, and when it does not contain
    'REGION', all output values shall be non-negative. The absolute value of
    each output shall equal the absolute value of the corresponding input.

    """
    quantities = [round(q, 2) for q in quantities]
    n = len(quantities)
    start = "2023-01-01T00:00Z"
    end_ts = pd.Timestamp(start) + pd.Timedelta(hours=n)
    end = end_ts.strftime('%Y-%m-%dT%H:%MZ')

    domain = "10Y_REGION_TEST__" if is_region else "10YDE-VE-------2"
    points = [(i + 1, q) for i, q in enumerate(quantities)]

    xml = _build_netposition_xml(
        [{
            "start": start,
            "end": end,
            "resolution": "PT60M",
            "points": points,
        }],
        out_domain_mrid=domain,
    )
    result = parse_netpositions(xml)

    assert len(result) == n
    for i, q in enumerate(quantities):
        expected_abs = abs(q)
        actual = result.iloc[i]
        assert abs(actual) == pytest.approx(expected_abs, abs=1e-9)
        if is_region:
            assert actual <= 0.0 + 1e-9  # non-positive
        else:
            assert actual >= 0.0 - 1e-9  # non-negative


# ---------------------------------------------------------------------------
# Unavailability parser — Requirements 8.1, 8.2, 8.3, 8.4
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_unavailabilities, HEADERS_UNAVAIL_GEN, HEADERS_UNAVAIL_TRANSM
from tests.conftest import (
    build_unavailability_zip,
    _build_gen_unavailability_ts,
    _build_unavailability_xml,
)


class TestParseUnavailabilitiesGeneration:
    """Requirement 8.1: generation unavailability ZIP returns DataFrame with HEADERS_UNAVAIL_GEN columns."""

    def test_gen_unavailability_returns_correct_columns(self):
        zip_bytes = build_unavailability_zip()
        result = parse_unavailabilities(zip_bytes, doctype='A77')

        assert isinstance(result, pd.DataFrame)
        # Index is created_doc_time, remaining columns should match HEADERS_UNAVAIL_GEN minus the index
        expected_cols = [h for h in HEADERS_UNAVAIL_GEN if h != 'created_doc_time']
        assert list(result.columns) == expected_cols
        assert result.index.name == 'created_doc_time'
        assert len(result) > 0


class TestParseUnavailabilitiesTransmission:
    """Requirement 8.2: transmission unavailability ZIP returns DataFrame with HEADERS_UNAVAIL_TRANSM columns."""

    def test_transm_unavailability_returns_correct_columns(self):
        # Build a transmission unavailability ZIP
        # Transmission timeseries need in_domain and out_domain instead of plant info
        ts_xml = (
            '  <timeseries>\n'
            '    <businesstype>A53</businesstype>\n'
            '    <in_domain.mrid>10YCZ-CEPS-----N</in_domain.mrid>\n'
            '    <out_domain.mrid>10YDE-VE-------2</out_domain.mrid>\n'
            '    <quantity_measure_unit.name>MAW</quantity_measure_unit.name>\n'
            '    <curvetype>A01</curvetype>\n'
            '    <available_period>\n'
            '      <timeinterval>\n'
            '        <start>2023-01-01T00:00Z</start>\n'
            '        <end>2023-01-02T00:00Z</end>\n'
            '      </timeinterval>\n'
            '      <resolution>PT60M</resolution>\n'
            '      <point>\n'
            '        <position>1</position>\n'
            '        <quantity>500.0</quantity>\n'
            '      </point>\n'
            '    </available_period>\n'
            '  </timeseries>\n'
        )
        zip_bytes = build_unavailability_zip(
            entries=[{
                'created_datetime': '2023-06-15T10:00Z',
                'mrid': 'DOC_TM_001',
                'revision_number': 1,
                'docstatus_value': 'A05',
                'timeseries_xml': ts_xml,
            }],
            doctype='A78',
        )
        result = parse_unavailabilities(zip_bytes, doctype='A78')

        assert isinstance(result, pd.DataFrame)
        expected_cols = [h for h in HEADERS_UNAVAIL_TRANSM if h != 'created_doc_time']
        assert list(result.columns) == expected_cols
        assert result.index.name == 'created_doc_time'


class TestParseUnavailabilitiesEmpty:
    """Requirement 8.3: empty ZIP returns empty DataFrame with correct headers."""

    def test_empty_zip_returns_empty_dataframe(self):
        # Build a ZIP with no XML files
        import io, zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            pass  # empty ZIP
        zip_bytes = buf.getvalue()

        result = parse_unavailabilities(zip_bytes, doctype='A77')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert 'created_doc_time' in result.columns or result.index.name == 'created_doc_time'


class TestParseUnavailabilitiesSorted:
    """Requirement 8.4: output is sorted by created_doc_time index."""

    def test_output_sorted_by_created_doc_time(self):
        ts_xml = _build_gen_unavailability_ts()
        zip_bytes = build_unavailability_zip(
            entries=[
                {
                    'created_datetime': '2023-06-20T10:00Z',
                    'mrid': 'DOC002',
                    'revision_number': 1,
                    'docstatus_value': 'A05',
                    'timeseries_xml': ts_xml,
                },
                {
                    'created_datetime': '2023-06-10T08:00Z',
                    'mrid': 'DOC001',
                    'revision_number': 1,
                    'docstatus_value': 'A05',
                    'timeseries_xml': ts_xml,
                },
            ],
        )
        result = parse_unavailabilities(zip_bytes, doctype='A77')

        assert isinstance(result, pd.DataFrame)
        assert result.index.name == 'created_doc_time'
        assert result.index.is_monotonic_increasing


# ---------------------------------------------------------------------------
# Property 26: Unavailability output sorted by created_doc_time
# ---------------------------------------------------------------------------


@given(
    n_entries=st.integers(min_value=1, max_value=4),
    base_hour=st.integers(min_value=0, max_value=23),
)
@settings(max_examples=100)
def test_property_unavailability_sorted_by_created_doc_time(n_entries, base_hour):
    """Property 26: Unavailability output sorted by created_doc_time.

    For all valid unavailability ZIP archives containing at least one XML file,
    the parsed DataFrame index (created_doc_time) shall be monotonically increasing.

    """
    ts_xml = _build_gen_unavailability_ts()
    entries = []
    for i in range(n_entries):
        # Create entries with varying timestamps (not necessarily sorted)
        hour = (base_hour + i * 3) % 24
        day = 10 + (i * 5) % 20
        entries.append({
            'created_datetime': f'2023-06-{day:02d}T{hour:02d}:00Z',
            'mrid': f'DOC{i:03d}',
            'revision_number': 1,
            'docstatus_value': 'A05',
            'timeseries_xml': ts_xml,
        })

    zip_bytes = build_unavailability_zip(entries=entries)
    result = parse_unavailabilities(zip_bytes, doctype='A77')

    assert isinstance(result, pd.DataFrame)
    assert result.index.name == 'created_doc_time'
    assert result.index.is_monotonic_increasing


# ---------------------------------------------------------------------------
# Imbalance price and volume ZIP parsing — Requirements 9.1, 9.2, 9.3, 9.4
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_imbalance_prices_zip, parse_imbalance_volumes_zip
from tests.conftest import build_imbalance_zip, _build_imbalance_price_xml, _build_imbalance_volume_xml


class TestParseImbalancePricesZip:
    """Requirement 9.1: imbalance price ZIP returns sorted DataFrame with Long and Short columns."""

    def test_price_zip_returns_long_short_columns(self):
        price_xml = _build_imbalance_price_xml([{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-01T01:00Z',
            'resolution': 'PT15M',
            'points': [
                (1, 50.0, 'A04'),   # Long
                (2, 55.0, 'A05'),   # Short
                (3, 52.0, 'A04'),   # Long
                (4, 48.0, 'A05'),   # Short
            ],
        }])
        zip_bytes = build_imbalance_zip(xml_contents=[price_xml], kind='price')
        result = parse_imbalance_prices_zip(zip_bytes)

        assert isinstance(result, pd.DataFrame)
        assert result.index.is_monotonic_increasing
        assert 'Long' in result.columns or 'Short' in result.columns


class TestParseImbalanceVolumesZip:
    """Requirement 9.2: imbalance volume ZIP returns sorted DataFrame with Imbalance Volume values."""

    def test_volume_zip_returns_imbalance_volume(self):
        vol_xml = _build_imbalance_volume_xml([{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-01T01:00Z',
            'resolution': 'PT15M',
            'points': [(1, 100.0), (2, 200.0), (3, 150.0), (4, 175.0)],
        }], flow_direction='A01')
        zip_bytes = build_imbalance_zip(xml_contents=[vol_xml], kind='volume')
        result = parse_imbalance_volumes_zip(zip_bytes)

        assert isinstance(result, pd.DataFrame)
        assert 'Imbalance Volume' in result.columns
        assert result.index.is_monotonic_increasing
        assert len(result) == 4
        assert list(result['Imbalance Volume'].values) == [100.0, 200.0, 150.0, 175.0]


class TestParseImbalanceVolumesIncludeResolution:
    """Requirement 9.3: include_resolution=True adds Resolution columns."""

    def test_include_resolution_adds_column(self):
        vol_xml = _build_imbalance_volume_xml([{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-01T01:00Z',
            'resolution': 'PT15M',
            'points': [(1, 100.0), (2, 200.0), (3, 150.0), (4, 175.0)],
        }], flow_direction='A01')
        zip_bytes = build_imbalance_zip(xml_contents=[vol_xml], kind='volume')
        result = parse_imbalance_volumes_zip(zip_bytes, include_resolution=True)

        assert isinstance(result, pd.DataFrame)
        assert 'Resolution' in result.columns
        assert all(result['Resolution'] == '15min')


class TestParseImbalanceVolumesA02Negation:
    """Requirement 9.4: flow direction A02 multiplies volume by -1."""

    def test_a02_negates_volume(self):
        vol_xml = _build_imbalance_volume_xml([{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-01T01:00Z',
            'resolution': 'PT15M',
            'points': [(1, 100.0), (2, 200.0), (3, 150.0), (4, 175.0)],
        }], flow_direction='A02')
        zip_bytes = build_imbalance_zip(xml_contents=[vol_xml], kind='volume')
        result = parse_imbalance_volumes_zip(zip_bytes)

        assert isinstance(result, pd.DataFrame)
        assert 'Imbalance Volume' in result.columns
        # A02 (out) → multiply by -1
        assert list(result['Imbalance Volume'].values) == [-100.0, -200.0, -150.0, -175.0]


# ---------------------------------------------------------------------------
# Property 25: Imbalance volume A02 sign negation
# ---------------------------------------------------------------------------


@given(
    quantities=st.lists(
        st.floats(min_value=0.1, max_value=1000.0, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=8,
    ),
)
@settings(max_examples=100)
def test_property_imbalance_volume_a02_sign_negation(quantities):
    """Property 25: Imbalance volume A02 sign negation.

    For all imbalance volume XML with flow direction A02 (out), the parsed
    volume values shall be the negation of the raw quantity values in the XML.

    """
    quantities = [round(q, 2) for q in quantities]
    n = len(quantities)
    start = "2023-01-01T00:00Z"
    end_ts = pd.Timestamp(start) + pd.Timedelta(minutes=n * 15)
    end = end_ts.strftime('%Y-%m-%dT%H:%MZ')

    points = [(i + 1, q) for i, q in enumerate(quantities)]
    vol_xml = _build_imbalance_volume_xml([{
        'start': start,
        'end': end,
        'resolution': 'PT15M',
        'points': points,
    }], flow_direction='A02')
    zip_bytes = build_imbalance_zip(xml_contents=[vol_xml], kind='volume')
    result = parse_imbalance_volumes_zip(zip_bytes)

    assert len(result) == n
    for i, q in enumerate(quantities):
        assert result['Imbalance Volume'].iloc[i] == pytest.approx(-q, abs=1e-9)


# ---------------------------------------------------------------------------
# Contracted reserve parser — Requirements 22.1, 22.2, 22.3
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_contracted_reserve
from entsoe.mappings import BSNTYPE


def _build_contracted_reserve_xml(
    periods: list,
    business_type: str = 'A95',
    flow_direction: str = 'A01',
    curve_type: str = 'A01',
    mrid: int = 1,
    label: str = 'quantity',
) -> str:
    """Build contracted reserve XML with business type and flow direction."""
    timeseries_parts = []
    for period in periods:
        points_xml = '\n'.join(
            f'        <point>\n'
            f'          <position>{pos}</position>\n'
            f'          <{label}>{val}</{label}>\n'
            f'        </point>'
            for pos, val in period['points']
        )
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <mrid>{mrid}</mrid>\n'
            f'    <businesstype>{business_type}</businesstype>\n'
            f'    <flowdirection.direction>{flow_direction}</flowdirection.direction>\n'
            f'    <curvetype>{curve_type}</curvetype>\n'
            f'      <period>\n'
            f'        <timeinterval>\n'
            f'          <start>{period["start"]}</start>\n'
            f'          <end>{period["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{period["resolution"]}</resolution>\n'
            f'{points_xml}\n'
            f'      </period>\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


class TestParseContractedReserveMultiIndex:
    """Requirement 22.1: valid XML returns DataFrame with MultiIndex columns (reserve type × direction)."""

    def test_multiindex_columns(self):
        xml = _build_contracted_reserve_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0), (3, 300.0)],
            }],
            business_type='A96',
            flow_direction='A01',
        )
        result = parse_contracted_reserve(xml, tz=None, label='quantity')

        assert isinstance(result, pd.DataFrame)
        assert isinstance(result.columns, pd.MultiIndex)
        assert len(result) == 3
        # Column should be (reserve_type_name, direction)
        reserve_name = BSNTYPE['A96']  # 'Automatic frequency restoration reserve'
        col = result.columns[0]
        assert col[0] == reserve_name
        assert col[1] == 'Up'


class TestParseContractedReserveBSNTYPE:
    """Requirement 22.2: business type codes map to reserve type names via BSNTYPE."""

    def test_business_type_mapping(self):
        for btype in ['A95', 'A96', 'A97', 'A98']:
            xml = _build_contracted_reserve_xml(
                [{
                    "start": "2023-01-01T00:00Z",
                    "end": "2023-01-01T02:00Z",
                    "resolution": "PT60M",
                    "points": [(1, 50.0), (2, 60.0)],
                }],
                business_type=btype,
                flow_direction='A01',
            )
            result = parse_contracted_reserve(xml, tz=None, label='quantity')
            col = result.columns[0]
            assert col[0] == BSNTYPE[btype]


class TestParseContractedReserveDirection:
    """Requirement 22.3: flow direction codes A01→Up, A02→Down."""

    def test_a01_maps_to_up(self):
        xml = _build_contracted_reserve_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 50.0), (2, 60.0)],
            }],
            flow_direction='A01',
        )
        result = parse_contracted_reserve(xml, tz=None, label='quantity')
        col = result.columns[0]
        assert col[1] == 'Up'

    def test_a02_maps_to_down(self):
        xml = _build_contracted_reserve_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 50.0), (2, 60.0)],
            }],
            flow_direction='A02',
        )
        result = parse_contracted_reserve(xml, tz=None, label='quantity')
        col = result.columns[0]
        assert col[1] == 'Down'


# ---------------------------------------------------------------------------
# Aggregated bids parser — Requirements 23.1, 23.2, 23.3
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_aggregated_bids


def _build_aggregated_bids_xml(
    periods: list,
    flow_direction: str = 'A01',
    curve_type: str = 'A01',
    mrid: int = 1,
    include_secondary: bool = False,
) -> str:
    """Build aggregated bids XML with flow direction and optional secondary quantity."""
    timeseries_parts = []
    for period in periods:
        points_xml = ''
        for pos, qty in period['points']:
            secondary_xml = ''
            if include_secondary:
                secondary_xml = f'          <secondaryquantity>{qty * 0.5}</secondaryquantity>\n'
            points_xml += (
                f'        <point>\n'
                f'          <position>{pos}</position>\n'
                f'          <quantity>{qty}</quantity>\n'
                f'{secondary_xml}'
                f'        </point>\n'
            )
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <mrid>{mrid}</mrid>\n'
            f'    <flowdirection.direction>{flow_direction}</flowdirection.direction>\n'
            f'    <curvetype>{curve_type}</curvetype>\n'
            f'      <period>\n'
            f'        <timeinterval>\n'
            f'          <start>{period["start"]}</start>\n'
            f'          <end>{period["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{period["resolution"]}</resolution>\n'
            f'{points_xml}'
            f'      </period>\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


class TestParseAggregatedBidsBasic:
    """Requirement 23.1: valid XML returns DataFrame indexed by timestamps."""

    def test_returns_dataframe_with_timestamp_index(self):
        xml = _build_aggregated_bids_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0), (3, 300.0)],
            }],
            include_secondary=True,
        )
        result = parse_aggregated_bids(xml)

        assert isinstance(result, pd.DataFrame)
        assert isinstance(result.index, pd.DatetimeIndex)
        assert len(result) == 3


class TestParseAggregatedBidsA03ForwardFill:
    """Requirement 23.2: A03 curve type forward-fills missing positions."""

    def test_a03_forward_fills(self):
        # A03 with sparse positions: only positions 1 and 3 out of 4
        xml = _build_aggregated_bids_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T04:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (3, 300.0)],
            }],
            curve_type='A03',
            include_secondary=False,
        )
        result = parse_aggregated_bids(xml)

        assert isinstance(result, pd.DataFrame)
        # A03 should produce a continuous index with forward-fill
        assert len(result) == 4


class TestParseAggregatedBidsMultipleBusinessTypes:
    """Requirement 23.3: multiple timeseries with different business types create separate columns."""

    def test_multiple_timeseries_separate_columns(self):
        # Two timeseries with different flow directions → separate column groups
        xml1 = _build_aggregated_bids_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 100.0), (2, 200.0)],
            }],
            flow_direction='A01',
            mrid=1,
            include_secondary=True,
        )
        xml2 = _build_aggregated_bids_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 50.0), (2, 60.0)],
            }],
            flow_direction='A02',
            mrid=2,
            include_secondary=True,
        )
        # Combine both timeseries into one document
        from bs4 import BeautifulSoup
        soup1 = BeautifulSoup(xml1, 'html.parser')
        soup2 = BeautifulSoup(xml2, 'html.parser')
        ts1 = soup1.find('timeseries')
        ts2 = soup2.find('timeseries')
        combined_xml = _wrap_document(str(ts1) + '\n' + str(ts2))

        result = parse_aggregated_bids(combined_xml)

        assert isinstance(result, pd.DataFrame)
        assert isinstance(result.columns, pd.MultiIndex)
        # Should have columns for both Up and Down directions
        directions = result.columns.get_level_values('direction').unique()
        assert 'Up' in directions
        assert 'Down' in directions


# ---------------------------------------------------------------------------
# Activated balancing energy prices parser — Requirements 25.1, 25.2, 25.3
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_activated_balancing_energy_prices


def _build_activated_balancing_energy_prices_xml(
    periods: list,
    flow_direction: str = 'A01',
    business_type: str = 'A96',
) -> str:
    """Build activated balancing energy prices XML."""
    timeseries_parts = []
    for period in periods:
        points_xml = '\n'.join(
            f'        <point>\n'
            f'          <position>{pos}</position>\n'
            f'          <activation_price.amount>{price}</activation_price.amount>\n'
            f'        </point>'
            for pos, price in period['points']
        )
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <businesstype>{business_type}</businesstype>\n'
            f'    <flowdirection.direction>{flow_direction}</flowdirection.direction>\n'
            f'    <curvetype>A01</curvetype>\n'
            f'      <period>\n'
            f'        <timeinterval>\n'
            f'          <start>{period["start"]}</start>\n'
            f'          <end>{period["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{period["resolution"]}</resolution>\n'
            f'{points_xml}\n'
            f'      </period>\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


class TestParseActivatedBalancingEnergyPricesBasic:
    """Requirement 25.1: valid XML returns DataFrame with Price, Direction, ReserveType columns."""

    def test_returns_dataframe_with_correct_columns(self):
        xml = _build_activated_balancing_energy_prices_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [(1, 10.0), (2, 20.0), (3, 30.0)],
            }],
            flow_direction='A01',
            business_type='A96',
        )
        result = parse_activated_balancing_energy_prices(xml)

        assert isinstance(result, pd.DataFrame)
        assert 'Price' in result.columns
        assert 'Direction' in result.columns
        assert 'ReserveType' in result.columns
        assert len(result) == 3


class TestParseActivatedBalancingEnergyPricesMapping:
    """Requirement 25.2: flow direction and business type code mapping."""

    def test_direction_and_reserve_type_mapping(self):
        test_cases = [
            ('A01', 'A95', 'Up', 'FCR'),
            ('A02', 'A96', 'Down', 'aFRR'),
            ('A01', 'A97', 'Up', 'mFRR'),
            ('A02', 'A98', 'Down', 'RR'),
        ]
        for flow_dir, btype, expected_dir, expected_reserve in test_cases:
            xml = _build_activated_balancing_energy_prices_xml(
                [{
                    "start": "2023-01-01T00:00Z",
                    "end": "2023-01-01T02:00Z",
                    "resolution": "PT60M",
                    "points": [(1, 10.0), (2, 20.0)],
                }],
                flow_direction=flow_dir,
                business_type=btype,
            )
            result = parse_activated_balancing_energy_prices(xml)

            assert result['Direction'].iloc[0] == expected_dir
            assert result['ReserveType'].iloc[0] == expected_reserve


class TestParseActivatedBalancingEnergyPricesForwardFill:
    """Requirement 25.3: forward-fill of missing price values."""

    def test_forward_fill_missing_prices(self):
        # Only provide positions 1 and 3 out of 4 — position 2 should be forward-filled
        xml = _build_activated_balancing_energy_prices_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T04:00Z",
                "resolution": "PT60M",
                "points": [(1, 10.0), (3, 30.0)],
            }],
        )
        result = parse_activated_balancing_energy_prices(xml)

        assert len(result) == 4
        # Position 1 → 10.0, Position 2 → forward-filled from 10.0,
        # Position 3 → 30.0, Position 4 → forward-filled from 30.0
        assert float(result['Price'].iloc[0]) == 10.0
        assert float(result['Price'].iloc[1]) == 10.0  # forward-filled
        assert float(result['Price'].iloc[2]) == 30.0
        assert float(result['Price'].iloc[3]) == 30.0  # forward-filled


# ---------------------------------------------------------------------------
# Procured balancing capacity parser — Requirements 24.1, 24.2, 24.3
# ---------------------------------------------------------------------------

from entsoe.parsers import parse_procured_balancing_capacity


def _build_procured_balancing_capacity_xml(
    periods: list,
    flow_direction: str = 'A01',
    mrid: int = 1,
) -> str:
    """Build procured balancing capacity XML with Price and Volume per point."""
    timeseries_parts = []
    for period in periods:
        points_xml = '\n'.join(
            f'        <point>\n'
            f'          <position>{pos}</position>\n'
            f'          <procurement_price.amount>{price}</procurement_price.amount>\n'
            f'          <quantity>{vol}</quantity>\n'
            f'        </point>'
            for pos, price, vol in period['points']
        )
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <mrid>{mrid}</mrid>\n'
            f'    <flowdirection.direction>{flow_direction}</flowdirection.direction>\n'
            f'    <curvetype>A01</curvetype>\n'
            f'      <period>\n'
            f'        <timeinterval>\n'
            f'          <start>{period["start"]}</start>\n'
            f'          <end>{period["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{period["resolution"]}</resolution>\n'
            f'{points_xml}\n'
            f'      </period>\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


class TestParseProcuredBalancingCapacityTimezone:
    """Requirement 24.1: valid XML with timezone returns DataFrame with timezone-aware index."""

    def test_timezone_aware_index(self):
        xml = _build_procured_balancing_capacity_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T03:00Z",
                "resolution": "PT60M",
                "points": [
                    (1, 10.0, 500.0),
                    (2, 15.0, 600.0),
                    (3, 20.0, 700.0),
                ],
            }],
        )
        result = parse_procured_balancing_capacity(xml, tz='Europe/Berlin')

        assert isinstance(result, pd.DataFrame)
        assert result.index.tz is not None
        assert len(result) == 3


class TestParseProcuredBalancingCapacityMapping:
    """Requirement 24.2: direction codes and business types map to readable column names."""

    def test_direction_mapping(self):
        xml_up = _build_procured_balancing_capacity_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 10.0, 500.0), (2, 15.0, 600.0)],
            }],
            flow_direction='A01',
        )
        result_up = parse_procured_balancing_capacity(xml_up, tz='Europe/Berlin')
        directions = result_up.columns.get_level_values('direction').unique()
        assert 'Up' in directions

        xml_down = _build_procured_balancing_capacity_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 10.0, 500.0), (2, 15.0, 600.0)],
            }],
            flow_direction='A02',
        )
        result_down = parse_procured_balancing_capacity(xml_down, tz='Europe/Berlin')
        directions = result_down.columns.get_level_values('direction').unique()
        assert 'Down' in directions


class TestParseProcuredBalancingCapacityMultiTimeseries:
    """Requirement 24.3: multiple timeseries concatenate into single DataFrame."""

    def test_multiple_timeseries_concatenated(self):
        xml1 = _build_procured_balancing_capacity_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 10.0, 500.0), (2, 15.0, 600.0)],
            }],
            flow_direction='A01',
            mrid=1,
        )
        xml2 = _build_procured_balancing_capacity_xml(
            [{
                "start": "2023-01-01T00:00Z",
                "end": "2023-01-01T02:00Z",
                "resolution": "PT60M",
                "points": [(1, 20.0, 700.0), (2, 25.0, 800.0)],
            }],
            flow_direction='A02',
            mrid=2,
        )
        # Combine both timeseries into one document
        from bs4 import BeautifulSoup
        soup1 = BeautifulSoup(xml1, 'html.parser')
        soup2 = BeautifulSoup(xml2, 'html.parser')
        ts1 = soup1.find('timeseries')
        ts2 = soup2.find('timeseries')
        combined_xml = _wrap_document(str(ts1) + '\n' + str(ts2))

        result = parse_procured_balancing_capacity(combined_xml, tz='Europe/Berlin')

        assert isinstance(result, pd.DataFrame)
        assert isinstance(result.columns, pd.MultiIndex)
        # Should have both Up and Down directions
        directions = result.columns.get_level_values('direction').unique()
        assert 'Up' in directions
        assert 'Down' in directions
        assert len(result) == 2
