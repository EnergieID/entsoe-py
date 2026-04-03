"""
Offline unit tests for entsoe.parsers.

Covers five public parsers against synthetic XML fixtures. The intent is
to lock in output *shape* (types, columns, index, lengths) plus a
handful of representative values, so that upstream schema drift or
refactors that change behaviour fail loudly and locally rather than at
runtime against the live API.

Fixtures live in tests/fixtures/ and are documented inline.
"""
import pandas as pd
import pytest

from entsoe.parsers import (
    parse_crossborder_flows,
    parse_generation,
    parse_installed_capacity_per_plant,
    parse_loads,
    parse_prices,
)


# --- parse_prices --------------------------------------------------------

def test_parse_prices_shape(load_fixture):
    xml = load_fixture("prices.xml")
    result = parse_prices(xml)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"15min", "30min", "60min"}
    assert all(isinstance(s, pd.Series) for s in result.values())


def test_parse_prices_concatenates_timeseries(load_fixture):
    """Two PT60M timeseries should concatenate into one sorted series."""
    xml = load_fixture("prices.xml")
    result = parse_prices(xml)

    s60 = result["60min"]
    assert len(s60) == 4
    assert s60.dtype == float
    assert s60.index.is_monotonic_increasing
    assert s60.iloc[0] == 50.25
    assert s60.iloc[-1] == 55.10
    # buckets for resolutions not present in the fixture stay empty
    assert len(result["15min"]) == 0
    assert len(result["30min"]) == 0


# --- parse_loads ---------------------------------------------------------

def test_parse_loads_a01_forecasted(load_fixture):
    xml = load_fixture("loads.xml")
    result = parse_loads(xml, process_type="A01")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["Forecasted Load"]
    assert len(result) == 3
    assert result["Forecasted Load"].iloc[0] == 5000.0
    assert result["Forecasted Load"].iloc[2] == 5200.0
    assert result.index.is_monotonic_increasing


def test_parse_loads_a16_actual(load_fixture):
    """A16 uses the same parse path as A01 but labels the column differently."""
    xml = load_fixture("loads.xml")
    result = parse_loads(xml, process_type="A16")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["Actual Load"]
    assert len(result) == 3
    assert result["Actual Load"].iloc[1] == 5100.0


# --- parse_crossborder_flows ---------------------------------------------

def test_parse_crossborder_flows(load_fixture):
    xml = load_fixture("crossborder_flows.xml")
    result = parse_crossborder_flows(xml)

    assert isinstance(result, pd.Series)
    assert result.dtype == float
    # One timeseries with two periods of two points each -> 4 rows.
    assert len(result) == 4
    assert result.index.is_monotonic_increasing
    assert result.iloc[0] == 1200.0
    assert result.iloc[-1] == 1500.0


# --- parse_installed_capacity_per_plant ----------------------------------
# This parser is the one most sensitive to ENTSO-E schema drift: the
# tag names it reads (registeredresource.*, production_powersystemresources.*)
# are looked up literally, and issue #513 -> PR #514 was exactly a silent
# tag rename upstream. The fixture pins the set of tags the parser
# currently requires.

EXPECTED_CAPACITY_COLUMNS = [
    "Name",
    "Production Type",
    "Bidding Zone",
    "Voltage Connection Level [kV]",
    "Start",
    "Installed Capacity [MW]",
]


def test_parse_installed_capacity_per_plant_shape(load_fixture):
    xml = load_fixture("installed_capacity_per_plant.xml")
    result = parse_installed_capacity_per_plant(xml)

    assert isinstance(result, pd.DataFrame)
    # One row per plant, indexed by registeredresource.mrid.
    assert result.shape == (3, 6)
    assert list(result.columns) == EXPECTED_CAPACITY_COLUMNS
    assert set(result.index) == {
        "22WPLANT-NUKE-01",
        "22WPLANT-SOLR-02",
        "22WPLANT-WIND-03",
    }


def test_parse_installed_capacity_per_plant_values(load_fixture):
    xml = load_fixture("installed_capacity_per_plant.xml")
    result = parse_installed_capacity_per_plant(xml)

    nuke = result.loc["22WPLANT-NUKE-01"]
    assert nuke["Name"] == "Test Nuclear Plant"
    assert nuke["Production Type"] == "Nuclear"  # B14 -> PSRTYPE_MAPPINGS
    assert nuke["Voltage Connection Level [kV]"] == "380"
    assert nuke["Installed Capacity [MW]"] == "1000.0"
    assert nuke["Start"] == pd.Timestamp("2026-01-01T00:00Z")

    solar = result.loc["22WPLANT-SOLR-02"]
    assert solar["Production Type"] == "Solar"  # B16
    assert solar["Installed Capacity [MW]"] == "250.0"

    wind = result.loc["22WPLANT-WIND-03"]
    assert wind["Production Type"] == "Wind Onshore"  # B19


# --- parse_generation ----------------------------------------------------

def test_parse_generation_drops_redundant_level(load_fixture):
    """
    When all timeseries share the same direction (inbiddingzone ->
    'Actual Aggregated'), that last level is redundant and
    _calc_nett_and_drop_redundant_columns drops it, leaving columns
    named directly by production type.
    """
    xml = load_fixture("generation.xml")
    result = parse_generation(xml)

    assert isinstance(result, pd.DataFrame)
    assert result.columns.nlevels == 1
    assert set(result.columns) == {"Nuclear", "Solar"}
    assert len(result) == 3
    assert result["Nuclear"].iloc[0] == 900.0
    assert result["Solar"].iloc[2] == 180.0


def test_parse_generation_mixed_direction_keeps_multiindex(load_fixture):
    """
    When the same production type reports both inbiddingzone and
    outbiddingzone timeseries, the parser must keep the 2-level
    MultiIndex (type, metric) so callers can distinguish generation
    from consumption.
    """
    xml = load_fixture("generation_mixed.xml")
    result = parse_generation(xml)

    assert isinstance(result, pd.DataFrame)
    assert result.columns.nlevels == 2
    cols = set(result.columns)
    assert ("Hydro Pumped Storage", "Actual Aggregated") in cols
    assert ("Hydro Pumped Storage", "Actual Consumption") in cols
    assert result[("Hydro Pumped Storage", "Actual Aggregated")].iloc[0] == 300.0
    assert result[("Hydro Pumped Storage", "Actual Consumption")].iloc[0] == 50.0


def test_parse_generation_nett(load_fixture):
    """nett=True collapses Aggregated - Consumption into a single column."""
    xml = load_fixture("generation_mixed.xml")
    result = parse_generation(xml, nett=True)

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["Hydro Pumped Storage"]
    # 300 - 50 = 250, 310 - 60 = 250
    assert result["Hydro Pumped Storage"].iloc[0] == 250.0
    assert result["Hydro Pumped Storage"].iloc[1] == 250.0
