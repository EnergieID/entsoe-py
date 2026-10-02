from entsoe.parsers import parse_activated_balancing_energy_prices
from entsoe.series_parsers import _resolution_to_timedelta
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "resolution, expected",
    [("PT15M", "15min"), ("PT1M", "1min"), ("PT4S", "4s")],
)
def test_resolution_to_timedelta(resolution, expected):
    assert _resolution_to_timedelta(resolution) == expected


def test_resolution_to_timedelta_unknown():
    with pytest.raises(NotImplementedError):
        _resolution_to_timedelta("PT2S")


def test_parse_activated_balancing_energy_prices_missing_amount():
    xml = """<Balancing_MarketDocument><TimeSeries>
        <businessType>A96</businessType>
        <flowDirection.direction>A01</flowDirection.direction>
        <Period>
            <timeInterval><start>2026-03-01T00:00Z</start><end>2026-03-01T01:00Z</end></timeInterval>
            <resolution>PT15M</resolution>
            <Point><position>1</position><activation_Price.amount>10.5</activation_Price.amount></Point>
            <Point><position>2</position></Point>
            <Point><position>3</position><activation_Price.amount>12.0</activation_Price.amount></Point>
        </Period>
    </TimeSeries></Balancing_MarketDocument>"""
    df = parse_activated_balancing_energy_prices(xml)
    # point 2 has no price -> NaN, point 4 is missing entirely -> forward filled
    assert len(df) == 4
    assert pd.isna(df['Price'].iloc[1])
    assert df['Price'].iloc[3] == 12.0
    assert (df['ReserveType'] == 'aFRR').all()
