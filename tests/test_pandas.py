from itertools import product
import os

from dotenv import load_dotenv
from entsoe import EntsoePandasClient
import pandas as pd
import pytest

load_dotenv()

API_KEY = os.getenv("API_KEY")


@pytest.fixture
def client():
    yield EntsoePandasClient(api_key=API_KEY)


@pytest.fixture
def start():
    return pd.Timestamp("20171201", tz="Europe/Brussels")


@pytest.fixture
def end():
    return pd.Timestamp("20180101", tz="Europe/Brussels")


@pytest.fixture
def country_code():
    return "BE"  # Belgium


@pytest.fixture
def country_code_from():
    return "FR"  # France


@pytest.fixture
def country_code_to():
    return "DE_LU"  # Germany-Luxembourg


STARTS = [pd.Timestamp("20171201", tz="Europe/Brussels")]
ENDS = [pd.Timestamp("20180101", tz="Europe/Brussels")]
COUNTRY_CODES = ["BE"]  # Belgium
COUNTRY_CODES_FROM = ["FR"]  # France
COUNTRY_CODES_TO = ["DE_LU"]  # Germany-Luxembourg

BASIC_QUERIES_SERIES = [
    "query_day_ahead_prices",
    "query_net_position_dayahead",
    "query_load",
    "query_load_forecast",
]

BASIC_QUERIES_DATAFRAME = [
    "query_wind_and_solar_forecast",
    "query_generation_forecast",
    "query_generation",
    "query_generation_per_plant",
    "query_installed_generation_capacity",
    "query_installed_generation_capacity_per_unit",
    "query_imbalance_prices",
    "query_withdrawn_unavailability_of_generation_units",
    "query_unavailability_of_generation_units",
    "query_unavailability_of_production_units",
    "query_import",
    "query_generation_import",
]

CROSSBORDER_QUERIES = [
    "query_crossborder_flows",
    "query_scheduled_exchanges",
    "query_net_transfer_capacity_dayahead",
    "query_net_transfer_capacity_weekahead",
    "query_net_transfer_capacity_monthahead",
    "query_net_transfer_capacity_yearahead",
    "query_intraday_offered_capacity",
]

# pandas.Series

@pytest.mark.parametrize(
    "country_code, start, end, query",
    product(COUNTRY_CODES, STARTS, ENDS, BASIC_QUERIES_SERIES),
)
def test_basic_queries_series(client, query, country_code, start, end):
    result = getattr(client, query)(country_code, start=start, end=end)
    assert not result.empty


@pytest.mark.parametrize(
    "country_code_from, country_code_to, start, end, query",
    product(COUNTRY_CODES_FROM, COUNTRY_CODES_TO, STARTS, ENDS, CROSSBORDER_QUERIES),
)
def test_crossborder_queries(
    client, query, country_code_from, country_code_to, start, end
):
    result = getattr(client, query)(country_code_from, country_code_to, start=start, end=end)
    assert not result.empty

# pandas.DataFrames

@pytest.mark.parametrize(
    "country_code, start, end, query",
    product(COUNTRY_CODES, STARTS, ENDS, BASIC_QUERIES_DATAFRAME),
)
def test_basic_queries_dataframe(client, query, country_code, start, end):
    result = getattr(client, query)(country_code, start=start, end=end)
    assert not result.empty


def test_query_contracted_reserve_prices(client, country_code, start, end):
    type_marketagreement_type = "A01"
    result = client.query_contracted_reserve_prices(
        country_code, start=start, end=end, type_marketagreement_type=type_marketagreement_type
    )
    assert not result.empty


def test_query_contracted_reserve_amount(client, country_code, start, end):
    type_marketagreement_type = "A01"
    result = client.query_contracted_reserve_amount(
        country_code, start=start, end=end, type_marketagreement_type=type_marketagreement_type
    )
    assert not result.empty


def test_query_unavailability_transmission(client, country_code_from, country_code_to, start, end):
    result = client.query_unavailability_transmission(
        country_code_from, country_code_to, start=start, end=end
    )
    assert not result.empty


def test_query_procured_balancing_capacity_process_type_not_allowed(client, country_code, start, end):
    process_type = "A01"
    with pytest.raises(ValueError):
        client.query_procured_balancing_capacity(
            country_code, start=start, end=end, process_type=process_type
        )


def test_query_procured_balancing_capacity(client, country_code, start, end):
    process_type = "A47"
    result = client.query_procured_balancing_capacity(
        country_code, start=start, end=end, process_type=process_type
    )
    assert not result.empty
