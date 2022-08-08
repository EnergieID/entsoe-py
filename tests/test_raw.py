from itertools import product

from bs4 import BeautifulSoup
from entsoe import EntsoeRawClient
from entsoe.exceptions import PaginationError
import pandas as pd
import pytest

from settings import api_key


@pytest.fixture
def client():
    yield EntsoeRawClient(api_key=api_key)


def valid_xml(s: str) -> bool:
    try:
        BeautifulSoup(s, "html.parser")
        return True
    except Exception:
        return False


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

BASIC_QUERIES = [
    "query_day_ahead_prices",
    "query_net_position_dayahead",
    "query_load",
    "query_load_forecast",
    "query_wind_and_solar_forecast",
    "query_generation_forecast",
    "query_generation",
    "query_generation_per_plant",
    "query_installed_generation_capacity",
    "query_installed_generation_capacity_per_unit",
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

# XML

@pytest.mark.parametrize(
    "country_code, start, end, query",
    product(COUNTRY_CODES, STARTS, ENDS, BASIC_QUERIES),
)
def test_basic_queries(client, query, country_code, start, end):
    result = getattr(client, query)(country_code, start, end)
    assert isinstance(result, str)
    assert valid_xml(result)


@pytest.mark.parametrize(
    "country_code_from, country_code_to, start, end, query",
    product(COUNTRY_CODES_FROM, COUNTRY_CODES_TO, STARTS, ENDS, CROSSBORDER_QUERIES),
)
def test_crossborder_queries(
    client, query, country_code_from, country_code_to, start, end
):
    result = getattr(client, query)(country_code_from, country_code_to, start, end)
    assert isinstance(result, str)
    assert valid_xml(result)


def test_query_contracted_reserve_prices(client, country_code, start, end):
    type_marketagreement_type = "A01"
    result = client.query_contracted_reserve_prices(
        country_code, start, end, type_marketagreement_type
    )
    assert isinstance(result, str)
    assert valid_xml(result)


def test_query_contracted_reserve_amount(client, country_code, start, end):
    type_marketagreement_type = "A01"
    result = client.query_contracted_reserve_amount(
        country_code, start, end, type_marketagreement_type
    )
    assert isinstance(result, str)
    assert valid_xml(result)


def test_query_procured_balancing_capacity_bytearray(client, country_code, start, end):
    type_marketagreement_type = "A01"
    process_type = "A47"
    result = client.query_procured_balancing_capacity(
        country_code, start, end, process_type, type_marketagreement_type
    )
    assert isinstance(result, str)
    assert valid_xml(result)


def test_query_procured_balancing_capacity_process_type_not_allowed(client):
    type_marketagreement_type = "A01"
    process_type = "A01"
    with pytest.raises(ValueError):
        client.query_procured_balancing_capacity(
            country_code, start, end, process_type, type_marketagreement_type
        )

# ZIP

def test_query_imbalance_prices(client, country_code, start, end):
    result = client.query_imbalance_prices(country_code, start, end)
    assert isinstance(result, (bytes, bytearray))


def test_query_unavailability_of_generation_units(client, country_code, start, end):
    result = client.query_unavailability_of_generation_units(country_code, start, end,)
    assert isinstance(result, (bytes, bytearray))


def test_query_unavailability_of_production_units(client, country_code, start, end):
    result = client.query_unavailability_of_production_units(country_code, start, end,)
    assert isinstance(result, (bytes, bytearray))


def test_query_unavailability_transmission(
    client, country_code_from, country_code_to, start, end
):
    result = client.query_unavailability_transmission(
        country_code_from, country_code_to, start, end,
    )
    assert isinstance(result, (bytes, bytearray))


def test_query_withdrawn_unavailability_of_generation_units(
    client, country_code, start, end
):
    result = client.query_withdrawn_unavailability_of_generation_units(
        country_code, start, end,
    )
    assert isinstance(result, (bytes, bytearray))
