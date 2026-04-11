from itertools import product
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from entsoe import EntsoeRawClient
import pandas as pd
import pytest

load_dotenv()

API_KEY = os.getenv("API_KEY")


@pytest.fixture
def client():
    yield EntsoeRawClient(api_key=API_KEY)


def valid_xml(s: str) -> bool:
    try:
        BeautifulSoup(s, "html.parser")
        return True
    except Exception:
        return False


STARTS = [pd.Timestamp("20260301", tz="Europe/Brussels")]
ENDS = [pd.Timestamp("20260331", tz="Europe/Brussels")]

COUNTRY_CODES = ["NL", "BE", "DE_LU", "FR"]
COUNTRY_CODES_FROM = ["NL"]
COUNTRY_CODES_TO = ["DE_LU", 'NO_2']

BASIC_QUERIES_SPECIAL_ZONE = {
    'query_aggregate_water_reservoirs_and_hydro_storage': 'NO_2'
}


BASIC_QUERIES = [
    "query_day_ahead_prices",
    "query_net_position",
    "query_load",
    "query_load_forecast",
    "query_wind_and_solar_forecast",
    "query_generation_forecast",
    "query_generation",
    "query_generation_per_plant",
    "query_installed_generation_capacity",
    "query_installed_generation_capacity_per_unit"
]

CROSSBORDER_QUERIES = [
    "query_crossborder_flows",
    "query_scheduled_exchanges",
    "query_net_transfer_capacity_dayahead",
    "query_net_transfer_capacity_weekahead",
    "query_net_transfer_capacity_monthahead",
    "query_net_transfer_capacity_yearahead",
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


def test_query_aggregate_water_reservoirs_and_hydro_storage(client, start, end):
    result = client.query_aggregate_water_reservoirs_and_hydro_storage('NO_2', start=start, end=end)
    assert isinstance(result, str)
    assert valid_xml(result)


@pytest.mark.parametrize(
    "country_code_from, country_code_to, start, end",
    product(COUNTRY_CODES_FROM, COUNTRY_CODES_TO, STARTS, ENDS),
)
def test_query_intraday_offered_capacity(client, country_code_from, country_code_to, start, end):
    result = client.query_intraday_offered_capacity(
        country_code_from, country_code_to, start, end, implicit=True
    )
    assert isinstance(result, str)
    assert valid_xml(result)


@pytest.mark.parametrize(
    "country_code, process_type, start, end",
    product(["NL", "BE", "DE_AMPRION", "FR"], ['A51', 'A52', 'A47'], STARTS, ENDS),
)
def test_query_contracted_reserve_prices_procured_capacity(client, country_code, process_type, start, end):
    # [O] A51 = Automatic frequency restoration reserve; A52 = Frequency containment reserve; A47 = Manual frequency restoration reserve; A46 = Replacement reserve
    result = client.query_contracted_reserve_prices_procured_capacity(
        country_code, start, end, process_type=process_type, type_marketagreement_type='A01'
    )
    # zip
    assert isinstance(result, (bytes, bytearray))


# hier gebleven:
def test_query_unavailability_of_generation_units(client, country_code, start, end):
    result = client.query_unavailability_of_generation_units(country_code, start, end, docstatus=None, periodstartupdate=None, periodendupdate=None)
    assert isinstance(result, (bytes, bytearray))


def test_query_unavailability_of_production_units(client, country_code, start, end):
    result = client.query_unavailability_of_production_units(country_code, start, end, docstatus=None, periodstartupdate=None, periodendupdate=None)
    assert isinstance(result, (bytes, bytearray))


def test_query_unavailability_transmission(
    client, country_code_from, country_code_to, start, end
):
    result = client.query_unavailability_transmission(
        country_code_from, country_code_to, start, end, docstatus=None, periodstartupdate=None, periodendupdate=None
    )
    assert isinstance(result, (bytes, bytearray))


def test_query_withdrawn_unavailability_of_generation_units(
    client, country_code, start, end
):
    result = client.query_withdrawn_unavailability_of_generation_units(
        country_code, start, end,
    )
    assert isinstance(result, (bytes, bytearray))


#TODO: imbalance_volumes imbalance_prices balancing_state