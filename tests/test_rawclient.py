import pandas as pd
import pytest
import requests as r
import requests_mock

from entsoe import EntsoeRawClient

start = pd.Timestamp("20171201", tz="Europe/Brussels")
end = pd.Timestamp("20180101", tz="Europe/Brussels")
country_code = "BE"  # Belgium
country_code_from = "FR"  # France
country_code_to = "DE_LU"  # Germany-Luxembourg
type_marketagreement_type = "A01"
URL = "https://transparency.entsoe.eu/api"


def test_invalid_rawclient():
    """Will initiate RawClient without API key."""
    with pytest.raises(TypeError):
        EntsoeRawClient()


def test_rawclient_get_exception():
    """Will test if exception handling is triggered in _base_request."""
    rm = requests_mock.Mocker()

    with pytest.raises(r.HTTPError):
        rm.get(URL, status_code=500)
        client = EntsoeRawClient(api_key="test123")
        client.query_load(country_code, start, end)
