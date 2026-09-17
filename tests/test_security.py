"""
Tests that the API key never leaks into log records or error messages.

These tests mock the HTTP layer, so they need no API key and make no
network calls.
"""
import logging
import traceback

from entsoe import EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError, PaginationError
import pandas as pd
import pytest
import requests

API_KEY = "test-key-should-never-be-logged"
START = pd.Timestamp("20240101", tz="Europe/Brussels")
END = pd.Timestamp("20240102", tz="Europe/Brussels")


def build_response(status_code: int, body: str) -> requests.Response:
    """A response whose url carries the securityToken, like the real API."""
    response = requests.Response()
    response.status_code = status_code
    response.url = (
        "https://web-api.tp.entsoe.eu/api"
        f"?securityToken={API_KEY}&documentType=A44"
    )
    response._content = body.encode()
    response.headers["content-type"] = "text/html"
    response.request = requests.Request("GET", response.url).prepare()
    return response


@pytest.fixture
def client(monkeypatch):
    """A client whose session returns a canned response instead of calling out.

    Outgoing calls are recorded in client.sent_params, so tests can check that
    masking changes what is logged without changing what is sent.
    """
    def make(status_code: int, body: str) -> EntsoeRawClient:
        client = EntsoeRawClient(api_key=API_KEY, retry_count=1)
        client.sent_params = []

        def fake_get(*args, **kwargs):
            client.sent_params.append(kwargs.get("params"))
            return build_response(status_code, body)

        monkeypatch.setattr(client.session, "get", fake_get)
        return client
    yield make


def test_http_error_message_masks_api_key(client):
    unauthorized = client(401, "<html><body>Unauthorized</body></html>")

    with pytest.raises(requests.HTTPError) as excinfo:
        unauthorized.query_day_ahead_prices("DE_LU", start=START, end=END)

    assert API_KEY not in str(excinfo.value)
    assert "securityToken=<hidden>" in str(excinfo.value)


def test_http_error_traceback_masks_api_key(client):
    unauthorized = client(401, "<html><body>Unauthorized</body></html>")

    try:
        unauthorized.query_day_ahead_prices("DE_LU", start=START, end=END)
    except requests.HTTPError:
        assert API_KEY not in traceback.format_exc()
    else:
        pytest.fail("expected an HTTPError")


def test_http_error_keeps_type_and_response(client):
    """Callers catching requests.HTTPError and reading .response keep working."""
    unauthorized = client(401, "<html><body>Unauthorized</body></html>")

    with pytest.raises(requests.HTTPError) as excinfo:
        unauthorized.query_day_ahead_prices("DE_LU", start=START, end=END)

    error = excinfo.value
    assert isinstance(error, requests.exceptions.RequestException)
    assert error.response is not None
    assert error.response.status_code == 401
    assert error.request is not None


def test_no_matching_data_traceback_masks_api_key(client):
    """This path runs routinely, so its exception context must not leak either."""
    no_data = client(
        400,
        "<html><body><text>No matching data found for Data item "
        "Day-ahead Prices</text></body></html>")

    try:
        no_data.query_day_ahead_prices("DE_LU", start=START, end=END)
    except NoMatchingDataError:
        assert API_KEY not in traceback.format_exc()
    else:
        pytest.fail("expected a NoMatchingDataError")


def test_pagination_error_traceback_masks_api_key(client):
    # The wording is shaped to match the positional parsing already in
    # _base_request, which reads the limits off the end of the error text.
    too_much = client(
        400,
        "<html><body><text>The amount of requested data exceeds allowed limit "
        "of 200 documents (Requested: 600 documents)</text></body></html>")

    try:
        too_much.query_day_ahead_prices("DE_LU", start=START, end=END)
    except PaginationError as error:
        assert API_KEY not in traceback.format_exc()
        assert "200" in str(error)
    else:
        pytest.fail("expected a PaginationError")


def test_debug_log_masks_api_key(client, caplog):
    ok = client(401, "<html><body>Unauthorized</body></html>")

    with caplog.at_level(logging.DEBUG, logger="entsoe.entsoe"):
        with pytest.raises(requests.HTTPError):
            ok.query_day_ahead_prices("DE_LU", start=START, end=END)

    messages = [record.getMessage() for record in caplog.records]
    assert messages, "expected the request to be logged at debug level"
    assert not any(API_KEY in message for message in messages)
    assert any("securityToken" in message for message in messages)

    # The masking must not reach the request itself: the real token is still sent.
    assert ok.sent_params, "expected the request to be sent"
    assert ok.sent_params[-1]["securityToken"] == API_KEY
