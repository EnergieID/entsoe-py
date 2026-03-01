"""
Tests for entsoe client: _datetime_to_str, _base_request error handling,
and input validation.
"""
import os
import re

import pandas as pd
import pytz
import pytest
import requests
from unittest.mock import Mock, patch

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import (
    NoMatchingDataError,
    PaginationError,
    InvalidBusinessParameterError,
    InvalidPSRTypeError,
)


# ---------------------------------------------------------------------------
# 11.1  Unit tests for _datetime_to_str
# ---------------------------------------------------------------------------

class TestDatetimeToStr:
    """Unit tests for EntsoeRawClient._datetime_to_str."""

    def test_timezone_aware_converts_to_utc(self):
        """Timezone-aware timestamp in Europe/Berlin converts to UTC before formatting."""
        # 2023-01-15 14:00 Berlin == 2023-01-15 13:00 UTC (CET = UTC+1)
        dt = pd.Timestamp("2023-01-15 14:00", tz="Europe/Berlin")
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202301151300"

    def test_timezone_naive_treated_as_utc(self):
        """Timezone-naive timestamp is treated as UTC (no conversion)."""
        dt = pd.Timestamp("2023-06-20 09:00")
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202306200900"

    def test_rounding_to_nearest_hour_down(self):
        """Timestamp at 29 minutes rounds down to the current hour."""
        dt = pd.Timestamp("2023-03-10 07:29:59", tz="UTC")
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202303100700"

    def test_rounding_to_nearest_hour_up(self):
        """Timestamp at 30+ minutes rounds up to the next hour."""
        dt = pd.Timestamp("2023-03-10 07:30:00", tz="UTC")
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202303100800"

    def test_output_format_matches_pattern(self):
        """Output is exactly 12 characters matching YYYYMMDDhh00."""
        dt = pd.Timestamp("2024-12-31 23:15:00", tz="UTC")
        result = EntsoeRawClient._datetime_to_str(dt)
        assert re.fullmatch(r"\d{10}00", result), f"Unexpected format: {result}"

    def test_utc_timestamp_no_conversion_needed(self):
        """A UTC timestamp passes through without conversion."""
        dt = pd.Timestamp("2023-07-04 16:00", tz="UTC")
        result = EntsoeRawClient._datetime_to_str(dt)
        assert result == "202307041600"


# ---------------------------------------------------------------------------
# 11.2  Property test for datetime-to-string format
# ---------------------------------------------------------------------------

# Strategy: generate pandas Timestamps across a wide date range, both tz-aware and naive
_tz_options = st.sampled_from([None, "UTC", "Europe/Berlin", "US/Eastern", "Asia/Tokyo"])

@st.composite
def pandas_timestamps(draw):
    """Generate random pandas Timestamps, optionally timezone-aware."""
    # Use a reasonable date range to avoid overflow issues
    dt = draw(st.datetimes(
        min_value=pd.Timestamp("1970-01-01").to_pydatetime(),
        max_value=pd.Timestamp("2099-12-31 23:59:59").to_pydatetime(),
    ))
    tz = draw(_tz_options)
    ts = pd.Timestamp(dt)
    if tz is not None:
        ts = ts.tz_localize(tz)
    return ts


class TestDatetimeToStrProperty:
    """Property 20: Datetime-to-string format and UTC conversion."""

    @given(ts=pandas_timestamps())
    @settings(max_examples=100)
    def test_format_and_utc_conversion(self, ts: pd.Timestamp):
        """For all pandas Timestamps, _datetime_to_str returns a string matching
        \\d{10}00 and the encoded hour equals the UTC hour of the input rounded
        to the nearest hour."""
        result = EntsoeRawClient._datetime_to_str(ts)

        # Format check: exactly 12 digits, last two are '00'
        assert re.fullmatch(r"\d{10}00", result), f"Bad format: {result}"

        # Compute expected UTC hour after rounding
        if ts.tzinfo is not None:
            utc_ts = ts.tz_convert("UTC")
        else:
            utc_ts = ts  # naive treated as UTC
        rounded = utc_ts.round(freq="h")

        expected = rounded.strftime("%Y%m%d%H00")
        assert result == expected, f"Expected {expected}, got {result} for input {ts}"


# ---------------------------------------------------------------------------
# 11.3  Unit tests for _base_request error handling
# ---------------------------------------------------------------------------

def _make_client():
    """Create an EntsoeRawClient with a test key."""
    return EntsoeRawClient(api_key="test_key")


def _make_error_response(text_body: str):
    """Build a mock response whose raise_for_status raises HTTPError
    and whose .text contains the given body wrapped in <text> tags."""
    resp = Mock(spec=requests.Response)
    resp.text = f"<html><body><text>{text_body}</text></body></html>"
    resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    resp.headers = {}
    return resp


def _make_ok_response(text_body: str, content_type: str = "application/xml"):
    """Build a mock 200 response with given body and content-type."""
    resp = Mock(spec=requests.Response)
    resp.text = text_body
    resp.raise_for_status.return_value = None  # no error
    resp.headers = {"content-type": content_type}
    return resp


class TestBaseRequestErrorHandling:
    """Unit tests for EntsoeRawClient._base_request error translation."""

    START = pd.Timestamp("2023-01-01", tz="UTC")
    END = pd.Timestamp("2023-01-02", tz="UTC")
    PARAMS = {"documentType": "A44"}

    def test_no_matching_data_http_error(self):
        """HTTP error with 'No matching data found' raises NoMatchingDataError."""
        client = _make_client()
        mock_resp = _make_error_response("No matching data found")
        client.session = Mock()
        client.session.get.return_value = mock_resp

        with pytest.raises(NoMatchingDataError):
            client._base_request(self.PARAMS.copy(), self.START, self.END)

    def test_pagination_error(self):
        """HTTP error with 'amount of requested data exceeds allowed limit' raises PaginationError."""
        client = _make_client()
        msg = "The amount of requested data exceeds allowed limit of 100 elements. Requested 200 elements."
        mock_resp = _make_error_response(msg)
        client.session = Mock()
        client.session.get.return_value = mock_resp

        with pytest.raises(PaginationError):
            client._base_request(self.PARAMS.copy(), self.START, self.END)

    def test_invalid_business_parameter_error(self):
        """HTTP error with 'check you request against dependency tables' raises InvalidBusinessParameterError."""
        client = _make_client()
        mock_resp = _make_error_response(
            "Please check you request against dependency tables"
        )
        client.session = Mock()
        client.session.get.return_value = mock_resp

        with pytest.raises(InvalidBusinessParameterError):
            client._base_request(self.PARAMS.copy(), self.START, self.END)

    def test_invalid_psr_type_error(self):
        """HTTP error with 'is not valid for this area' raises InvalidPSRTypeError."""
        client = _make_client()
        mock_resp = _make_error_response("B99 is not valid for this area")
        client.session = Mock()
        client.session.get.return_value = mock_resp

        with pytest.raises(InvalidPSRTypeError):
            client._base_request(self.PARAMS.copy(), self.START, self.END)

    def test_200_xml_no_matching_data(self):
        """200 response with XML content-type containing 'No matching data found' raises NoMatchingDataError."""
        client = _make_client()
        mock_resp = _make_ok_response(
            "<html><body>No matching data found for the request</body></html>",
            content_type="application/xml",
        )
        client.session = Mock()
        client.session.get.return_value = mock_resp

        with pytest.raises(NoMatchingDataError):
            client._base_request(self.PARAMS.copy(), self.START, self.END)

    def test_200_xml_text_content_type_no_matching_data(self):
        """200 response with text/xml content-type containing 'No matching data found' raises NoMatchingDataError."""
        client = _make_client()
        mock_resp = _make_ok_response(
            "No matching data found",
            content_type="text/xml",
        )
        client.session = Mock()
        client.session.get.return_value = mock_resp

        with pytest.raises(NoMatchingDataError):
            client._base_request(self.PARAMS.copy(), self.START, self.END)

    def test_200_non_xml_no_matching_data_passes(self):
        """200 response with non-XML content-type does NOT raise even if body contains the string."""
        client = _make_client()
        mock_resp = _make_ok_response(
            "No matching data found",
            content_type="application/zip",
        )
        client.session = Mock()
        client.session.get.return_value = mock_resp

        result = client._base_request(self.PARAMS.copy(), self.START, self.END)
        assert result is mock_resp

    def test_unknown_http_error_propagates(self):
        """HTTP error without recognized text re-raises the original HTTPError."""
        client = _make_client()
        resp = Mock(spec=requests.Response)
        resp.text = "<text>Some unknown server error</text>"
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
        resp.headers = {}
        client.session = Mock()
        client.session.get.return_value = resp

        with pytest.raises(requests.HTTPError):
            client._base_request(self.PARAMS.copy(), self.START, self.END)


# ---------------------------------------------------------------------------
# 11.4  Unit tests for client input validation
# ---------------------------------------------------------------------------

class TestClientInputValidation:
    """Unit tests for client constructor and query input validation."""

    def test_api_key_none_no_env_raises_type_error(self):
        """api_key=None with no ENTSOE_API_KEY env var raises TypeError."""
        with patch.dict(os.environ, {}, clear=True):
            # Ensure the env var is definitely absent
            os.environ.pop("ENTSOE_API_KEY", None)
            with pytest.raises(TypeError, match="API key cannot be None"):
                EntsoeRawClient(api_key=None)

    def test_invalid_country_code_raises_value_error(self):
        """Invalid country code raises ValueError via lookup_area."""
        client = EntsoeRawClient(api_key="test_key")
        start = pd.Timestamp("2023-01-01", tz="UTC")
        end = pd.Timestamp("2023-01-02", tz="UTC")

        with pytest.raises(ValueError):
            client.query_day_ahead_prices("INVALID_CODE", start, end)

    def test_invalid_process_type_query_aggregated_bids(self):
        """Invalid process_type in query_aggregated_bids raises ValueError."""
        client = EntsoeRawClient(api_key="test_key")
        start = pd.Timestamp("2023-01-01", tz="UTC")
        end = pd.Timestamp("2023-01-02", tz="UTC")

        with pytest.raises(ValueError, match="processType allowed values"):
            client.query_aggregated_bids("DE_LU", "INVALID", start, end)

    def test_invalid_process_type_query_procured_balancing_capacity(self):
        """Invalid process_type in query_procured_balancing_capacity raises ValueError."""
        client = EntsoeRawClient(api_key="test_key")
        start = pd.Timestamp("2023-01-01", tz="UTC")
        end = pd.Timestamp("2023-01-02", tz="UTC")

        with pytest.raises(ValueError, match="processType allowed values"):
            client.query_procured_balancing_capacity(
                "DE_LU", start, end, process_type="INVALID"
            )
