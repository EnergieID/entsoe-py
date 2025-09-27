import pytest
import pandas as pd
from unittest.mock import patch, Mock
from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError


class TestWorkingSuite:
    
    @pytest.fixture
    def raw_client(self):
        return EntsoeRawClient(api_key="test_key")
    
    @pytest.fixture
    def pandas_client(self):
        return EntsoePandasClient(api_key="test_key")
    
    @patch('entsoe.entsoe.requests.Session.get')
    def test_query_wind_and_solar_forecast(self, mock_get, raw_client):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.headers = {'content-type': 'application/xml'}
        mock_response.text = '<xml>test</xml>'
        mock_get.return_value = mock_response
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = raw_client.query_wind_and_solar_forecast('DE', start, end, psr_type='B16')
        assert result == '<xml>test</xml>'
    
    @patch('entsoe.entsoe.requests.Session.get')
    def test_query_generation_per_plant(self, mock_get, raw_client):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.headers = {'content-type': 'application/xml'}
        mock_response.text = '<xml>test</xml>'
        mock_get.return_value = mock_response
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = raw_client.query_generation_per_plant('DE', start, end, psr_type='B14')
        assert result == '<xml>test</xml>'
    
    @patch('entsoe.entsoe.requests.Session.get')
    def test_query_crossborder_flows_raw(self, mock_get, raw_client):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.headers = {'content-type': 'application/xml'}
        mock_response.text = '<xml>test</xml>'
        mock_get.return_value = mock_response
        
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        result = raw_client.query_crossborder_flows('DE', 'FR', start, end)
        assert result == '<xml>test</xml>'
    
    def test_query_aggregated_bids_invalid_process_type(self, raw_client):
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        with pytest.raises(ValueError, match='processType allowed values'):
            raw_client.query_aggregated_bids('DE', 'INVALID', start, end)
    
    def test_query_procured_balancing_capacity_invalid_process_type(self, raw_client):
        start = pd.Timestamp('2023-01-01', tz='UTC')
        end = pd.Timestamp('2023-01-02', tz='UTC')
        
        with pytest.raises(ValueError, match='processType allowed values'):
            raw_client.query_procured_balancing_capacity('DE', start, end, 'INVALID')